#!/usr/bin/env node

import { readdir } from "node:fs/promises";
import { readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { createClient } from "@supabase/supabase-js";
import matter from "gray-matter";
import crypto from "node:crypto";
import { GoogleGenAI } from "@google/genai";

function toPosix(p) {
  return p.split(path.sep).join("/");
}

function sha256(text) {
  return crypto.createHash("sha256").update(text).digest("hex");
}

function simpleChunk(text, maxChars = 1200, overlap = 150) {
  if (!text) return [];
  const chunks = [];
  let start = 0;
  while (start < text.length) {
    const end = Math.min(text.length, start + maxChars);
    chunks.push(text.slice(start, end));
    if (end === text.length) break;
    start = Math.max(0, end - overlap);
  }
  return chunks;
}

async function walkMarkdownFiles(startDir) {
  const results = [];
  async function walk(dir) {
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (e) {
      return;
    }
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        await walk(fullPath);
      } else if (entry.isFile() && entry.name.toLowerCase().endsWith(".md")) {
        results.push(fullPath);
      }
    }
  }
  await walk(startDir);
  return results;
}

async function main() {
  const repoRoot = process.cwd();
  const contentRoot = path.join(repoRoot, "content");

  const markdownPaths = await walkMarkdownFiles(contentRoot);
  console.log(`[sync-to-supabase] markdown files found: ${markdownPaths.length}`);

  // Show a few samples for observability
  for (const sample of markdownPaths.slice(0, 5)) {
    console.log(`[sync-to-supabase] sample: ${toPosix(path.relative(repoRoot, sample))}`);
  }

  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
  const geminiApiKey = process.env.GEMINI_API_KEY;

  if (!supabaseUrl || !serviceRoleKey) {
    console.log("[sync-to-supabase] SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is not set. Skipping upload.");
    return;
  }

  const supabase = createClient(supabaseUrl, serviceRoleKey);
  const genAI = geminiApiKey ? new GoogleGenAI({ apiKey: geminiApiKey }) : null;

  // Upsert to documents table
  const rows = [];
  const chunkRows = [];
  for (const absPath of markdownPaths) {
    const relPath = toPosix(path.relative(repoRoot, absPath));
    const file = await readFile(absPath, "utf8");
    const parsed = matter(file);
    const fm = parsed.data ?? {};
    const body = parsed.content.trim();

    if (!fm.id || !fm.type || !fm.title) {
      // Skip files without minimal front matter
      continue;
    }

    const slug = (fm.slug || relPath.replace(/\s+/g, "-").toLowerCase()).replace(/[^a-z0-9/_-]/g, "-");

    rows.push({
      id: String(fm.id),
      type: String(fm.type),
      title: String(fm.title),
      summary: fm.summary ? String(fm.summary) : null,
      tags: Array.isArray(fm.tags) ? fm.tags.map(String) : [],
      campus: fm.campus ? String(fm.campus) : null,
      license: fm.license ? String(fm.license) : null,
      geometry: fm.geometry ?? null,
      floor: typeof fm.floor === "number" ? fm.floor : null,
      version: fm.version ?? null,
      body,
      path: relPath,
      slug,
      last_updated: fm.version?.updated_at ? new Date(fm.version.updated_at).toISOString() : null,
      git_file_url: process.env.GITHUB_REPOSITORY ? `https://github.com/${process.env.GITHUB_REPOSITORY}/blob/${process.env.GITHUB_DEFAULT_BRANCH || "main"}/${relPath}` : null,
      synced_at: new Date().toISOString(),
    });

    // Embeddings chunks (optional if key provided)
    if (genAI) {
      const docId = String(fm.id);
      const contentChunks = simpleChunk(body);
      const total = contentChunks.length;
      for (let idx = 0; idx < total; idx += 1) {
        const chunkContent = contentChunks[idx];
        const chunkHash = sha256(`${docId}:${idx}:${chunkContent}`);
        chunkRows.push({
          id: chunkHash,
          doc_id: docId,
          path: relPath,
          title: String(fm.title),
          slug,
          chunk_index: idx,
          total_chunks: total,
          summary: fm.summary ? String(fm.summary) : null,
          hash: chunkHash,
          content: chunkContent,
          embedding: null, // filled after model call
        });
      }
    }
  }

  if (rows.length === 0) {
    console.log("[sync-to-supabase] No valid markdown rows to upsert. Exiting.");
    return;
  }

  // Batch upsert in chunks to avoid payload limits
  const chunkSize = 500;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const { error } = await supabase.from("documents").upsert(chunk, { onConflict: "id" });
    if (error) {
      console.log(`[sync-to-supabase] upsert error: ${error.message}`);
      // continue to next chunk; do not fail pipeline
    } else {
      console.log(`[sync-to-supabase] upserted ${chunk.length} rows`);
    }
  }
  console.log("[sync-to-supabase] Completed upsert to documents.");

  // Generate embeddings and upsert to document_chunks
  if (genAI && chunkRows.length > 0) {
    console.log(`[sync-to-supabase] Generating embeddings for ${chunkRows.length} chunks using gemini-embedding-001`);
    const model = "gemini-embedding-001"; // see docs: https://ai.google.dev/gemini-api/docs/embeddings?hl=ja
    const EMBEDDING_DIM = 768; // match table dimension

    // Process in batches to respect rate limits
    const embedBatch = async (items) => {
      const contents = items.map((r) => r.content);
      // The JS SDK supports batch via embedContent on array with map, fallback to per-item to be safe
      const embedded = [];
      for (let i = 0; i < contents.length; i += 1) {
        try {
          const resp = await genAI.models.embedContent({ model, contents: contents[i], outputDimensionality: EMBEDDING_DIM });
          let vector = resp?.embeddings?.[0]?.values || resp?.embedding?.values || [];
          // Ensure exact dimension
          if (Array.isArray(vector)) {
            if (vector.length > EMBEDDING_DIM) vector = vector.slice(0, EMBEDDING_DIM);
            if (vector.length < EMBEDDING_DIM) vector = vector.concat(new Array(EMBEDDING_DIM - vector.length).fill(0));
          }
          embedded.push(vector);
        } catch (e) {
          console.log(`[sync-to-supabase] embedding error: ${e instanceof Error ? e.message : String(e)}`);
          embedded.push(new Array(EMBEDDING_DIM).fill(0));
        }
        // small delay to reduce burst
        await new Promise((r) => setTimeout(r, 50));
      }
      return embedded;
    };

    const upsertBatch = async (batchRows) => {
      const { error } = await supabase.from("document_chunks").upsert(batchRows, { onConflict: "id" });
      if (error) console.log(`[sync-to-supabase] chunks upsert error: ${error.message}`);
    };

    const size = 50; // conservative batch size
    for (let i = 0; i < chunkRows.length; i += size) {
      const slice = chunkRows.slice(i, i + size);
      const vectors = await embedBatch(slice);
      const rowsWithEmb = slice.map((r, idx) => ({ ...r, embedding: vectors[idx] }));
      await upsertBatch(rowsWithEmb);
      console.log(`[sync-to-supabase] upserted chunk batch ${i}-${i + rowsWithEmb.length - 1}`);
    }
    console.log("[sync-to-supabase] Completed embeddings upsert to document_chunks.");
  }
}

main().catch((e) => {
  console.error("[sync-to-supabase] Unhandled error:", e);
  // Do not fail the workflow for now; exit 0 for observability-first rollout
  process.exit(0);
});


