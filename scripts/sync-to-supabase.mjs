#!/usr/bin/env node

import { readdir } from "node:fs/promises";
import { readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { createClient } from "@supabase/supabase-js";
import matter from "gray-matter";

function toPosix(p) {
  return p.split(path.sep).join("/");
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

  if (!supabaseUrl || !serviceRoleKey) {
    console.log("[sync-to-supabase] SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is not set. Skipping upload.");
    return;
  }

  const supabase = createClient(supabaseUrl, serviceRoleKey);

  // Upsert to documents table
  const rows = [];
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
}

main().catch((e) => {
  console.error("[sync-to-supabase] Unhandled error:", e);
  // Do not fail the workflow for now; exit 0 for observability-first rollout
  process.exit(0);
});


