/**
 * reembed.mjs
 * Re-generates embeddings for all laptops using brand+model+cpu+description
 * as input text. Uses the same model (Xenova/bge-small-en-v1.5) as the search API route.
 *
 * Usage:
 *   cd frontend
 *   node scripts/reembed.mjs
 */

import { pipeline, env } from "@huggingface/transformers";

env.cacheDir = "./models_cache";

const SUPABASE_URL = "https://kvadnjfauzzoplqzcscd.supabase.co";
const SUPABASE_KEY =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imt2YWRuamZhdXp6b3BscXpjc2NkIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI4Mzg1MDYsImV4cCI6MjA4ODQxNDUwNn0.78sODER5OJSmG5Kpm-uVJr8wrz_zNvTCVXTPORKwQGE";

const headers = {
    apikey: SUPABASE_KEY,
    Authorization: `Bearer ${SUPABASE_KEY}`,
    "Content-Type": "application/json",
    Prefer: "return=minimal",
};

// Fetch all laptops
async function fetchAll() {
    const all = [];
    let offset = 0;
    const step = 1000;
    while (true) {
        const url = `${SUPABASE_URL}/rest/v1/laptops?select=id,brand,model,cpu,description&offset=${offset}&limit=${step}`;
        const res = await fetch(url, { headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` } });
        const data = await res.json();
        if (!data || data.length === 0) break;
        all.push(...data);
        offset += step;
        if (data.length < step) break;
    }
    return all;
}

// Build search text from row
function buildSearchText(row) {
    const parts = [
        row.brand || "",
        row.model || "",
        row.cpu || "",
        (row.description || "").slice(0, 200),
    ].filter(Boolean);
    return parts.join(" ").trim() || "laptop";
}

// Update embedding for a single row
async function updateEmbedding(id, embedding) {
    const url = `${SUPABASE_URL}/rest/v1/laptops?id=eq.${id}`;
    const res = await fetch(url, {
        method: "PATCH",
        headers,
        body: JSON.stringify({ embedding: JSON.stringify(embedding) }),
    });
    if (!res.ok) {
        throw new Error(`PATCH failed for id=${id}: ${res.status} ${await res.text()}`);
    }
}

async function main() {
    console.log("Loading model (Xenova/bge-small-en-v1.5)...");
    const embedder = await pipeline("feature-extraction", "Xenova/bge-small-en-v1.5");
    console.log("Model loaded.");

    console.log("Fetching all laptops...");
    const rows = await fetchAll();
    console.log(`Fetched ${rows.length} laptops.`);

    let done = 0;
    let errors = 0;
    const startTime = Date.now();

    for (const row of rows) {
        const text = buildSearchText(row);
        try {
            const output = await embedder(text, { pooling: "mean", normalize: true });
            const embedding = Array.from(output.data);
            await updateEmbedding(row.id, embedding);
            done++;

            if (done % 50 === 0) {
                const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
                const rate = (done / elapsed * 60).toFixed(0);
                const eta = (((rows.length - done) / (done / elapsed)) / 60).toFixed(1);
                console.log(`  ${done}/${rows.length} done (${rate}/min, ETA: ${eta} min, errors: ${errors})`);
            }
        } catch (err) {
            errors++;
            if (errors <= 5) console.error(`  Error on id=${row.id}: ${err.message}`);
        }
    }

    console.log(`\nDone! ${done} embeddings updated, ${errors} errors.`);
    console.log(`Total time: ${((Date.now() - startTime) / 1000 / 60).toFixed(1)} minutes`);
}

main().catch(console.error);
