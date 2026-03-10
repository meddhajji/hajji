import { NextRequest, NextResponse } from "next/server";
import { pipeline, env } from "@huggingface/transformers";
import { createClient } from "@supabase/supabase-js";

// Force model cache into a local writable directory (fixes Windows EACCES error)
env.cacheDir = "./models_cache";

const supabase = createClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL ?? "",
    process.env.SUPABASE_SERVICE_KEY ?? process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? ""
);

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let embedder: any = null;

async function getEmbedder() {
    if (!embedder) {
        embedder = await pipeline("feature-extraction", "Xenova/bge-small-en-v1.5");
    }
    return embedder;
}

export async function GET(req: NextRequest) {
    const q = req.nextUrl.searchParams.get("q");
    const count = parseInt(req.nextUrl.searchParams.get("count") ?? "20", 10);

    if (!q || !q.trim()) {
        return NextResponse.json({ error: "Missing q parameter" }, { status: 400 });
    }

    try {
        const embed = await getEmbedder();
        const output = await embed(q.trim(), { pooling: "mean", normalize: true });
        const embedding = Array.from(output.data as Float32Array);

        const { data, error } = await supabase.rpc("search_laptops", {
            query_embedding: embedding,
            match_count: count,
        });

        if (error) {
            return NextResponse.json({ error: error.message }, { status: 500 });
        }

        return NextResponse.json({ results: data, total: data?.length ?? 0 });
    } catch (err) {
        const message = err instanceof Error ? err.message : "Unknown error";
        return NextResponse.json({ error: message }, { status: 500 });
    }
}
