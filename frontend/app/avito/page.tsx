"use client";

import { useEffect, useState, useCallback } from "react";
import { supabase } from "@/lib/supabase";
import type { Laptop } from "@/lib/types";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import {
    Dialog,
    DialogContent,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
} from "@/components/ui/dialog";
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Badge } from "@/components/ui/badge";

const PAGE_SIZE = 20;

type SortOption = "default" | "score" | "price" | "value";

interface Filters {
    brand: string;
    city: string;
    gpu_type: string;
    price_min: string;
    price_max: string;
    ram_min: string;
    storage_min: string;
    gpu_vram_min: string;
    screen_size_min: string;
    refresh_rate_min: string;
    is_new: boolean | null;
    is_shop: boolean | null;
    has_delivery: boolean | null;
    touchscreen: boolean | null;
    ssd: boolean | null;
    hide_sold: boolean;
}

const emptyFilters: Filters = {
    brand: "",
    city: "",
    gpu_type: "all",
    price_min: "",
    price_max: "",
    ram_min: "",
    storage_min: "",
    gpu_vram_min: "",
    screen_size_min: "",
    refresh_rate_min: "",
    is_new: null,
    is_shop: null,
    has_delivery: null,
    touchscreen: null,
    ssd: null,
    hide_sold: true,
};

// ------- column toggle config -------
interface ColDef {
    key: string;
    label: string;
    defaultOn: boolean;
    align?: "right" | "center";
    width?: string;
    render: (l: Laptop) => React.ReactNode;
}

const SCORE_BADGE = (l: Laptop) => {
    const s = l.score ?? 0;
    const cls =
        s >= 700
            ? "bg-emerald-500/20 text-emerald-400"
            : s >= 400
                ? "bg-amber-500/20 text-amber-400"
                : "bg-zinc-500/20 text-zinc-400";
    return (
        <span
            className={`inline-flex items-center justify-center w-10 h-6 rounded text-xs font-bold ${cls}`}
        >
            {s}
        </span>
    );
};

const ALL_COLUMNS: ColDef[] = [
    { key: "score", label: "Score", defaultOn: true, align: "center", width: "w-[60px]", render: SCORE_BADGE },
    { key: "brand", label: "Brand", defaultOn: true, width: "w-[100px]", render: (l) => <span className="font-medium">{l.brand || "—"}</span> },
    { key: "model", label: "Model", defaultOn: true, render: (l) => l.model || "—" },
    { key: "cpu", label: "CPU", defaultOn: true, render: (l) => <span className="max-w-[140px] truncate block">{l.cpu || "—"}</span> },
    { key: "ram", label: "RAM", defaultOn: true, align: "right", render: (l) => (l.ram != null ? `${l.ram}` : "—") },
    { key: "storage", label: "Storage", defaultOn: true, align: "right", render: (l) => (l.storage != null ? `${l.storage}` : "—") },
    { key: "gpu", label: "GPU", defaultOn: true, render: (l) => <span className="max-w-[120px] truncate block">{l.gpu || "—"}</span> },
    { key: "price", label: "Price", defaultOn: true, align: "right", render: (l) => l.is_sold ? <Badge variant="destructive" className="text-[10px] uppercase font-bold tracking-wider opacity-80 py-0 leading-tight">Sold</Badge> : (l.price ? <span className="font-semibold">{l.price.toLocaleString()} DH</span> : "—") },
    { key: "city", label: "City", defaultOn: true, render: (l) => l.city || "—" },
    { key: "new", label: "New", defaultOn: true, width: "w-[60px]", render: (l) => l.new === 1 ? <Badge variant="secondary" className="text-xs">New</Badge> : <span className="text-xs text-muted-foreground">Used</span> },
    { key: "gpu_type", label: "GPU type", defaultOn: false, render: (l) => l.gpu_type || "—" },
    { key: "gpu_vram", label: "VRAM", defaultOn: false, align: "right", render: (l) => (l.gpu_vram != null ? `${l.gpu_vram}` : "—") },
    { key: "screen_size", label: "Screen", defaultOn: false, align: "right", render: (l) => (l.screen_size != null ? `${l.screen_size}"` : "—") },
    { key: "refresh_rate", label: "Hz", defaultOn: false, align: "right", render: (l) => (l.refresh_rate != null ? `${l.refresh_rate}` : "—") },
    { key: "ssd", label: "SSD", defaultOn: false, width: "w-[50px]", render: (l) => (l.ssd === 1 ? "✓" : "—") },
    { key: "touchscreen", label: "Touch", defaultOn: false, width: "w-[50px]", render: (l) => (l.touchscreen === 1 ? "✓" : "—") },
    { key: "is_shop", label: "Shop", defaultOn: false, width: "w-[50px]", render: (l) => (l.is_shop ? "✓" : "—") },
    { key: "has_delivery", label: "Delivery", defaultOn: false, width: "w-[60px]", render: (l) => (l.has_delivery ? "✓" : "—") },
    { key: "link", label: "Link", defaultOn: false, render: (l) => l.link ? <a href={l.link} target="_blank" rel="noopener noreferrer" className="text-blue-400 hover:underline text-xs">Avito ↗</a> : "—" },
    { key: "description", label: "Description", defaultOn: false, render: (l) => <span className="max-w-[200px] truncate block text-xs">{l.description || "—"}</span> },
    { key: "avito_id", label: "Avito ID", defaultOn: false, render: (l) => <span className="text-xs">{l.avito_id || "—"}</span> },
];

const DEFAULT_VISIBLE = new Set(ALL_COLUMNS.filter((c) => c.defaultOn).map((c) => c.key));

export default function AvitoPage() {
    const [laptops, setLaptops] = useState<Laptop[]>([]);
    const [total, setTotal] = useState(0);
    const [page, setPage] = useState(0);
    const [loading, setLoading] = useState(true);
    const [search, setSearch] = useState("");
    const [committedSearch, setCommittedSearch] = useState("");
    const [filters, setFilters] = useState<Filters>({ ...emptyFilters });
    const [activeFilters, setActiveFilters] = useState<Filters>({
        ...emptyFilters,
    });
    const [filterOpen, setFilterOpen] = useState(false);
    const [sortBy, setSortBy] = useState<SortOption>("default");
    const [stats, setStats] = useState<{ created_at: string; new_items: number; updated_items: number; sold_items: number; total_items: number; } | null>(null);

    // column toggle
    const [visibleCols, setVisibleCols] = useState<Set<string>>(new Set(DEFAULT_VISIBLE));
    const [colMenuOpen, setColMenuOpen] = useState(false);

    const toggleCol = (key: string) => {
        setVisibleCols((prev) => {
            const next = new Set(prev);
            if (next.has(key)) next.delete(key);
            else next.add(key);
            return next;
        });
    };

    const activeCols = ALL_COLUMNS.filter((c) => visibleCols.has(c.key));

    // distinct values for gpu_type dropdown only
    const [gpuTypes, setGpuTypes] = useState<string[]>([]);

    useEffect(() => {
        async function loadGpuTypes() {
            const all: string[] = [];
            let from = 0;
            const step = 1000;
            let done = false;
            while (!done) {
                const { data } = await supabase
                    .from("laptops")
                    .select("gpu_type")
                    .not("gpu_type", "is", null)
                    .not("gpu_type", "eq", "")
                    .range(from, from + step - 1);
                if (data && data.length > 0) {
                    const rows = data as unknown as Record<string, string>[];
                    all.push(...rows.map((r) => r["gpu_type"]).filter(Boolean));
                    from += step;
                    if (data.length < step) done = true;
                } else {
                    done = true;
                }
            }
            setGpuTypes([...new Set(all)].sort());
        }
        async function loadStats() {
            const { data } = await supabase.from("pipeline_stats").select("*").order("created_at", { ascending: false }).limit(1).single();
            if (data) setStats(data as any);
        }
        loadGpuTypes();
        loadStats();
    }, []);

    function applyClientFilters(rows: Laptop[], f: Filters): Laptop[] {
        return rows.filter((r) => {
            if (f.brand && !r.brand?.toLowerCase().includes(f.brand.toLowerCase())) return false;
            if (f.city && !r.city?.toLowerCase().includes(f.city.toLowerCase())) return false;
            if (f.gpu_type !== "all" && r.gpu_type?.toLowerCase() !== f.gpu_type.toLowerCase()) return false;
            if (f.price_min && (r.price ?? 0) < parseFloat(f.price_min)) return false;
            if (f.price_max && (r.price ?? Infinity) > parseFloat(f.price_max)) return false;
            if (f.ram_min && (r.ram ?? 0) < parseFloat(f.ram_min)) return false;
            if (f.storage_min && (r.storage ?? 0) < parseFloat(f.storage_min)) return false;
            if (f.gpu_vram_min && (r.gpu_vram ?? 0) < parseFloat(f.gpu_vram_min)) return false;
            if (f.screen_size_min && (r.screen_size ?? 0) < parseFloat(f.screen_size_min)) return false;
            if (f.refresh_rate_min && (r.refresh_rate ?? 0) < parseFloat(f.refresh_rate_min)) return false;
            if (f.is_new === true && r.new !== 1) return false;
            if (f.is_shop === true && !r.is_shop) return false;
            if (f.has_delivery === true && !r.has_delivery) return false;
            if (f.touchscreen === true && r.touchscreen !== 1) return false;
            if (f.ssd === true && r.ssd !== 1) return false;
            if (f.hide_sold && r.is_sold) return false;
            return true;
        });
    }

    const fetchLaptops = useCallback(async () => {
        setLoading(true);

        // similarity search via API route
        if (committedSearch.trim()) {
            try {
                const res = await fetch(`/api/search?q=${encodeURIComponent(committedSearch.trim())}&count=200`);
                const json = await res.json();
                if (json.error) {
                    console.error("Search error:", json.error);
                    setLaptops([]);
                    setTotal(0);
                } else {
                    let results = json.results as Laptop[];
                    results = applyClientFilters(results, activeFilters);

                    if (sortBy === "score") {
                        results.sort((a, b) => (b.score ?? 0) - (a.score ?? 0));
                    } else if (sortBy === "price") {
                        results.sort((a, b) => (b.price ?? 0) - (a.price ?? 0));
                    } else if (sortBy === "value") {
                        results.sort((a, b) => {
                            const va = (a.score ?? 0) / Math.max(a.price ?? 1, 1);
                            const vb = (b.score ?? 0) / Math.max(b.price ?? 1, 1);
                            return vb - va;
                        });
                    }

                    setTotal(results.length);
                    const from = page * PAGE_SIZE;
                    setLaptops(results.slice(from, from + PAGE_SIZE));
                }
            } catch (err) {
                console.error("Search fetch failed:", err);
                setLaptops([]);
                setTotal(0);
            }
            setLoading(false);
            return;
        }

        // For "value" sort, fetch a larger set and sort client-side
        if (sortBy === "value") {
            let query = supabase
                .from("laptops")
                .select("*")
                .gt("price", 0)
                .gt("score", 0);

            if (activeFilters.brand) query = query.ilike("brand", `%${activeFilters.brand}%`);
            if (activeFilters.city) query = query.ilike("city", `%${activeFilters.city}%`);
            if (activeFilters.gpu_type !== "all") query = query.ilike("gpu_type", activeFilters.gpu_type);
            if (activeFilters.price_min) query = query.gte("price", parseFloat(activeFilters.price_min));
            if (activeFilters.price_max) query = query.lte("price", parseFloat(activeFilters.price_max));
            if (activeFilters.ram_min) query = query.gte("ram", parseFloat(activeFilters.ram_min));
            if (activeFilters.storage_min) query = query.gte("storage", parseFloat(activeFilters.storage_min));
            if (activeFilters.gpu_vram_min) query = query.gte("gpu_vram", parseFloat(activeFilters.gpu_vram_min));
            if (activeFilters.screen_size_min) query = query.gte("screen_size", parseFloat(activeFilters.screen_size_min));
            if (activeFilters.refresh_rate_min) query = query.gte("refresh_rate", parseFloat(activeFilters.refresh_rate_min));
            if (activeFilters.is_new === true) query = query.eq("new", 1);
            if (activeFilters.is_shop === true) query = query.eq("is_shop", true);
            if (activeFilters.has_delivery === true) query = query.eq("has_delivery", true);
            if (activeFilters.touchscreen === true) query = query.eq("touchscreen", 1);
            if (activeFilters.ssd === true) query = query.eq("ssd", 1);
            if (activeFilters.hide_sold) query = query.eq("is_sold", false);

            query = query.order("score", { ascending: false }).limit(2000);
            const { data, error } = await query;
            if (error?.message) console.error("Fetch error:", error.message);

            let results = data || [];
            results.sort((a, b) => {
                const va = (a.score ?? 0) / Math.max(a.price ?? 1, 1);
                const vb = (b.score ?? 0) / Math.max(b.price ?? 1, 1);
                return vb - va;
            });

            setTotal(results.length);
            const from = page * PAGE_SIZE;
            setLaptops(results.slice(from, from + PAGE_SIZE));
            setLoading(false);
            return;
        }

        // standard filtered query
        const from = page * PAGE_SIZE;
        const to = from + PAGE_SIZE - 1;

        let query = supabase
            .from("laptops")
            .select("*", { count: "estimated" });

        if (activeFilters.brand) query = query.ilike("brand", `%${activeFilters.brand}%`);
        if (activeFilters.city) query = query.ilike("city", `%${activeFilters.city}%`);
        if (activeFilters.gpu_type !== "all") query = query.ilike("gpu_type", activeFilters.gpu_type);
        if (activeFilters.price_min) query = query.gte("price", parseFloat(activeFilters.price_min));
        if (activeFilters.price_max) query = query.lte("price", parseFloat(activeFilters.price_max));
        if (activeFilters.ram_min) query = query.gte("ram", parseFloat(activeFilters.ram_min));
        if (activeFilters.storage_min) query = query.gte("storage", parseFloat(activeFilters.storage_min));
        if (activeFilters.gpu_vram_min) query = query.gte("gpu_vram", parseFloat(activeFilters.gpu_vram_min));
        if (activeFilters.screen_size_min) query = query.gte("screen_size", parseFloat(activeFilters.screen_size_min));
        if (activeFilters.refresh_rate_min) query = query.gte("refresh_rate", parseFloat(activeFilters.refresh_rate_min));
        if (activeFilters.is_new === true) query = query.eq("new", 1);
        if (activeFilters.is_shop === true) query = query.eq("is_shop", true);
        if (activeFilters.has_delivery === true) query = query.eq("has_delivery", true);
        if (activeFilters.touchscreen === true) query = query.eq("touchscreen", 1);
        if (activeFilters.ssd === true) query = query.eq("ssd", 1);
        if (activeFilters.hide_sold) query = query.eq("is_sold", false);

        if (sortBy === "score") {
            query = query.order("score", { ascending: false, nullsFirst: false });
        } else if (sortBy === "price") {
            query = query.order("price", { ascending: false, nullsFirst: false });
        }

        query = query.range(from, to);

        const { data, count, error } = await query;
        if (error?.message) console.error("Fetch error:", error.message);
        setLaptops(data || []);
        setTotal(count || 0);
        setLoading(false);
    }, [page, committedSearch, activeFilters, sortBy]);

    useEffect(() => {
        fetchLaptops();
    }, [fetchLaptops]);

    const totalPages = Math.ceil(total / PAGE_SIZE);

    const handleSearch = (e: React.FormEvent) => {
        e.preventDefault();
        setPage(0);
        setCommittedSearch(search);
    };

    const applyFilters = () => {
        setActiveFilters({ ...filters });
        setPage(0);
        setFilterOpen(false);
    };

    const clearFilters = () => {
        setFilters({ ...emptyFilters });
        setActiveFilters({ ...emptyFilters });
        setPage(0);
        setFilterOpen(false);
    };

    const activeFilterCount = [
        activeFilters.brand,
        activeFilters.city,
        activeFilters.gpu_type !== "all" ? "y" : "",
        activeFilters.price_min,
        activeFilters.price_max,
        activeFilters.ram_min,
        activeFilters.storage_min,
        activeFilters.gpu_vram_min,
        activeFilters.screen_size_min,
        activeFilters.refresh_rate_min,
        activeFilters.is_new !== null ? "y" : "",
        activeFilters.is_shop !== null ? "y" : "",
        activeFilters.has_delivery !== null ? "y" : "",
        activeFilters.touchscreen !== null ? "y" : "",
        activeFilters.ssd !== null ? "y" : "",
        !activeFilters.hide_sold ? "y" : "",
    ].filter(Boolean).length;

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold tracking-tight">Avito laptops</h1>
                    <div className="flex items-center gap-3 mt-1 text-sm text-muted-foreground">
                        <p>{total.toLocaleString()} laptops found</p>
                        <Dialog>
                            <DialogTrigger render={
                                <button className="text-blue-400 hover:text-blue-300 transition-colors bg-blue-500/10 hover:bg-blue-500/20 px-2 py-0.5 rounded text-xs font-medium border border-blue-500/20">
                                    Insights
                                </button>
                            } />
                            <DialogContent className="sm:max-w-md">
                                <DialogHeader>
                                    <DialogTitle>Pipeline Statistics</DialogTitle>
                                </DialogHeader>
                                {stats ? (
                                    <>
                                        <div className="grid grid-cols-2 gap-4 py-4">
                                            <div className="space-y-1">
                                                <p className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">Active Inventory</p>
                                                <p className="text-3xl font-bold font-mono tracking-tight">{stats.total_items.toLocaleString()}</p>
                                                <p className="text-xs text-muted-foreground">Stored laptops</p>
                                            </div>
                                            <div className="space-y-1">
                                                <p className="text-[11px] font-semibold uppercase tracking-wider text-emerald-500">Newly Added</p>
                                                <p className="text-3xl font-bold font-mono tracking-tight text-emerald-400">+{stats.new_items.toLocaleString()}</p>
                                                <p className="text-xs text-muted-foreground">in the last 24h</p>
                                            </div>
                                            <div className="space-y-1">
                                                <p className="text-[11px] font-semibold uppercase tracking-wider text-blue-500">Price Drops</p>
                                                <p className="text-3xl font-bold font-mono tracking-tight text-blue-400">{stats.updated_items.toLocaleString()}</p>
                                                <p className="text-xs text-muted-foreground">Updates processed</p>
                                            </div>
                                            <div className="space-y-1">
                                                <p className="text-[11px] font-semibold uppercase tracking-wider text-red-500">Sold Items</p>
                                                <p className="text-3xl font-bold font-mono tracking-tight text-red-400">-{stats.sold_items.toLocaleString()}</p>
                                                <p className="text-xs text-muted-foreground">Removed remotely</p>
                                            </div>
                                        </div>
                                        <div className="text-[11px] text-muted-foreground flex items-center justify-between border-t border-border/50 pt-3 mt-1">
                                            <span className="flex items-center gap-1.5 font-medium">
                                                <span className="relative flex h-2 w-2">
                                                    <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                                                    <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
                                                </span>
                                                Updated: {new Date(stats.created_at).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })}
                                            </span>
                                            <span>Gemini 3.1 & BAAI Embeddings</span>
                                        </div>
                                    </>
                                ) : (
                                    <div className="py-8 text-center space-y-3">
                                        <div className="mx-auto flex h-10 w-10 items-center justify-center rounded-full bg-muted">
                                            <svg className="h-5 w-5 text-muted-foreground" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="2"><path strokeLinecap="round" strokeLinejoin="round" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
                                        </div>
                                        <p className="text-sm text-muted-foreground">Statistics currently syncing...</p>
                                    </div>
                                )}
                            </DialogContent>
                        </Dialog>
                    </div>
                </div>
            </div>

            {/* search + sort + filter bar */}
            <div className="flex gap-2 items-center w-full">
                <form onSubmit={handleSearch} className="flex flex-1 gap-2">
                    <Input
                        placeholder="Search laptops..."
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                        className="flex-1"
                    />
                    <Button type="submit" variant="outline">
                        Search
                    </Button>
                </form>
                <Select value={sortBy} onValueChange={(v) => { setSortBy(v as SortOption); setPage(0); }}>
                    <SelectTrigger className="w-[120px]">
                        <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                        <SelectItem value="default">Default</SelectItem>
                        <SelectItem value="score">Score</SelectItem>
                        <SelectItem value="price">Price</SelectItem>
                        <SelectItem value="value">Value</SelectItem>
                    </SelectContent>
                </Select>
                <Dialog open={filterOpen} onOpenChange={setFilterOpen}>
                    <DialogTrigger
                        render={
                            <Button variant="outline">
                                Filters
                                {activeFilterCount > 0 && (
                                    <Badge variant="secondary" className="ml-1.5 h-5 px-1.5 text-[10px] leading-none">
                                        {activeFilterCount}
                                    </Badge>
                                )}
                            </Button>
                        }
                    />
                    <DialogContent className="max-w-lg">
                        <DialogHeader>
                            <DialogTitle>Filter laptops</DialogTitle>
                        </DialogHeader>
                        <div className="grid gap-4 py-4">
                            {/* text search filters */}
                            <div className="grid grid-cols-3 gap-3">
                                <div className="space-y-1.5">
                                    <label className="text-xs text-muted-foreground">Brand</label>
                                    <Input
                                        placeholder="e.g. Lenovo"
                                        value={filters.brand}
                                        onChange={(e) => setFilters({ ...filters, brand: e.target.value })}
                                        className="h-9 text-sm"
                                    />
                                </div>
                                <div className="space-y-1.5">
                                    <label className="text-xs text-muted-foreground">City</label>
                                    <Input
                                        placeholder="e.g. Casablanca"
                                        value={filters.city}
                                        onChange={(e) => setFilters({ ...filters, city: e.target.value })}
                                        className="h-9 text-sm"
                                    />
                                </div>
                                <div className="space-y-1.5">
                                    <label className="text-xs text-muted-foreground">GPU type</label>
                                    <Select
                                        value={filters.gpu_type}
                                        onValueChange={(v) => setFilters({ ...filters, gpu_type: String(v) })}
                                    >
                                        <SelectTrigger className="h-9 text-sm">
                                            <SelectValue placeholder="Any" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="all">Any</SelectItem>
                                            {gpuTypes.map((g) => (
                                                <SelectItem key={g} value={g}>{g}</SelectItem>
                                            ))}
                                        </SelectContent>
                                    </Select>
                                </div>
                            </div>

                            {/* range filters: price */}
                            <div className="grid grid-cols-3 gap-3">
                                <div className="space-y-1.5">
                                    <label className="text-xs text-muted-foreground">Price (DH)</label>
                                    <div className="flex gap-1">
                                        <Input type="number" placeholder="Min" value={filters.price_min} onChange={(e) => setFilters({ ...filters, price_min: e.target.value })} className="h-8 text-sm" />
                                        <Input type="number" placeholder="Max" value={filters.price_max} onChange={(e) => setFilters({ ...filters, price_max: e.target.value })} className="h-8 text-sm" />
                                    </div>
                                </div>
                                <div className="space-y-1.5">
                                    <label className="text-xs text-muted-foreground">Min RAM (GB)</label>
                                    <Input type="number" placeholder="e.g. 8" value={filters.ram_min} onChange={(e) => setFilters({ ...filters, ram_min: e.target.value })} className="h-8 text-sm" />
                                </div>
                                <div className="space-y-1.5">
                                    <label className="text-xs text-muted-foreground">Min Storage (GB)</label>
                                    <Input type="number" placeholder="e.g. 256" value={filters.storage_min} onChange={(e) => setFilters({ ...filters, storage_min: e.target.value })} className="h-8 text-sm" />
                                </div>
                            </div>

                            {/* min specs row */}
                            <div className="grid grid-cols-3 gap-3">
                                <div className="space-y-1.5">
                                    <label className="text-xs text-muted-foreground">Min GPU VRAM (GB)</label>
                                    <Input type="number" placeholder="e.g. 4" value={filters.gpu_vram_min} onChange={(e) => setFilters({ ...filters, gpu_vram_min: e.target.value })} className="h-8 text-sm" />
                                </div>
                                <div className="space-y-1.5">
                                    <label className="text-xs text-muted-foreground">Min Screen (&quot;)</label>
                                    <Input type="number" placeholder="e.g. 14" value={filters.screen_size_min} onChange={(e) => setFilters({ ...filters, screen_size_min: e.target.value })} className="h-8 text-sm" />
                                </div>
                                <div className="space-y-1.5">
                                    <label className="text-xs text-muted-foreground">Min Refresh (Hz)</label>
                                    <Input type="number" placeholder="e.g. 120" value={filters.refresh_rate_min} onChange={(e) => setFilters({ ...filters, refresh_rate_min: e.target.value })} className="h-8 text-sm" />
                                </div>
                            </div>

                            {/* boolean filters */}
                            <div className="flex flex-wrap gap-x-6 gap-y-3 pt-1">
                                <div className="flex items-center gap-2">
                                    <Switch checked={filters.is_new === true} onCheckedChange={(v) => setFilters({ ...filters, is_new: v ? true : null })} />
                                    <label className="text-sm">New only</label>
                                </div>
                                <div className="flex items-center gap-2">
                                    <Switch checked={filters.ssd === true} onCheckedChange={(v) => setFilters({ ...filters, ssd: v ? true : null })} />
                                    <label className="text-sm">SSD</label>
                                </div>
                                <div className="flex items-center gap-2">
                                    <Switch checked={filters.touchscreen === true} onCheckedChange={(v) => setFilters({ ...filters, touchscreen: v ? true : null })} />
                                    <label className="text-sm">Touchscreen</label>
                                </div>
                                <div className="flex items-center gap-2">
                                    <Switch checked={filters.is_shop === true} onCheckedChange={(v) => setFilters({ ...filters, is_shop: v ? true : null })} />
                                    <label className="text-sm">Shop</label>
                                </div>
                                <div className="flex items-center gap-2 border-l border-border pl-6 ml-2">
                                    <Switch checked={filters.hide_sold} onCheckedChange={(v) => setFilters({ ...filters, hide_sold: v })} />
                                    <label className="text-sm font-medium">Hide sold items</label>
                                </div>
                            </div>

                            {/* actions */}
                            <div className="flex gap-2 pt-2">
                                <Button onClick={applyFilters} className="flex-1">Apply</Button>
                                <Button variant="outline" onClick={clearFilters} className="flex-1">Clear</Button>
                            </div>
                        </div>
                    </DialogContent>
                </Dialog>
            </div>

            {/* data table */}
            <div className="rounded-md border">
                <Table>
                    <TableHeader>
                        <TableRow>
                            {activeCols.map((c) => (
                                <TableHead
                                    key={c.key}
                                    className={[c.width, c.align === "right" ? "text-right" : c.align === "center" ? "text-center" : ""].filter(Boolean).join(" ")}
                                >
                                    {c.label}
                                </TableHead>
                            ))}
                            {/* column toggle button */}
                            <TableHead className="w-[40px] p-0">
                                <div className="relative">
                                    <button
                                        onClick={() => setColMenuOpen(!colMenuOpen)}
                                        className="w-full h-full flex items-center justify-center hover:text-foreground text-muted-foreground transition-colors p-2"
                                        title="Toggle columns"
                                    >
                                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                            <path d="M6 9l6 6 6-6" />
                                        </svg>
                                    </button>
                                    {colMenuOpen && (
                                        <>
                                            {/* backdrop */}
                                            <div className="fixed inset-0 z-40" onClick={() => setColMenuOpen(false)} />
                                            {/* menu */}
                                            <div className="absolute right-0 top-full mt-1 z-50 w-48 rounded-md border bg-popover p-2 shadow-lg">
                                                <p className="text-xs text-muted-foreground mb-2 px-1">Columns</p>
                                                {ALL_COLUMNS.map((c) => (
                                                    <label
                                                        key={c.key}
                                                        className="flex items-center gap-2 px-1 py-1 hover:bg-accent rounded cursor-pointer text-sm"
                                                    >
                                                        <input
                                                            type="checkbox"
                                                            checked={visibleCols.has(c.key)}
                                                            onChange={() => toggleCol(c.key)}
                                                            className="accent-emerald-500"
                                                        />
                                                        {c.label}
                                                    </label>
                                                ))}
                                            </div>
                                        </>
                                    )}
                                </div>
                            </TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {loading ? (
                            <TableRow>
                                <TableCell colSpan={activeCols.length + 1} className="h-48 text-center">
                                    <span className="text-muted-foreground">Loading...</span>
                                </TableCell>
                            </TableRow>
                        ) : laptops.length === 0 ? (
                            <TableRow>
                                <TableCell colSpan={activeCols.length + 1} className="h-48 text-center">
                                    <span className="text-muted-foreground">No results</span>
                                </TableCell>
                            </TableRow>
                        ) : (
                            laptops.map((l) => (
                                <TableRow key={l.id}>
                                    {activeCols.map((c) => (
                                        <TableCell
                                            key={c.key}
                                            className={c.align === "right" ? "text-right" : c.align === "center" ? "text-center" : ""}
                                        >
                                            {c.render(l)}
                                        </TableCell>
                                    ))}
                                    <TableCell className="w-[40px]" />
                                </TableRow>
                            ))
                        )}
                    </TableBody>
                </Table>
            </div >

            {/* pagination */}
            {
                totalPages > 1 && (
                    <div className="flex items-center justify-between">
                        <p className="text-sm text-muted-foreground">
                            Page {page + 1} of {totalPages}
                        </p>
                        <div className="flex gap-1">
                            <Button variant="outline" size="sm" disabled={page === 0} onClick={() => setPage(page - 1)}>
                                Previous
                            </Button>
                            {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                                let p: number;
                                if (totalPages <= 5) {
                                    p = i;
                                } else if (page < 3) {
                                    p = i;
                                } else if (page > totalPages - 4) {
                                    p = totalPages - 5 + i;
                                } else {
                                    p = page - 2 + i;
                                }
                                return (
                                    <Button key={p} variant={p === page ? "default" : "outline"} size="sm" onClick={() => setPage(p)}>
                                        {p + 1}
                                    </Button>
                                );
                            })}
                            <Button variant="outline" size="sm" disabled={page >= totalPages - 1} onClick={() => setPage(page + 1)}>
                                Next
                            </Button>
                        </div>
                    </div>
                )
            }

        </div >
    );
}
