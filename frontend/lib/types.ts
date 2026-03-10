export interface Laptop {
    id: number;
    avito_id: string;
    description: string;
    price: number | null;
    city: string | null;
    link: string | null;
    is_shop: boolean;
    has_delivery: boolean;
    brand: string | null;
    model: string | null;
    cpu: string | null;
    ram: number | null;
    storage: number | null;
    ssd: number | null;
    gpu: string | null;
    gpu_type: string | null;
    gpu_vram: number | null;
    screen_size: number | null;
    refresh_rate: number | null;
    new: number | null;
    touchscreen: number | null;
    score: number | null;
    created_at: string;
    updated_at: string;
    is_sold?: boolean;
    similarity?: number;
}
