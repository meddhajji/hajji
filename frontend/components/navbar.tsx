"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useTheme } from "@/components/theme";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

const links = [
    { href: "/", label: "Home" },
    { href: "/avito", label: "Avito" },
    { href: "/epl", label: "EPL" },
];

export function Navbar() {
    const pathname = usePathname();
    const { theme, toggle } = useTheme();

    return (
        <header className="sticky top-0 z-50 border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
            <nav className="mx-auto flex h-14 max-w-6xl items-center justify-between px-4">
                <div className="flex items-center gap-6">
                    <Link href="/" className="text-lg font-semibold tracking-tight">
                        Hajji
                    </Link>
                    <div className="flex items-center gap-1">
                        {links.map((link) => (
                            <Link
                                key={link.href}
                                href={link.href}
                                className={cn(
                                    "rounded-md px-3 py-1.5 text-sm transition-colors",
                                    pathname === link.href
                                        ? "bg-accent text-accent-foreground font-medium"
                                        : "text-muted-foreground hover:text-foreground hover:bg-accent/50"
                                )}
                            >
                                {link.label}
                            </Link>
                        ))}
                    </div>
                </div>
                <Button variant="ghost" size="sm" onClick={toggle}>
                    {theme === "dark" ? "Light" : "Dark"}
                </Button>
            </nav>
        </header>
    );
}
