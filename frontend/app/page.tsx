import Link from "next/link";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function Home() {
  return (
    <div className="space-y-8">
      <section className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">Hajji</h1>
        <p className="text-muted-foreground">
          Data projects portfolio. Explore the projects below.
        </p>
      </section>

      <div className="grid gap-4 md:grid-cols-2">
        <Link href="/avito">
          <Card className="h-full transition-colors hover:bg-accent/50">
            <CardHeader>
              <CardTitle className="text-lg">Avito laptops</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground">
                Browse, search and filter 15,000+ laptop listings scraped from
                Avito.ma. Find the best deals with smart filtering and
                similarity search.
              </p>
            </CardContent>
          </Card>
        </Link>

        <Link href="/epl">
          <Card className="h-full transition-colors hover:bg-accent/50">
            <CardHeader>
              <CardTitle className="text-lg">Premier league</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground">
                Predicted standings and match results using Monte Carlo
                simulations. Coming soon.
              </p>
            </CardContent>
          </Card>
        </Link>
      </div>
    </div>
  );
}
