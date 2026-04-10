import { AlertTriangle } from 'lucide-react'

export function Disclaimer() {
  return (
    <footer className="border-t border-border bg-card/50 py-2">
      <div className="container mx-auto px-4">
        <div className="flex items-center justify-center gap-2 text-xs text-muted-foreground">
          <AlertTriangle className="h-3 w-3" />
          <span>
            Educational purposes only. Not financial advice. Arbitrage opportunities shown may not
            be executable due to latency, fees, and market conditions.
          </span>
        </div>
      </div>
    </footer>
  )
}
