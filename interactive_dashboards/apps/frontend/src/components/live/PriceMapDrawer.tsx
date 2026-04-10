import { Search } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'
import { Input } from '@/components/ui/input'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet'
import type { PriceMapEntry } from '@/types'

interface PriceMapDrawerProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  entries: PriceMapEntry[]
  onRequest: () => void
}

export function PriceMapDrawer({ open, onOpenChange, entries, onRequest }: PriceMapDrawerProps) {
  const [search, setSearch] = useState('')

  // Request fresh data when drawer opens
  useEffect(() => {
    if (open) {
      onRequest()
      setSearch('')
    }
  }, [open, onRequest])

  const sorted = useMemo(
    () => [...entries].sort((a, b) => a.symbol.localeCompare(b.symbol)),
    [entries]
  )

  const filtered = useMemo(() => {
    if (!search) return sorted
    const term = search.toUpperCase()
    return sorted.filter((e) => e.symbol.includes(term))
  }, [sorted, search])

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="flex flex-col gap-0">
        <SheetHeader>
          <SheetTitle>Price Map</SheetTitle>
          <SheetDescription>
            {entries.length} symbols with live bid/ask prices from the WebSocket feed.
          </SheetDescription>
        </SheetHeader>

        <div className="px-4">
          <div className="relative">
            <Search className="absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="Search symbols..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-9"
            />
          </div>
          <p className="mt-2 text-xs text-muted-foreground">
            {filtered.length} of {entries.length} symbols
          </p>
        </div>

        <ScrollArea className="min-h-0 flex-1 px-4 py-2">
          <div className="flex flex-col">
            {filtered.map((entry) => (
              <div
                key={entry.symbol}
                className="flex items-center justify-between rounded-md px-2 py-1.5 text-sm"
              >
                <span className="font-medium">{entry.symbol}</span>
                <div className="flex items-center gap-3 font-mono text-xs text-muted-foreground">
                  <span className="text-green-500">{entry.bid.toPrecision(6)}</span>
                  <span className="text-red-500">{entry.ask.toPrecision(6)}</span>
                </div>
              </div>
            ))}
          </div>
        </ScrollArea>
      </SheetContent>
    </Sheet>
  )
}
