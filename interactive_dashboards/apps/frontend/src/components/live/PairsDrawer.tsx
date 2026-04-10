import { Search } from 'lucide-react'
import { useMemo, useState } from 'react'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet'
import type { BinanceSymbol } from '@/types'

interface PairsDrawerProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  pairs: BinanceSymbol[]
}

export function PairsDrawer({ open, onOpenChange, pairs }: PairsDrawerProps) {
  const [search, setSearch] = useState('')

  const sortedPairs = useMemo(
    () => [...pairs].sort((a, b) => a.symbol.localeCompare(b.symbol)),
    [pairs]
  )

  const filtered = useMemo(() => {
    if (!search) return sortedPairs
    const term = search.toUpperCase()
    return sortedPairs.filter(
      (p) => p.symbol.includes(term) || p.baseAsset.includes(term) || p.quoteAsset.includes(term)
    )
  }, [sortedPairs, search])

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="flex flex-col gap-0">
        <SheetHeader>
          <SheetTitle>Trading Pairs</SheetTitle>
          <SheetDescription>
            {pairs.length} pairs currently monitored based on your selected coins.
          </SheetDescription>
        </SheetHeader>

        <div className="px-4">
          <div className="relative">
            <Search className="absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="Search pairs..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-9"
            />
          </div>
          <p className="mt-2 text-xs text-muted-foreground">
            {filtered.length} of {pairs.length} pairs
          </p>
        </div>

        <ScrollArea className="min-h-0 flex-1 px-4 py-2">
          <div className="flex flex-col">
            {filtered.map((pair) => (
              <div
                key={pair.symbol}
                className="flex items-center gap-3 rounded-md px-2 py-1.5 text-sm"
              >
                <span className="font-medium">{pair.symbol}</span>
                <span className="ml-auto flex items-center gap-1 text-xs text-muted-foreground">
                  <Badge variant="secondary" className="text-xs">
                    {pair.baseAsset}
                  </Badge>
                  <span>/</span>
                  <Badge variant="secondary" className="text-xs">
                    {pair.quoteAsset}
                  </Badge>
                </span>
              </div>
            ))}
          </div>
        </ScrollArea>
      </SheetContent>
    </Sheet>
  )
}
