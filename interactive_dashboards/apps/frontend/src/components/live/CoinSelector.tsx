import { Check, Search } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet'
import type { CoinInfo } from '@/lib/graph'

const MAX_COINS = 10

interface CoinSelectorProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  allCoins: CoinInfo[]
  selectedCoins: string[]
  onSave: (coins: string[]) => void
}

export function CoinSelector({
  open,
  onOpenChange,
  allCoins,
  selectedCoins,
  onSave,
}: CoinSelectorProps) {
  const [draft, setDraft] = useState<Set<string>>(() => new Set(selectedCoins))
  const [search, setSearch] = useState('')

  // Reset draft when drawer opens
  useEffect(() => {
    if (open) {
      setDraft(new Set(selectedCoins))
      setSearch('')
    }
  }, [open, selectedCoins])

  const filteredCoins = useMemo(() => {
    if (!search) return allCoins
    const term = search.toUpperCase()
    return allCoins.filter((c) => c.symbol.includes(term))
  }, [allCoins, search])

  const atMax = draft.size >= MAX_COINS

  function toggle(symbol: string) {
    setDraft((prev) => {
      const next = new Set(prev)
      if (next.has(symbol)) {
        next.delete(symbol)
      } else if (next.size < MAX_COINS) {
        next.add(symbol)
      }
      return next
    })
  }

  function handleSave() {
    onSave([...draft])
    onOpenChange(false)
  }

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="flex flex-col gap-0">
        <SheetHeader>
          <SheetTitle>Select Coins</SheetTitle>
          <SheetDescription>
            Choose up to {MAX_COINS} coins to monitor for triangular arbitrage.
          </SheetDescription>
        </SheetHeader>

        <div className="flex flex-col gap-3 px-4">
          <div className="relative">
            <Search className="absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="Search coins..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-9"
            />
          </div>
          <p className="text-xs text-muted-foreground">
            {draft.size} / {MAX_COINS} selected
            {atMax && ' (max reached)'}
          </p>
        </div>

        <ScrollArea className="min-h-0 flex-1 px-4 py-2">
          <div className="flex flex-col">
            {filteredCoins.map((coin) => {
              const isSelected = draft.has(coin.symbol)
              const isDisabled = !isSelected && atMax

              return (
                <button
                  type="button"
                  key={coin.symbol}
                  onClick={() => toggle(coin.symbol)}
                  disabled={isDisabled}
                  className="flex items-center gap-3 rounded-md px-2 py-1.5 text-sm transition-colors hover:bg-accent disabled:cursor-not-allowed disabled:opacity-40"
                >
                  <span className="flex size-4 shrink-0 items-center justify-center">
                    {isSelected && <Check className="size-4 text-primary" />}
                  </span>
                  <span className="font-medium">{coin.symbol}</span>
                  <Badge variant="secondary" className="ml-auto text-xs">
                    {coin.pairCount}
                  </Badge>
                </button>
              )
            })}
          </div>
        </ScrollArea>

        <SheetFooter>
          <Button onClick={handleSave} disabled={draft.size === 0} className="w-full">
            Save ({draft.size} coin{draft.size !== 1 ? 's' : ''})
          </Button>
        </SheetFooter>
      </SheetContent>
    </Sheet>
  )
}
