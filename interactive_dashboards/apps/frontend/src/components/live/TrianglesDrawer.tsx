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
import type { Triangle } from '@/types'

interface TrianglesDrawerProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  triangles: Triangle[]
}

export function TrianglesDrawer({ open, onOpenChange, triangles }: TrianglesDrawerProps) {
  const [search, setSearch] = useState('')

  const sortedTriangles = useMemo(
    () => [...triangles].sort((a, b) => a.key.localeCompare(b.key)),
    [triangles]
  )

  const filtered = useMemo(() => {
    if (!search) return sortedTriangles
    const term = search.toUpperCase()
    return sortedTriangles.filter(
      (t) =>
        t.key.toUpperCase().includes(term) ||
        t.currencies.some((c) => c.includes(term)) ||
        t.pairs.some((p) => p.includes(term))
    )
  }, [sortedTriangles, search])

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="flex flex-col gap-0">
        <SheetHeader>
          <SheetTitle>Triangles</SheetTitle>
          <SheetDescription>
            {triangles.length} triangular paths detected from your selected coins and pairs.
          </SheetDescription>
        </SheetHeader>

        <div className="px-4">
          <div className="relative">
            <Search className="absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="Search triangles..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-9"
            />
          </div>
          <p className="mt-2 text-xs text-muted-foreground">
            {filtered.length} of {triangles.length} triangles
          </p>
        </div>

        <ScrollArea className="min-h-0 flex-1 px-4 py-2">
          <div className="flex flex-col gap-1">
            {filtered.map((tri) => (
              <div key={tri.key} className="rounded-md px-2 py-1.5 text-sm">
                <div className="flex items-center gap-1.5">
                  {tri.currencies.map((c, i) => (
                    <span key={c} className="flex items-center gap-1.5">
                      {i > 0 && <span className="text-muted-foreground">→</span>}
                      <span className="font-medium">{c}</span>
                    </span>
                  ))}
                </div>
                <div className="mt-0.5 flex flex-wrap gap-1">
                  {tri.pairs.map((p) => (
                    <Badge key={p} variant="secondary" className="text-xs">
                      {p}
                    </Badge>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </ScrollArea>
      </SheetContent>
    </Sheet>
  )
}
