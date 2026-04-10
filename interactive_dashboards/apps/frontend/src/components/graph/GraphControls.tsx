import { SlidersHorizontal } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from '@/components/ui/sheet'
import { Slider } from '@/components/ui/slider'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'

interface GraphControlsProps {
  minFrequency: number
  minProfitPct: number
  onMinFrequencyChange: (value: number) => void
  onMinProfitPctChange: (value: number) => void
}

export function GraphControls({
  minFrequency,
  minProfitPct,
  onMinFrequencyChange,
  onMinProfitPctChange,
}: GraphControlsProps) {
  return (
    <Sheet>
      <TooltipProvider>
        <Tooltip>
          <TooltipTrigger asChild>
            <SheetTrigger asChild>
              <Button variant="ghost" size="icon" className="h-8 w-8">
                <SlidersHorizontal className="h-4 w-4" />
              </Button>
            </SheetTrigger>
          </TooltipTrigger>
          <TooltipContent>
            <p>Graph Filters</p>
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>

      <SheetContent>
        <SheetHeader>
          <SheetTitle>Graph Filters</SheetTitle>
        </SheetHeader>

        <div className="mt-6 space-y-6">
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium">Min Frequency</label>
              <Input
                type="number"
                value={minFrequency}
                onChange={(e) => onMinFrequencyChange(Number(e.target.value))}
                className="w-20 text-right"
              />
            </div>
            <Slider
              value={[minFrequency]}
              min={0}
              max={1000}
              step={10}
              onValueChange={([v]) => onMinFrequencyChange(v)}
            />
            <p className="text-xs text-muted-foreground">
              Hide edges with fewer than {minFrequency} opportunities
            </p>
          </div>

          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium">Min Profit %</label>
              <Input
                type="number"
                value={minProfitPct}
                onChange={(e) => onMinProfitPctChange(Number(e.target.value))}
                className="w-20 text-right"
                step={0.01}
              />
            </div>
            <Slider
              value={[minProfitPct * 100]}
              min={0}
              max={100}
              step={1}
              onValueChange={([v]) => onMinProfitPctChange(v / 100)}
            />
            <p className="text-xs text-muted-foreground">
              Hide opportunities below {minProfitPct.toFixed(2)}% profit
            </p>
          </div>
        </div>
      </SheetContent>
    </Sheet>
  )
}
