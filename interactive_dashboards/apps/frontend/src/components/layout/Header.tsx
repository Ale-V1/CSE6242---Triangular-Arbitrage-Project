import { Activity, BarChart3 } from 'lucide-react'
import { TimeSlider } from '@/components/historical/TimeSlider'
import { useAppContext } from '@/context/AppContext'
import { ModeToggle } from './ModeToggle'

export function Header() {
  const { mode, fetchRange, setFetchRange, bin, setBin } = useAppContext()

  const handleStartDateChange = (date: string) => {
    setFetchRange((current) => ({ ...current, start: date }))
  }

  const handleEndDateChange = (date: string) => {
    setFetchRange((current) => ({ ...current, end: date }))
  }

  const handleBinChange = (newBin: 'day' | 'month' | 'year') => {
    setBin(newBin)
  }

  return (
    <header className="border-b border-border bg-card">
      <div className="container mx-auto flex flex-col gap-2 px-4 py-2 md:grid md:min-h-16 md:grid-cols-[1fr_auto_1fr] md:items-center md:gap-4">
        <div className="flex items-center justify-between gap-3">
          <div className="flex items-center gap-2 text-primary">
            {mode === 'live' ? <Activity className="h-5 w-5" /> : <BarChart3 className="h-5 w-5" />}
            <h1 className="text-lg font-semibold">Triangular Arbitrage</h1>
          </div>
          <span className="hidden text-sm text-muted-foreground sm:inline">
            {mode === 'live' ? 'Live Detection' : 'Historical Analysis'}
          </span>
          <div className="md:hidden">
            <ModeToggle />
          </div>
        </div>

        <div className="flex justify-center">
          {mode === 'historical' ? (
            <TimeSlider
              fetchRange={fetchRange}
              bin={bin}
              onStartDateChange={handleStartDateChange}
              onEndDateChange={handleEndDateChange}
              onBinChange={handleBinChange}
            />
          ) : null}
        </div>

        <div className="hidden items-center justify-end gap-4 md:flex">
          <ModeToggle />
        </div>
      </div>
    </header>
  )
}
