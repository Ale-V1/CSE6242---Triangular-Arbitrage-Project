import { HistoricalDashboard } from '@/components/historical/HistoricalDashboard'
import { LiveDashboard } from '@/components/live/LiveDashboard'
import { useAppContext } from '@/context/AppContext'

export function Dashboard() {
  const { mode } = useAppContext()

  return mode === 'historical' ? <HistoricalDashboard /> : <LiveDashboard />
}
