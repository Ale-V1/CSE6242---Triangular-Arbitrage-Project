import { Activity, History } from 'lucide-react'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { useAppContext } from '@/context/AppContext'
import type { AppMode } from '@/types'

export function ModeToggle() {
  const { mode, setMode } = useAppContext()

  return (
    <Tabs value={mode} onValueChange={(v) => setMode(v as AppMode)}>
      <TabsList className="grid w-[100px] grid-cols-2 sm:w-[200px]">
        <TabsTrigger value="historical" className="flex items-center gap-1.5">
          <History className="h-3.5 w-3.5" />
          <span className="hidden sm:inline">Historical</span>
        </TabsTrigger>
        <TabsTrigger value="live" className="flex items-center gap-1.5">
          <Activity className="h-3.5 w-3.5" />
          <span className="hidden sm:inline">Live</span>
        </TabsTrigger>
      </TabsList>
    </Tabs>
  )
}
