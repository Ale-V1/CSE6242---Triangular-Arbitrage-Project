import { Pause, Play, RefreshCw, Trash2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Slider } from '@/components/ui/slider'
import { Switch } from '@/components/ui/switch'
import type { ConnectionStatus as ConnectionStatusType, LiveConfig } from '@/types'
import { ConnectionStatus } from './ConnectionStatus'

interface LiveControlsProps {
  config: LiveConfig
  onConfigChange: (config: LiveConfig) => void
  isPaused: boolean
  onPauseChange: (paused: boolean) => void
  onClear: () => void
  status: ConnectionStatusType
  onReconnect: () => void
  staleMinutes: number
  onStaleMinutesChange: (minutes: number) => void
  showNearMisses: boolean
  onShowNearMissesChange: (show: boolean) => void
}

export function LiveControls({
  config,
  onConfigChange,
  isPaused,
  onPauseChange,
  onClear,
  status,
  onReconnect,
  staleMinutes,
  onStaleMinutesChange,
  showNearMisses,
  onShowNearMissesChange,
}: LiveControlsProps) {
  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <div className="flex flex-wrap items-center gap-4 gap-y-2">
        {/* Connection status */}
        <div className="flex items-center gap-2">
          <ConnectionStatus status={status} />
          {status === 'disconnected' || status === 'error' ? (
            <Button variant="ghost" size="sm" onClick={onReconnect}>
              <RefreshCw className="mr-1 h-3 w-3" />
              Reconnect
            </Button>
          ) : null}
        </div>

        <div className="hidden h-6 w-px bg-border sm:block" />

        {/* Playback controls */}
        <div className="flex items-center gap-2">
          <Button
            variant={isPaused ? 'default' : 'secondary'}
            size="sm"
            onClick={() => onPauseChange(!isPaused)}
          >
            {isPaused ? (
              <>
                <Play className="mr-1 h-3 w-3" />
                Resume
              </>
            ) : (
              <>
                <Pause className="mr-1 h-3 w-3" />
                Pause
              </>
            )}
          </Button>
          <Button variant="ghost" size="sm" onClick={onClear}>
            <Trash2 className="mr-1 h-3 w-3" />
            Clear
          </Button>
        </div>

        <div className="hidden h-6 w-px bg-border sm:block" />

        {/* Fee control */}
        <div className="flex items-center gap-2">
          <label className="text-sm text-muted-foreground">Fee %</label>
          <div className="w-20">
            <Slider
              value={[config.fee * 10]}
              min={0}
              max={10}
              step={1}
              onValueChange={([v]) => onConfigChange({ ...config, fee: v / 10 })}
            />
          </div>
          <span className="w-12 font-mono text-sm">{config.fee.toFixed(2)}%</span>
        </div>

        <div className="hidden h-6 w-px bg-border sm:block" />

        {/* Min profit control */}
        <div className="flex items-center gap-2">
          <label className="text-sm text-muted-foreground">Min Profit</label>
          <div className="w-20">
            <Slider
              value={[config.minProfit * 100]}
              min={0}
              max={200}
              step={1}
              onValueChange={([v]) => onConfigChange({ ...config, minProfit: v / 100 })}
            />
          </div>
          <span className="w-12 font-mono text-sm">{config.minProfit.toFixed(2)}%</span>
        </div>

        <div className="hidden h-6 w-px bg-border sm:block" />

        {/* Notional control */}
        <div className="flex items-center gap-2">
          <label className="text-sm text-muted-foreground">Notional $</label>
          <Input
            type="number"
            value={config.notional}
            onChange={(e) => onConfigChange({ ...config, notional: Number(e.target.value) })}
            className="w-20"
            min={1}
            max={100000}
          />
        </div>

        <div className="hidden h-6 w-px bg-border sm:block" />

        {/* Stale minutes control */}
        <div className="flex items-center gap-2">
          <label className="text-sm text-muted-foreground">Stale</label>
          <div className="w-16">
            <Slider
              value={[staleMinutes]}
              min={1}
              max={30}
              step={1}
              onValueChange={([v]) => onStaleMinutesChange(v)}
            />
          </div>
          <span className="w-10 font-mono text-sm">{staleMinutes}m</span>
        </div>

        <div className="hidden h-6 w-px bg-border sm:block" />

        {/* Near misses toggle + floor */}
        <div className="flex items-center gap-2">
          <label className="flex cursor-pointer items-center gap-1.5">
            <span className="text-sm text-muted-foreground">Near Misses</span>
            <Switch size="sm" checked={showNearMisses} onCheckedChange={onShowNearMissesChange} />
          </label>
          <div className="w-16">
            <Slider
              value={[config.nearMissFloor * 100]}
              min={-100}
              max={0}
              step={5}
              disabled={!showNearMisses}
              onValueChange={([v]) => onConfigChange({ ...config, nearMissFloor: v / 100 })}
            />
          </div>
          <span
            className={`w-14 font-mono text-sm ${!showNearMisses ? 'text-muted-foreground/50' : ''}`}
          >
            {config.nearMissFloor.toFixed(2)}%
          </span>
        </div>
      </div>
    </div>
  )
}
