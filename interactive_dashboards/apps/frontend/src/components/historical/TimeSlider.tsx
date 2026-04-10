import { useCallback } from 'react'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'

interface TimeSliderProps {
  fetchRange: { start: string; end: string }
  bin: 'day' | 'month' | 'year'
  onStartDateChange: (date: string) => void
  onEndDateChange: (date: string) => void
  onBinChange: (bin: 'day' | 'month' | 'year') => void
}

const MIN_DATE = '2017-01-01'
const MAX_DATE = '2022-12-31'

export function TimeSlider({
  fetchRange,
  bin,
  onStartDateChange,
  onEndDateChange,
  onBinChange,
}: TimeSliderProps) {
  const handleStartInputChange = useCallback(
    (value: string) => {
      if (!value) return
      onStartDateChange(value)
    },
    [onStartDateChange]
  )

  const handleEndInputChange = useCallback(
    (value: string) => {
      if (!value) return
      onEndDateChange(value)
    },
    [onEndDateChange]
  )

  return (
    <div className="flex flex-wrap items-center justify-center gap-3">
      <div className="flex items-center gap-2">
        <span className="text-xs font-medium text-muted-foreground">Start</span>
        <Input
          type="date"
          value={fetchRange.start}
          max={fetchRange.end || MAX_DATE}
          min={MIN_DATE}
          onChange={(event) => handleStartInputChange(event.target.value)}
          className="h-9 w-40"
        />
      </div>

      <div className="flex items-center gap-2">
        <span className="text-xs font-medium text-muted-foreground">End</span>
        <Input
          type="date"
          value={fetchRange.end}
          max={MAX_DATE}
          min={fetchRange.start || MIN_DATE}
          onChange={(event) => handleEndInputChange(event.target.value)}
          className="h-9 w-40"
        />
      </div>

      <div className="flex items-center gap-2">
        <span className="text-xs font-medium text-muted-foreground">Interval</span>
        <Select value={bin} onValueChange={(v) => onBinChange(v as 'day' | 'month' | 'year')}>
          <SelectTrigger className="h-9 w-28">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="day">Day</SelectItem>
            <SelectItem value="month">Month</SelectItem>
            <SelectItem value="year">Year</SelectItem>
          </SelectContent>
        </Select>
      </div>
    </div>
  )
}
