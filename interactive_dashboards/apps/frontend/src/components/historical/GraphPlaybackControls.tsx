import { Pause, Play, SkipBack, SkipForward } from 'lucide-react'
import { useCallback, useEffect, useRef, useState } from 'react'
import { Button } from '@/components/ui/button'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Slider } from '@/components/ui/slider'
import { formatDate } from '@/lib/utils'
import type { GraphSnapshot } from '@/types'

interface GraphPlaybackControlsProps {
  snapshots: GraphSnapshot[]
  currentIndex: number
  onAnimationPositionChange: (index: number) => void
}

// Debounce delay in milliseconds for slider scrubbing
const SLIDER_DEBOUNCE_MS = 16 // ~60fps

export function GraphPlaybackControls({
  snapshots,
  currentIndex,
  onAnimationPositionChange,
}: GraphPlaybackControlsProps) {
  const [isPlaying, setIsPlaying] = useState(false)
  const [playbackSpeed, setPlaybackSpeed] = useState(1)
  const [localIndex, setLocalIndex] = useState(currentIndex)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const maxIndex = Math.max(0, snapshots.length - 1)
  const currentSnapshot = snapshots[localIndex]

  // Sync localIndex with currentIndex when it changes externally
  useEffect(() => {
    setLocalIndex(currentIndex)
  }, [currentIndex])

  // Debounced slider change handler for smooth scrubbing
  const handleSliderChange = useCallback(
    (values: number[]) => {
      const newIndex = values[0]
      // Update local state immediately for responsive UI
      setLocalIndex(newIndex)

      // Debounce the actual graph update
      if (debounceRef.current) {
        clearTimeout(debounceRef.current)
      }
      debounceRef.current = setTimeout(() => {
        onAnimationPositionChange(newIndex)
      }, SLIDER_DEBOUNCE_MS)
    },
    [onAnimationPositionChange]
  )

  // Cleanup debounce on unmount
  useEffect(() => {
    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current)
      }
    }
  }, [])

  const stepForward = useCallback(() => {
    if (localIndex < maxIndex) {
      const newIndex = localIndex + 1
      setLocalIndex(newIndex)
      onAnimationPositionChange(newIndex)
    }
  }, [localIndex, maxIndex, onAnimationPositionChange])

  const stepBackward = useCallback(() => {
    if (localIndex > 0) {
      const newIndex = localIndex - 1
      setLocalIndex(newIndex)
      onAnimationPositionChange(newIndex)
    }
  }, [localIndex, onAnimationPositionChange])

  useEffect(() => {
    if (isPlaying) {
      intervalRef.current = setInterval(() => {
        stepForward()
      }, 1000 / playbackSpeed)
    } else if (intervalRef.current) {
      clearInterval(intervalRef.current)
      intervalRef.current = null
    }

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
      }
    }
  }, [isPlaying, playbackSpeed, stepForward])

  useEffect(() => {
    if (localIndex >= maxIndex) {
      setIsPlaying(false)
    }
  }, [localIndex, maxIndex])

  return (
    <div className="flex flex-wrap items-center gap-4">
      <div className="flex items-center gap-1">
        <Button
          variant="ghost"
          size="icon"
          onClick={stepBackward}
          disabled={localIndex <= 0 || snapshots.length === 0}
          aria-label="Step backward"
        >
          <SkipBack className="h-4 w-4" />
        </Button>
        <Button
          variant="ghost"
          size="icon"
          onClick={() => setIsPlaying(!isPlaying)}
          disabled={snapshots.length === 0}
          aria-label={isPlaying ? 'Pause playback' : 'Play playback'}
        >
          {isPlaying ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
        </Button>
        <Button
          variant="ghost"
          size="icon"
          onClick={stepForward}
          disabled={localIndex >= maxIndex || snapshots.length === 0}
          aria-label="Step forward"
        >
          <SkipForward className="h-4 w-4" />
        </Button>
      </div>

      <div className="flex items-center gap-2 text-sm">
        <span className="font-medium">
          {currentSnapshot ? formatDate(currentSnapshot.date) : 'No data'}
        </span>
        {snapshots.length > 0 && (
          <span className="text-muted-foreground">
            ({localIndex + 1} / {snapshots.length})
          </span>
        )}
      </div>

      <div className="min-w-0 flex-1">
        <Slider
          value={[localIndex]}
          min={0}
          max={maxIndex}
          step={1}
          onValueChange={handleSliderChange}
          disabled={snapshots.length === 0}
        />
      </div>

      <div className="flex items-center gap-2">
        <span className="hidden text-sm font-medium sm:inline">Speed:</span>
        <Select value={String(playbackSpeed)} onValueChange={(v) => setPlaybackSpeed(Number(v))}>
          <SelectTrigger className="w-20">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="0.5">0.5x</SelectItem>
            <SelectItem value="1">1x</SelectItem>
            <SelectItem value="2">2x</SelectItem>
            <SelectItem value="4">4x</SelectItem>
          </SelectContent>
        </Select>
      </div>
    </div>
  )
}
