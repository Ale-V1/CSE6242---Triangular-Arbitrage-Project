import { useEffect, useState } from 'react'

export function useElapsedTime(startTime: number): string {
  const [now, setNow] = useState(Date.now)

  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(id)
  }, [])

  // Reset displayed time immediately when startTime changes
  // biome-ignore lint/correctness/useExhaustiveDependencies: startTime triggers an immediate reset
  useEffect(() => {
    setNow(Date.now())
  }, [startTime])

  const elapsed = Math.max(0, Math.floor((now - startTime) / 1000))
  const days = Math.floor(elapsed / 86400)
  const hours = Math.floor((elapsed % 86400) / 3600)
  const minutes = Math.floor((elapsed % 3600) / 60)

  if (days > 0) return `${days}d ${hours}h ${minutes}m`
  if (hours > 0) return `${hours}h ${minutes}m`
  return `${minutes}m`
}
