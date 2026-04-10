import { useCallback, useState } from 'react'

interface StoredEntry<T> {
  value: T
  expiresAt: number
}

const DEFAULT_TTL_MS = 7 * 24 * 60 * 60 * 1000 // 7 days

function readStorage<T>(key: string, fallback: T, ttlMs: number): T {
  try {
    const raw = localStorage.getItem(key)
    if (!raw) return fallback

    const entry: StoredEntry<T> = JSON.parse(raw)
    if (Date.now() > entry.expiresAt) {
      localStorage.removeItem(key)
      return fallback
    }

    // Reset the TTL on read (sliding window)
    entry.expiresAt = Date.now() + ttlMs
    localStorage.setItem(key, JSON.stringify(entry))

    return entry.value
  } catch {
    localStorage.removeItem(key)
    return fallback
  }
}

function writeStorage<T>(key: string, value: T, ttlMs: number): void {
  const entry: StoredEntry<T> = {
    value,
    expiresAt: Date.now() + ttlMs,
  }
  try {
    localStorage.setItem(key, JSON.stringify(entry))
  } catch {
    // Storage full or unavailable — silently ignore
  }
}

/**
 * Like `useState`, but persisted to localStorage with a sliding TTL.
 * The expiry timer resets on every read (mount) and write (setState).
 *
 * @param key - localStorage key (prefix with `live:` or similar to namespace)
 * @param fallback - default value when nothing is stored or entry has expired
 * @param ttlMs - time-to-live in ms (default: 7 days), resets on access
 */
export function usePersistedState<T>(
  key: string,
  fallback: T,
  ttlMs = DEFAULT_TTL_MS
): [T, (value: T | ((prev: T) => T)) => void] {
  const [state, setStateInner] = useState<T>(() => readStorage(key, fallback, ttlMs))

  const setState = useCallback(
    (value: T | ((prev: T) => T)) => {
      setStateInner((prev) => {
        const next = typeof value === 'function' ? (value as (prev: T) => T)(prev) : value
        writeStorage(key, next, ttlMs)
        return next
      })
    },
    [key, ttlMs]
  )

  return [state, setState]
}
