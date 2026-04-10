import {
  createContext,
  type Dispatch,
  type ReactNode,
  type SetStateAction,
  useCallback,
  useContext,
  useState,
} from 'react'
import type { AppMode } from '@/types'

interface AppContextValue {
  mode: AppMode
  setMode: (mode: AppMode) => void
  fetchRange: { start: string; end: string }
  setFetchRange: Dispatch<SetStateAction<{ start: string; end: string }>>
  bin: 'day' | 'month' | 'year'
  setBin: Dispatch<SetStateAction<'day' | 'month' | 'year'>>
  selectedNode: string | null
  setSelectedNode: (node: string | null) => void
  selectedTriangle: string | null
  setSelectedTriangle: (triangle: string | null) => void
  detailPanelOpen: boolean
  setDetailPanelOpen: (open: boolean) => void
  openNodeDetail: (nodeId: string) => void
  openTriangleDetail: (triangleKey: string) => void
  closeDetail: () => void
}

const AppContext = createContext<AppContextValue | null>(null)

const DEFAULT_START = '2017-01-01'
const DEFAULT_END = '2022-12-31'

function getInitialMode(): AppMode {
  if (typeof window === 'undefined') return 'historical'
  const stored = localStorage.getItem('app-mode')
  return stored === 'live' ? 'live' : 'historical'
}

export function AppProvider({ children }: { children: ReactNode }) {
  const [mode, setModeState] = useState<AppMode>(getInitialMode)
  const [fetchRange, setFetchRange] = useState({ start: DEFAULT_START, end: DEFAULT_END })
  const [bin, setBin] = useState<'day' | 'month' | 'year'>('month')
  const [selectedNode, setSelectedNode] = useState<string | null>(null)
  const [selectedTriangle, setSelectedTriangle] = useState<string | null>(null)
  const [detailPanelOpen, setDetailPanelOpen] = useState(false)

  const setMode = useCallback((newMode: AppMode) => {
    setModeState(newMode)
    localStorage.setItem('app-mode', newMode)
  }, [])

  const openNodeDetail = useCallback((nodeId: string) => {
    setSelectedNode(nodeId)
    setSelectedTriangle(null)
    setDetailPanelOpen(true)
  }, [])

  const openTriangleDetail = useCallback((triangleKey: string) => {
    setSelectedTriangle(triangleKey)
    setSelectedNode(null)
    setDetailPanelOpen(true)
  }, [])

  const closeDetail = useCallback(() => {
    setDetailPanelOpen(false)
    setSelectedNode(null)
    setSelectedTriangle(null)
  }, [])

  return (
    <AppContext.Provider
      value={{
        mode,
        setMode,
        fetchRange,
        setFetchRange,
        bin,
        setBin,
        selectedNode,
        setSelectedNode,
        selectedTriangle,
        setSelectedTriangle,
        detailPanelOpen,
        setDetailPanelOpen,
        openNodeDetail,
        openTriangleDetail,
        closeDetail,
      }}
    >
      {children}
    </AppContext.Provider>
  )
}

export function useAppContext() {
  const context = useContext(AppContext)
  if (!context) {
    throw new Error('useAppContext must be used within AppProvider')
  }
  return context
}
