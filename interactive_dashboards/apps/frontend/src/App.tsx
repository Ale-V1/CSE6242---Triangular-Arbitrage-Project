import { QueryClientProvider } from '@tanstack/react-query'
import { RouterProvider } from '@tanstack/react-router'
import { AppProvider } from '@/context/AppContext'
import { queryClient } from '@/lib/query-client'
import { router } from '@/routes/router'

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <AppProvider>
        <RouterProvider router={router} />
      </AppProvider>
    </QueryClientProvider>
  )
}
