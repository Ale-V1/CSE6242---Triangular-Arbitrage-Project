import { Outlet } from '@tanstack/react-router'
import { Disclaimer } from './Disclaimer'
import { Header } from './Header'

export function RootLayout() {
  return (
    <div className="flex min-h-screen flex-col bg-background">
      <Header />
      <main className="flex-1">
        <Outlet />
      </main>
      <Disclaimer />
    </div>
  )
}
