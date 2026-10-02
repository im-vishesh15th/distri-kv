import { Routes, Route, Navigate } from 'react-router-dom'
import { useAuth } from './context/AuthContext'
import { Layout } from './components/layout/Layout'
import { LoginPage } from './pages/auth/LoginPage'
import { SignupPage } from './pages/auth/SignupPage'
import { DashboardPage } from './pages/dashboard/DashboardPage'
import { KeysPage } from './pages/keys/KeysPage'
import { KeyDetailPage } from './pages/keys/KeyDetailPage'
import { CreateKeyPage } from './pages/keys/CreateKeyPage'
import { UsagePage } from './pages/usage/UsagePage'
import { SettingsPage } from './pages/settings/SettingsPage'
import { AdminDashboardPage } from './pages/admin/AdminDashboardPage'
import { AdminTenantsPage } from './pages/admin/AdminTenantsPage'
import { AdminTenantDetailPage } from './pages/admin/AdminTenantDetailPage'
import { LandingPage } from './pages/landing/LandingPage'
import { DocumentationPage } from './pages/docs/DocumentationPage'

function ProtectedRoute({
  children,
  adminOnly = false,
}: {
  children: React.ReactNode
  adminOnly?: boolean
}) {
  const { user, loading } = useAuth()

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-paper-white">
        <div className="animate-spin rounded-full h-8 w-8 border-2 border-ink-black/20 border-t-ink-black"></div>
      </div>
    )
  }

  if (!user) {
    return <Navigate to="/login" replace />
  }

  if (adminOnly && !user.is_admin) {
    return <Navigate to="/dashboard" replace />
  }

  return <>{children}</>
}

function PublicOnly({ children }: { children: React.ReactNode }) {
  const { user, loading } = useAuth()

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-paper-white">
        <div className="animate-spin rounded-full h-8 w-8 border-2 border-ink-black/20 border-t-ink-black"></div>
      </div>
    )
  }

  if (user) {
    return <Navigate to="/dashboard" replace />
  }

  return <>{children}</>
}

function RootRoute() {
  const { user, loading } = useAuth()

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-paper-white">
        <div className="animate-spin rounded-full h-8 w-8 border-2 border-ink-black/20 border-t-ink-black"></div>
      </div>
    )
  }

  if (user) {
    return <Navigate to={user.is_admin ? '/admin' : '/dashboard'} replace />
  }

  return <LandingPage />
}

function DashboardRoute() {
  const { user } = useAuth()
  if (user?.is_admin && !user?.tenant_id) {
    return <Navigate to="/admin" replace />
  }
  return <DashboardPage />
}

export function App() {
  return (
    <Routes>
      {/* Root landing page */}
      <Route path="/" element={<RootRoute />} />

      {/* Public auth routes */}
      <Route
        path="/login"
        element={
          <PublicOnly>
            <LoginPage />
          </PublicOnly>
        }
      />
      <Route
        path="/signup"
        element={
          <PublicOnly>
            <SignupPage />
          </PublicOnly>
        }
      />

      {/* Public documentation routes */}
      <Route path="/docs" element={<DocumentationPage />} />
      <Route path="/docs/*" element={<DocumentationPage />} />

      {/* Protected customer routes */}
      <Route
        element={
          <ProtectedRoute>
            <Layout />
          </ProtectedRoute>
        }
      >
        <Route path="/dashboard" element={<DashboardRoute />} />
        <Route path="/keys" element={<KeysPage />} />
        <Route path="/keys/new" element={<CreateKeyPage />} />
        <Route path="/keys/:prefix" element={<KeyDetailPage />} />
        <Route path="/usage" element={<UsagePage />} />
        <Route path="/settings" element={<SettingsPage />} />
      </Route>

      {/* Admin routes */}
      <Route
        element={
          <ProtectedRoute adminOnly>
            <Layout />
          </ProtectedRoute>
        }
      >
        <Route path="/admin" element={<AdminDashboardPage />} />
        <Route path="/admin/tenants" element={<AdminTenantsPage />} />
        <Route path="/admin/tenants/:id" element={<AdminTenantDetailPage />} />
      </Route>

      {/* Catch-all redirect */}
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}

export default App