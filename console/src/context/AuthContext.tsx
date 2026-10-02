import { createContext, useContext, useState, useEffect, ReactNode } from 'react'
import axios from 'axios'

interface User {
  id: string
  email: string
  is_admin: boolean
  tenant_id?: string
  created_at: string
}

interface Tenant {
  id: string
  name: string
  quota_rps: number
  quota_burst: number
  max_concurrent: number
  max_keys: number
  max_key_ttl_hours: number
  created_at: string
  effective_rps: number
  effective_burst: number
}

interface AuthState {
  user: User | null
  tenant: Tenant | null
  loading: boolean
  error: string | null
}

export interface SignupResult {
  user: User
  tenant: Tenant | null
  firstKey: { key: string; prefix: string; name: string } | null
}

interface AuthContextType extends AuthState {
  login: (email: string, password: string) => Promise<User>
  signup: (email: string, password: string, tenant_id?: string, name?: string) => Promise<SignupResult>
  logout: () => Promise<void>
  changePassword: (currentPassword: string, newPassword: string) => Promise<void>
  fetchTenant: () => Promise<void>
  clearError: () => void
}

const AuthContext = createContext<AuthContextType | null>(null)

const api = axios.create({
  baseURL: '/api/v1',
  withCredentials: true,
  headers: {
    'Content-Type': 'application/json',
  },
})

function isPublicPath(pathname: string) {
  return (
    pathname === '/' ||
    pathname === '/login' ||
    pathname === '/signup' ||
    pathname.startsWith('/docs')
  )
}

function isSessionProbe(url: string | undefined) {
  if (!url) return false
  const path = url.split('?')[0]
  return (
    path.endsWith('/me') ||
    path.endsWith('/auth/login') ||
    path.endsWith('/auth/signup') ||
    path.endsWith('/auth/logout')
  )
}

// Redirect only when an established session dies mid-use — not when /me
// returns 401 because the visitor was never logged in.
api.interceptors.response.use(
  response => response,
  error => {
    if (error.response?.status === 401) {
      if (!isPublicPath(window.location.pathname) && !isSessionProbe(error.config?.url)) {
        window.location.href = '/login?reason=session_expired'
      }
    }
    return Promise.reject(error)
  }
)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<AuthState>({
    user: null,
    tenant: null,
    loading: true,
    error: null,
  })

  const fetchTenant = async () => {
    try {
      const response = await api.get('/tenant')
      setState(prev => ({ ...prev, tenant: response.data.tenant }))
    } catch (error) {
      console.error('Failed to fetch tenant:', error)
    }
  }

  const fetchUser = async () => {
    try {
      const response = await api.get('/me')
      setState(prev => ({ ...prev, user: response.data.user, loading: false }))
      if (response.data.user.tenant_id) {
        await fetchTenant()
      }
    } catch {
      setState(prev => ({ ...prev, user: null, loading: false }))
    }
  }

  useEffect(() => {
    fetchUser()
  }, [])

  const login = async (email: string, password: string): Promise<User> => {
    setState(prev => ({ ...prev, loading: true, error: null }))
    try {
      const response = await api.post('/auth/login', { email, password })
      const loggedUser = response.data.user
      const loggedTenant = response.data.tenant || null
      setState(prev => ({ ...prev, user: loggedUser, tenant: loggedTenant, loading: false }))
      if (loggedUser.tenant_id && !loggedTenant) {
        await fetchTenant()
      }
      return loggedUser
    } catch (error: any) {
      const message = error.response?.data?.error?.message || 'Login failed'
      setState(prev => ({ ...prev, loading: false, error: message }))
      throw error
    }
  }

  const signup = async (
    email: string,
    password: string,
    tenant_id?: string,
    name?: string
  ): Promise<SignupResult> => {
    setState(prev => ({ ...prev, loading: true, error: null }))
    try {
      const response = await api.post('/auth/signup', { email, password, tenant_id, name })
      const { user, tenant, api_key } = response.data
      setState(prev => ({
        ...prev,
        user,
        tenant: tenant || prev.tenant,
        loading: false,
      }))
      return {
        user,
        tenant: tenant || null,
        firstKey: api_key
          ? { key: api_key.key, prefix: api_key.prefix, name: api_key.name }
          : null,
      }
    } catch (error: any) {
      const message = error.response?.data?.error?.message || 'Signup failed'
      setState(prev => ({ ...prev, loading: false, error: message }))
      throw error
    }
  }

  const logout = async () => {
    try {
      await api.post('/auth/logout')
    } finally {
      setState({ user: null, tenant: null, loading: false, error: null })
    }
  }

  const changePassword = async (currentPassword: string, newPassword: string) => {
    setState(prev => ({ ...prev, loading: true, error: null }))
    try {
      await api.put('/me/password', {
        current_password: currentPassword,
        new_password: newPassword,
      })
      await logout()
    } catch (error: any) {
      const message = error.response?.data?.error?.message || 'Password change failed'
      setState(prev => ({ ...prev, loading: false, error: message }))
      throw error
    }
  }

  const clearError = () => {
    setState(prev => ({ ...prev, error: null }))
  }

  const value: AuthContextType = {
    ...state,
    login,
    signup,
    logout,
    changePassword,
    fetchTenant,
    clearError,
  }

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider')
  }
  return context
}

export { api }