import { useState } from 'react'
import { Outlet, NavLink, useNavigate } from 'react-router-dom'
import { useAuth } from '../../context/AuthContext'
import {
  HomeIcon,
  KeyIcon,
  ChartBarIcon,
  Cog6ToothIcon,
  ArrowRightOnRectangleIcon,
  UsersIcon,
  ShieldCheckIcon,
  Bars3Icon,
  XMarkIcon,
  DocumentTextIcon,
} from '@heroicons/react/24/outline'

const customerNav = [
  { name: 'Dashboard',     href: '/dashboard', icon: HomeIcon },
  { name: 'API Keys',      href: '/keys',       icon: KeyIcon },
  { name: 'Usage',         href: '/usage',      icon: ChartBarIcon },
  { name: 'Documentation', href: '/docs',       icon: DocumentTextIcon },
  { name: 'Settings',      href: '/settings',   icon: Cog6ToothIcon },
]

const adminFleetNav = [
  { name: 'Cluster Overview', href: '/admin',         icon: ShieldCheckIcon },
  { name: 'Tenants Fleet',    href: '/admin/tenants', icon: UsersIcon },
  { name: 'Documentation',   href: '/docs',           icon: DocumentTextIcon },
  { name: 'Settings',        href: '/settings',       icon: Cog6ToothIcon },
]

interface NavItemProps {
  href: string
  icon: React.ElementType
  name: string
  end?: boolean
}

function NavItem({ href, icon: Icon, name, end }: NavItemProps) {
  return (
    <NavLink
      to={href}
      end={end}
      className={({ isActive }) =>
        `nav-item ${isActive ? 'active' : ''}`
      }
    >
      <Icon className="h-[15px] w-[15px] flex-shrink-0 opacity-80" />
      <span>{name}</span>
    </NavLink>
  )
}

interface SidebarProps {
  user: NonNullable<ReturnType<typeof useAuth>['user']>
  logout: () => void
  onClose?: () => void
}

function Sidebar({ user, logout, onClose }: SidebarProps) {
  const initials = user.email.substring(0, 2).toUpperCase()
  const isOperatorOnly = user.is_admin && !user.tenant_id

  return (
    <nav className="sidebar">
      {/* Logo */}
      <div className="px-4 pt-5 pb-4 flex items-center justify-between" style={{ borderBottom: '1px solid #ececec' }}>
        <span
          className="text-[18px] font-normal tracking-tight text-ink-black select-none"
          style={{ fontFamily: "'Georgia', ui-serif, serif", letterSpacing: '-0.02em' }}
        >
          Distri<span style={{ fontStyle: 'italic', color: '#5d2a1a' }}>KV</span>
        </span>
        {onClose && (
          <button onClick={onClose} className="btn-ghost p-1 -mr-1 text-slate-gray hover:text-ink-black">
            <XMarkIcon className="h-4 w-4" />
          </button>
        )}
      </div>

      {/* Main navigation */}
      <div className="flex-1 px-3 py-3 space-y-0.5 overflow-y-auto">
        {isOperatorOnly ? (
          <>
            <p className="text-overline px-2 pb-1.5 pt-0.5">Fleet</p>
            {adminFleetNav.map((item) => (
              <NavItem key={item.href} {...item} end={item.href === '/admin'} />
            ))}
          </>
        ) : (
          <>
            <p className="text-overline px-2 pb-1.5 pt-0.5">Console</p>
            {customerNav.map((item) => (
              <NavItem key={item.href} {...item} end={item.href === '/dashboard'} />
            ))}
            {user.is_admin && (
              <>
                <div className="pt-4 pb-1">
                  <p className="text-overline px-2">Admin</p>
                </div>
                <NavItem href="/admin"         icon={ShieldCheckIcon} name="Cluster Overview" end />
                <NavItem href="/admin/tenants" icon={UsersIcon}       name="Tenants Fleet" />
              </>
            )}
          </>
        )}
      </div>

      {/* User footer */}
      <div className="px-3 pb-4 pt-3" style={{ borderTop: '1px solid #ececec' }}>
        <div className="flex items-center gap-2.5 px-2 py-2 mb-1 rounded-xl">
          {/* Avatar */}
          <div
            className="w-7 h-7 rounded-full flex items-center justify-center text-[11px] font-semibold text-paper-white flex-shrink-0 select-none"
            style={{ background: '#17191c' }}
          >
            {initials}
          </div>
          <div className="flex-1 min-w-0">
            <p className="text-[13px] font-[500] text-ink-black truncate leading-tight">{user.email}</p>
            {user.tenant_id && (
              <p className="text-[11px] text-slate-gray truncate mt-0.5 font-mono">{user.tenant_id}</p>
            )}
          </div>
        </div>

        <button
          onClick={logout}
          className="nav-item w-full text-slate-gray hover:text-ink-black"
        >
          <ArrowRightOnRectangleIcon className="h-[15px] w-[15px]" />
          <span>Sign out</span>
        </button>
      </div>
    </nav>
  )
}

export function Layout() {
  const { user, logout, loading } = useAuth()
  const navigate = useNavigate()
  const [mobileOpen, setMobileOpen] = useState(false)

  const handleLogout = async () => {
    await logout()
    navigate('/login')
  }

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-paper-white">
        <div className="flex flex-col items-center gap-3">
          <div className="w-7 h-7 border-2 border-mist-gray border-t-ink-black rounded-full animate-spin" />
          <p className="text-[13px] text-slate-gray">Loading…</p>
        </div>
      </div>
    )
  }

  if (!user) return <Outlet />

  return (
    <>
      {/* Desktop layout */}
      <div className="hidden lg:grid h-screen overflow-hidden" style={{ gridTemplateColumns: '240px 1fr' }}>
        <Sidebar user={user} logout={handleLogout} />

        <div className="flex flex-col h-screen overflow-hidden bg-fog-white">
          {/* Top header bar */}
          <header className="app-header">
            <div />
            <div className="flex items-center gap-2">
              <span
                className="text-[11.5px] font-[500] px-2.5 py-1 rounded-full"
                style={{ background: '#f2f2f3', color: '#777b86' }}
              >
                :8080 Data Plane
              </span>
              {user.is_admin && (
                <span
                  className="text-[11.5px] font-[500] px-2.5 py-1 rounded-full"
                  style={{ background: '#fbe1d1', color: '#5d2a1a' }}
                >
                  Admin
                </span>
              )}
            </div>
          </header>

          <main className="page-content">
            <div className="page-inner">
              <Outlet />
            </div>
          </main>
        </div>
      </div>

      {/* Mobile layout */}
      <div className="lg:hidden flex flex-col h-screen bg-fog-white">
        <header className="app-header">
          <span
            className="text-[17px] font-normal text-ink-black"
            style={{ fontFamily: "'Georgia', ui-serif, serif", fontStyle: 'normal', letterSpacing: '-0.02em' }}
          >
            Distri<span style={{ fontStyle: 'italic', color: '#5d2a1a' }}>KV</span>
          </span>
          <button
            onClick={() => setMobileOpen(true)}
            className="btn-ghost p-2"
            aria-label="Open menu"
          >
            <Bars3Icon className="h-5 w-5" />
          </button>
        </header>

        {mobileOpen && (
          <div className="fixed inset-0 z-50 flex">
            <div
              className="absolute inset-0"
              style={{ background: 'rgba(0,0,0,0.25)', backdropFilter: 'blur(4px)' }}
              onClick={() => setMobileOpen(false)}
            />
            <div className="relative w-64 h-full animate-slide-in-right">
              <Sidebar user={user} logout={handleLogout} onClose={() => setMobileOpen(false)} />
            </div>
          </div>
        )}

        <main className="page-content">
          <div className="px-4 py-6">
            <Outlet />
          </div>
        </main>
      </div>
    </>
  )
}