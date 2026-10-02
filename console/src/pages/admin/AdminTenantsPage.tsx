import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../../context/AuthContext'
import { PageHeader } from '../../components/ui/PageHeader'
import { Card } from '../../components/ui/Card'
import { Button } from '../../components/ui/Button'
import { EmptyState } from '../../components/ui/EmptyState'
import { format } from 'date-fns'
import {
  MagnifyingGlassIcon,
  BuildingOfficeIcon,
  InformationCircleIcon,
  ArrowPathIcon,
} from '@heroicons/react/24/outline'
import { Tenant } from '../../types/api'

export function AdminTenantsPage() {
  const [search, setSearch] = useState('')
  const [tenants, setTenants] = useState<Tenant[]>([])
  const [nextAfter, setNextAfter] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [isLoadingMore, setIsLoadingMore] = useState(false)

  const fetchInitial = async () => {
    setIsLoading(true)
    try {
      const response = await api.get('/admin/tenants?limit=50')
      const items = (response.data.tenants || response.data.items || response.data || []) as Tenant[]
      setTenants(items)
      setNextAfter(response.data.next_after || null)
    } catch {
      setTenants([])
      setNextAfter(null)
    } finally {
      setIsLoading(false)
    }
  }

  useEffect(() => {
    fetchInitial()
  }, [])

  const handleLoadMore = async () => {
    if (!nextAfter || isLoadingMore) return
    setIsLoadingMore(true)
    try {
      const response = await api.get(`/admin/tenants?limit=50&after=${encodeURIComponent(nextAfter)}`)
      const newItems = (response.data.tenants || response.data.items || []) as Tenant[]
      setTenants((prev) => [...prev, ...newItems])
      setNextAfter(response.data.next_after || null)
    } catch {
      // Keep existing list on failure
    } finally {
      setIsLoadingMore(false)
    }
  }

  const filteredTenants = tenants.filter(
    (t) =>
      t.id.toLowerCase().includes(search.toLowerCase()) ||
      (t.name && t.name.toLowerCase().includes(search.toLowerCase()))
  )

  return (
    <div className="space-y-8 max-w-[1200px]">
      <PageHeader
        title="Tenants Fleet"
        subtitle="Manage hardware resource allocations, RPS ceilings, and multi-tenant namespaces."
      />

      {/* Info notice — no POST /admin/tenants endpoint exists */}
      <div className="flex items-start gap-3 bg-blush-peach/60 border border-blush-peach rounded-[16px] px-5 py-4 text-sm text-sienna-brown">
        <InformationCircleIcon className="h-5 w-5 shrink-0 mt-0.5" />
        <p>
          Tenants are created automatically when a user registers via{' '}
          <strong>POST /auth/signup</strong>. There is no admin-side tenant provisioning endpoint.
          To onboard a new tenant, direct them to the sign-up flow or use{' '}
          <code className="font-mono text-xs bg-sienna-brown/10 px-1 py-0.5 rounded">gatewayctl</code>.
        </p>
      </div>      {/* Search and Action Bar */}
      <div className="flex items-center justify-between gap-3">
        <div className="relative flex-1 max-w-md">
          <MagnifyingGlassIcon className="absolute left-3.5 top-1/2 -translate-y-1/2 h-4 w-4 text-slate-gray pointer-events-none" />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Filter tenants by ID or name..."
            className="w-full bg-mist-gray border border-transparent rounded-[16px] pl-10 pr-4 py-2 text-sm text-ink-black focus:outline-none focus:bg-paper-white focus:border-ink-black"
          />
        </div>
        <Button
          variant="secondary"
          size="sm"
          onClick={fetchInitial}
          loading={isLoading}
        >
          <ArrowPathIcon className="h-4 w-4 mr-1.5 inline" />
          Refresh
        </Button>
      </div>

      {/* Tenants Table Card */}
      <Card variant="default" className="p-0 overflow-hidden">
        {isLoading ? (
          <div className="p-16 text-center">
            <div className="animate-spin rounded-full h-8 w-8 border-2 border-ink-black/20 border-t-ink-black mx-auto mb-4" />
            <p className="text-sm text-slate-gray">Loading tenant fleet...</p>
          </div>
        ) : filteredTenants.length === 0 ? (
          <div className="p-12">
            <EmptyState
              icon={<BuildingOfficeIcon className="h-10 w-10 text-ash-gray" />}
              title="No tenants yet"
              description="Tenants are provisioned when users sign up. Direct new tenants to the sign-up page or use the gatewayctl CLI."
            />
          </div>
        ) : (
          <div>
            <div className="overflow-x-auto">
              <table className="w-full text-left border-collapse text-sm">
                <thead>
                  <tr className="border-b border-[#ececec] bg-fog-white/60 text-xs uppercase tracking-wider text-slate-gray">
                    <th className="py-3 px-6 font-medium">Tenant ID</th>
                    <th className="py-3 px-6 font-medium">Organization Name</th>
                    <th className="py-3 px-6 font-medium">RPS Quota</th>
                    <th className="py-3 px-6 font-medium">Burst</th>
                    <th className="py-3 px-6 font-medium">Max Keys</th>
                    <th className="py-3 px-6 font-medium">Created</th>
                    <th className="py-3 px-6 font-medium text-right">Action</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-[#ececec]">
                  {filteredTenants.map((t) => (
                    <tr key={t.id} className="hover:bg-fog-white/50 transition-colors">
                      <td className="py-4 px-6 font-mono text-xs font-semibold text-ink-black">
                        <Link
                          to={`/admin/tenants/${t.id}`}
                          className="bg-mist-gray hover:bg-[#ececec] px-2 py-1 rounded"
                        >
                          {t.id}
                        </Link>
                      </td>
                      <td className="py-4 px-6 font-medium text-ink-black">
                        <Link to={`/admin/tenants/${t.id}`} className="hover:underline">
                          {t.name || '—'}
                        </Link>
                      </td>
                      <td className="py-4 px-6 text-ink-black font-medium">{t.quota_rps} RPS</td>
                      <td className="py-4 px-6 text-slate-gray">{t.quota_burst}</td>
                      <td className="py-4 px-6 text-slate-gray">{t.max_keys}</td>
                      <td className="py-4 px-6 text-slate-gray text-xs">
                        {t.created_at ? format(new Date(t.created_at), 'MMM d, yyyy') : '—'}
                      </td>
                      <td className="py-4 px-6 text-right">
                        <Link
                          to={`/admin/tenants/${t.id}`}
                          className="text-xs font-medium text-slate-gray hover:text-ink-black"
                        >
                          Configure →
                        </Link>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {nextAfter && (
              <div className="p-4 border-t border-[#ececec] bg-fog-white/40 flex items-center justify-between">
                <span className="text-xs text-slate-gray">
                  Showing {tenants.length} tenants (cursor: <code className="font-mono text-[11px] text-ink-black">{nextAfter}</code>)
                </span>
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={handleLoadMore}
                  loading={isLoadingMore}
                >
                  Load more tenants →
                </Button>
              </div>
            )}
          </div>
        )}
      </Card>
    </div>
  )
}
