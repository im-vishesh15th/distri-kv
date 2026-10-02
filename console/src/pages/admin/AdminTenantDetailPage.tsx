import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../../context/AuthContext'
import { PageHeader } from '../../components/ui/PageHeader'
import { Card } from '../../components/ui/Card'
import { Button } from '../../components/ui/Button'
import { Badge } from '../../components/ui/Badge'
import { ConfirmDialog } from '../../components/ui/ConfirmDialog'
import { toast } from 'react-hot-toast'
import { format } from 'date-fns'
import { ArrowLeftIcon } from '@heroicons/react/24/outline'
import { Tenant, ApiKey, Usage } from '../../types/api'

export function AdminTenantDetailPage() {
  const { id } = useParams<{ id: string }>()
  const queryClient = useQueryClient()
  const [keyToRevoke, setKeyToRevoke] = useState<ApiKey | null>(null)

  const [quotaForm, setQuotaForm] = useState({
    rps: 100,
    burst: 200,
  })

  const { data: tenant, isLoading } = useQuery({
    queryKey: ['adminTenant', id],
    queryFn: async () => {
      const response = await api.get(`/admin/tenants/${id}`)
      return (response.data.tenant || response.data) as Tenant
    },
    enabled: !!id,
  })

  const { data: tenantKeys = [] } = useQuery({
    queryKey: ['adminTenantKeys', id],
    queryFn: async () => {
      try {
        const response = await api.get(`/admin/tenants/${id}/keys`)
        return (response.data.keys || []) as ApiKey[]
      } catch {
        return []
      }
    },
    enabled: !!id,
  })

  const { data: tenantUsage, isLoading: isUsageLoading } = useQuery({
    queryKey: ['adminTenantUsage', id],
    queryFn: async () => {
      try {
        const response = await api.get(`/admin/tenants/${id}/usage`)
        return (response.data.usage || null) as Usage | null
      } catch {
        return null
      }
    },
    enabled: !!id,
  })

  const revokeKeyMutation = useMutation({
    mutationFn: async (prefix: string) => {
      await api.delete(`/admin/tenants/${id}/keys/${prefix}`)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['adminTenantKeys', id] })
      toast.success('Key revoked successfully')
      setKeyToRevoke(null)
    },
    onError: (err: any) => {
      toast.error(err.response?.data?.error?.message || 'Failed to revoke key')
    },
  })

  useEffect(() => {
    if (tenant) {
      setQuotaForm({
        rps: tenant.quota_rps || 100,
        burst: tenant.quota_burst || 200,
      })
    }
  }, [tenant])

  const updateQuotaMutation = useMutation({
    mutationFn: async (data: typeof quotaForm) => {
      // Backend only accepts { rps, burst } — other fields are global gateway config
      const response = await api.put(`/admin/tenants/${id}/quota`, {
        rps: data.rps,
        burst: data.burst,
      })
      return response.data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['adminTenant', id] })
      toast.success('Tenant quota updated')
    },
    onError: (err: any) => {
      toast.error(err.response?.data?.error?.message || 'Failed to update quota')
    },
  })

  if (isLoading) {
    return (
      <div className="min-h-[400px] flex items-center justify-center">
        <div className="animate-spin rounded-full h-8 w-8 border-2 border-ink-black/20 border-t-ink-black" />
      </div>
    )
  }

  return (
    <div className="space-y-8 max-w-[1100px]">
      <div>
        <Link
          to="/admin/tenants"
          className="inline-flex items-center gap-1.5 text-xs font-medium text-slate-gray hover:text-ink-black mb-4 transition-colors"
        >
          <ArrowLeftIcon className="h-3.5 w-3.5" /> Back to Tenants
        </Link>
        <PageHeader
          title={tenant?.name || tenant?.id || 'Tenant Details'}
          subtitle={`Hardware namespace ID: ${tenant?.id}`}
        />
      </div>

      {/* Tenant Telemetry & Usage Card */}
      <Card variant="default">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 mb-4">
          <div>
            <h2 className="text-base font-medium text-ink-black">Tenant Telemetry & Usage</h2>
            <p className="text-xs text-slate-gray">
              {tenantUsage?.since
                ? `Activity recorded since ${format(new Date(tenantUsage.since), 'MMM d, yyyy HH:mm')}`
                : 'Current aggregate usage metrics for this namespace'}
            </p>
          </div>
          <span className="text-xs px-2.5 py-1 rounded-full bg-mist-gray text-slate-gray font-mono self-start sm:self-auto">
            GET /admin/tenants/{id}/usage
          </span>
        </div>

        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
          <div className="bg-mist-gray rounded-[14px] p-3.5">
            <span className="text-xs text-slate-gray block">Total Requests</span>
            <span className="text-2xl font-semibold text-ink-black font-sans mt-1 block">
              {isUsageLoading ? '...' : (tenantUsage?.requests ?? 0).toLocaleString()}
            </span>
          </div>
          <div className="bg-mist-gray rounded-[14px] p-3.5">
            <span className="text-xs text-slate-gray block">Rate Limited</span>
            <span className={`text-2xl font-semibold font-sans mt-1 block ${
              (tenantUsage?.rate_limited ?? 0) > 0 ? 'text-[#cf222e]' : 'text-ink-black'
            }`}>
              {isUsageLoading ? '...' : (tenantUsage?.rate_limited ?? 0).toLocaleString()}
            </span>
          </div>
          <div className="bg-mist-gray rounded-[14px] p-3.5">
            <span className="text-xs text-slate-gray block">Writes (PUT + DEL)</span>
            <span className="text-2xl font-semibold text-ink-black font-sans mt-1 block">
              {isUsageLoading ? '...' : (
                ((tenantUsage?.by_op?.put ?? 0) + (tenantUsage?.by_op?.delete ?? tenantUsage?.by_op?.del ?? 0)).toLocaleString()
              )}
            </span>
          </div>
          <div className="bg-mist-gray rounded-[14px] p-3.5">
            <span className="text-xs text-slate-gray block">Reads (GET)</span>
            <span className="text-2xl font-semibold text-ink-black font-sans mt-1 block">
              {isUsageLoading ? '...' : (tenantUsage?.by_op?.get ?? 0).toLocaleString()}
            </span>
          </div>
        </div>

        {tenantUsage?.by_op && (
          <div className="mt-4 pt-3 border-t border-[#ececec] flex flex-wrap gap-4 text-xs text-slate-gray">
            <span>GET: <strong className="text-ink-black">{tenantUsage.by_op.get ?? 0}</strong></span>
            <span>PUT: <strong className="text-ink-black">{tenantUsage.by_op.put ?? 0}</strong></span>
            <span>CAS: <strong className="text-ink-black">{tenantUsage.by_op.cas ?? 0}</strong></span>
            <span>DEL: <strong className="text-ink-black">{tenantUsage.by_op.delete ?? tenantUsage.by_op.del ?? 0}</strong></span>
          </div>
        )}
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left 2 cols: Quota Configuration */}
        <div className="lg:col-span-2 space-y-6">
          <Card variant="default">
            <h2 className="text-base font-medium text-ink-black mb-1">Capacity & Rate Limiting</h2>
            <p className="text-xs text-slate-gray mb-6">
              Adjust sustained queries per second and burst absorption for this tenant.
            </p>

            <form
              onSubmit={(e) => {
                e.preventDefault()
                updateQuotaMutation.mutate(quotaForm)
              }}
              className="space-y-4"
            >
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label className="block mb-1 text-xs font-medium text-slate-gray">
                    Sustained RPS Quota
                  </label>
                  <input
                    type="number"
                    value={quotaForm.rps}
                    onChange={(e) =>
                      setQuotaForm({ ...quotaForm, rps: parseFloat(e.target.value) || 0 })
                    }
                    className="w-full bg-mist-gray border border-transparent rounded-[16px] px-3.5 py-2 text-sm text-ink-black focus:outline-none focus:bg-paper-white focus:border-ink-black"
                    min={0}
                    step={0.1}
                    required
                  />
                  <p className="mt-1 text-[11px] text-slate-gray">Set to 0 to use the gateway default</p>
                </div>

                <div>
                  <label className="block mb-1 text-xs font-medium text-slate-gray">
                    Burst Allowance
                  </label>
                  <input
                    type="number"
                    value={quotaForm.burst}
                    onChange={(e) =>
                      setQuotaForm({ ...quotaForm, burst: parseInt(e.target.value) || 0 })
                    }
                    className="w-full bg-mist-gray border border-transparent rounded-[16px] px-3.5 py-2 text-sm text-ink-black focus:outline-none focus:bg-paper-white focus:border-ink-black"
                    min={0}
                    required
                  />
                  <p className="mt-1 text-[11px] text-slate-gray">Set to 0 to use the gateway default</p>
                </div>
              </div>

              {/* Read-only gateway-level limits */}
              <div className="bg-mist-gray rounded-[16px] p-4 space-y-2">
                <p className="text-[11px] font-semibold uppercase tracking-wider text-slate-gray mb-3">
                  Global Gateway Limits (read-only)
                </p>
                <div className="grid grid-cols-3 gap-3 text-xs">
                  <div>
                    <span className="text-slate-gray block">Max Keys</span>
                    <span className="font-medium text-ink-black">{tenant?.max_keys ?? '—'}</span>
                  </div>
                  <div>
                    <span className="text-slate-gray block">Max Concurrent</span>
                    <span className="font-medium text-ink-black">{tenant?.max_concurrent ?? '—'}</span>
                  </div>
                  <div>
                    <span className="text-slate-gray block">Max TTL (hrs)</span>
                    <span className="font-medium text-ink-black">{tenant?.max_key_ttl_hours ?? '—'}</span>
                  </div>
                </div>
              </div>

              <div className="pt-2">
                <Button type="submit" variant="primary" loading={updateQuotaMutation.isPending}>
                  Save Quota Changes
                </Button>
              </div>
            </form>
          </Card>


          {/* Keys under tenant */}
          <Card variant="default">
            <h2 className="text-base font-medium text-ink-black mb-3">Tenant API Keys</h2>
            {tenantKeys.length === 0 ? (
              <p className="text-xs text-slate-gray py-4">No keys provisioned for this tenant.</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm border-collapse">
                  <thead>
                    <tr className="border-b border-[#ececec] text-xs uppercase tracking-wider text-slate-gray">
                      <th className="pb-2 font-medium">Prefix</th>
                      <th className="pb-2 font-medium">Name</th>
                      <th className="pb-2 font-medium">Status</th>
                      <th className="pb-2 font-medium">Created</th>
                      <th className="pb-2 font-medium text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-[#ececec]">
                    {tenantKeys.map((k) => (
                      <tr key={k.prefix} className="hover:bg-fog-white/60">
                        <td className="py-2.5 font-mono text-xs">{k.prefix}••••••••</td>
                        <td className="py-2.5 font-medium">{k.name}</td>
                        <td className="py-2.5">
                          <Badge status={k.status} />
                        </td>
                        <td className="py-2.5 text-xs text-slate-gray">
                          {format(new Date(k.created_at), 'MMM d, yyyy')}
                        </td>
                        <td className="py-2.5 text-right">
                          {k.status === 'active' ? (
                            <Button
                              variant="ghost"
                              size="sm"
                              className="text-red-600 hover:text-red-700 hover:bg-red-50 text-xs px-2.5 py-1 h-auto"
                              onClick={() => setKeyToRevoke(k)}
                            >
                              Revoke
                            </Button>
                          ) : (
                            <span className="text-xs text-slate-gray italic">{k.status}</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Card>
        </div>

        {/* Right col: Tenant Meta */}
        <div className="space-y-6">
          <Card variant="elevated" className="space-y-4">
            <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-gray">
              Tenant Summary
            </h3>
            <div className="space-y-3 text-xs">
              <div>
                <span className="text-slate-gray block">Partition ID</span>
                <span className="font-mono text-ink-black font-semibold">{tenant?.id}</span>
              </div>
              <div>
                <span className="text-slate-gray block">Created Timestamp</span>
                <span className="text-ink-black">
                  {tenant?.created_at ? format(new Date(tenant.created_at), 'MMMM d, yyyy') : '—'}
                </span>
              </div>
              <div>
                <span className="text-slate-gray block">Effective RPS</span>
                <span className="text-ink-black font-medium">{tenant?.effective_rps || tenant?.quota_rps} req/sec</span>
              </div>
              <div>
                <span className="text-slate-gray block">Effective Burst</span>
                <span className="text-ink-black font-medium">{tenant?.effective_burst || tenant?.quota_burst} capacity</span>
              </div>
            </div>
          </Card>
        </div>
      </div>

      <ConfirmDialog
        open={!!keyToRevoke}
        onClose={() => setKeyToRevoke(null)}
        onConfirm={() => {
          if (keyToRevoke) {
            revokeKeyMutation.mutate(keyToRevoke.prefix)
          }
        }}
        title="Revoke Tenant API Key"
        description={`Are you sure you want to revoke key "${keyToRevoke?.name || keyToRevoke?.prefix}"? Any service using this key will immediately lose access.`}
        confirmLabel="Revoke Key"
        loading={revokeKeyMutation.isPending}
      />
    </div>
  )
}
