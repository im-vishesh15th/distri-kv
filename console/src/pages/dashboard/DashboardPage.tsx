import { useQuery } from '@tanstack/react-query'
import { useAuth, api } from '../../context/AuthContext'
import {
  KeyIcon,
  PlusIcon,
  BoltIcon,
  ShieldCheckIcon,
  ClockIcon,
  ArrowRightIcon,
} from '@heroicons/react/24/outline'
import { Link } from 'react-router-dom'
import { format } from 'date-fns'
import { StatCard } from '../../components/ui/StatCard'
import { Card } from '../../components/ui/Card'
import { Badge } from '../../components/ui/Badge'
import { Button } from '../../components/ui/Button'
import { PageHeader } from '../../components/ui/PageHeader'
import { Tenant, Usage, UsageSeries, ApiKey } from '../../types/api'
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
} from 'recharts'

export function DashboardPage() {
  const { user } = useAuth()

  const { data: tenant, isLoading: isTenantLoading } = useQuery({
    queryKey: ['tenant'],
    queryFn: async () => {
      const response = await api.get('/tenant')
      return response.data.tenant as Tenant
    },
    enabled: !!user,
  })

  const { data: usage, isLoading: isUsageLoading } = useQuery({
    queryKey: ['usage'],
    queryFn: async () => {
      const response = await api.get('/tenant/usage')
      return response.data.usage as Usage
    },
    enabled: !!user,
    refetchInterval: 30_000,
  })

  const { data: series } = useQuery({
    queryKey: ['usage-series', '24h'],
    queryFn: async () => {
      const response = await api.get('/tenant/usage/series', { params: { range: '24h' } })
      return response.data.series as UsageSeries
    },
    enabled: !!user,
    refetchInterval: 30_000,
  })

  const { data: keys, isLoading: isKeysLoading } = useQuery({
    queryKey: ['tenantKeys'],
    queryFn: async () => {
      const response = await api.get('/tenant/keys')
      return response.data.keys as ApiKey[]
    },
    enabled: !!user,
  })

  const activeKeys = keys?.filter((k) => k.status === 'active').length || 0
  const totalKeys = keys?.length || 0

  const opData = [
    { name: 'GET', count: usage?.by_op?.get || 0 },
    { name: 'PUT', count: usage?.by_op?.put || 0 },
    { name: 'CAS', count: usage?.by_op?.cas || 0 },
    { name: 'DEL', count: usage?.by_op?.del || usage?.by_op?.delete || 0 },
  ]

  const activityData = (series?.points || []).map((p) => ({
    hour: format(new Date(p.t), 'HH:mm'),
    requests: p.requests || 0,
  }))

  const userGreeting = () => {
    const hour = new Date().getHours()
    if (hour < 12) return 'Good morning'
    if (hour < 18) return 'Good afternoon'
    return 'Good evening'
  }

  const effectiveRps = tenant?.effective_rps || tenant?.quota_rps || 100
  const effectiveBurst = tenant?.effective_burst || tenant?.quota_burst || 200

  return (
    <div className="space-y-8 max-w-[1200px]">
      {/* Page Header */}
      <PageHeader
        title="Dashboard"
        subtitle={`${userGreeting()}, ${user?.email?.split('@')[0]}. Here is your cluster's operational telemetry.`}
        actions={
          <div className="flex items-center gap-3">
            <Link to="/keys">
              <Button variant="secondary" size="md">
                Manage keys
              </Button>
            </Link>
            <Link to="/keys?create=true">
              <Button variant="primary" size="md">
                <PlusIcon className="h-4 w-4 shrink-0" />
                <span>New API key</span>
              </Button>
            </Link>
          </div>
        }
      />

      {/* Top Stat Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-5">
        <StatCard
          label="Active API Keys"
          value={`${activeKeys} / ${totalKeys}`}
          helperText={`Limit: ${tenant?.max_keys || 10} keys`}
          icon={<KeyIcon className="h-5 w-5" />}
          loading={isKeysLoading}
        />

        <StatCard
          label="Total Requests"
          value={usage?.requests?.toLocaleString() || '0'}
          delta={{
            value: `${usage?.by_op?.get || 0} reads · ${usage?.by_op?.put || 0} writes`,
            isNeutral: true,
          }}
          icon={<ClockIcon className="h-5 w-5" />}
          loading={isUsageLoading}
        />

        <StatCard
          label="Rate Limited"
          value={usage?.rate_limited?.toLocaleString() || '0'}
          delta={
            usage?.rate_limited && usage.rate_limited > 0
              ? { value: `${usage.rate_limited} dropped`, isPositive: false }
              : { value: 'Within quota limits', isPositive: true }
          }
          icon={<ShieldCheckIcon className="h-5 w-5" />}
          loading={isUsageLoading}
        />

        <StatCard
          label="Effective Quota"
          value={`${effectiveRps} RPS`}
          helperText={`Burst capacity: ${effectiveBurst} ops`}
          icon={<BoltIcon className="h-5 w-5" />}
          loading={isTenantLoading}
        />
      </div>

      {/* Charts & Quota Section */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Main Traffic Area Chart */}
        <Card variant="elevated" className="lg:col-span-2 flex flex-col justify-between">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h2 className="text-base font-medium text-ink-black">Traffic Telemetry (24h)</h2>
              <p className="text-xs text-slate-gray mt-0.5">Hourly request volume stored with your tenant</p>
            </div>
            <Link
              to="/usage"
              className="text-xs font-medium text-slate-gray hover:text-ink-black inline-flex items-center gap-1"
            >
              Full analytics <ArrowRightIcon className="h-3 w-3" />
            </Link>
          </div>

          <div className="h-64 w-full mt-4">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={activityData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                <defs>
                  <linearGradient id="reqGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#5d2a1a" stopOpacity={0.2} />
                    <stop offset="95%" stopColor="#5d2a1a" stopOpacity={0.0} />
                  </linearGradient>
                </defs>
                <XAxis
                  dataKey="hour"
                  stroke="#979799"
                  fontSize={12}
                  tickLine={false}
                  axisLine={{ stroke: '#ececec' }}
                />
                <YAxis
                  stroke="#979799"
                  fontSize={12}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={(val) => `${val}`}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#ffffff',
                    borderColor: '#ececec',
                    borderRadius: '12px',
                    boxShadow: '0 4px 12px rgba(0,0,0,0.05)',
                    fontSize: '13px',
                  }}
                  labelStyle={{ fontWeight: 500, color: '#17191c' }}
                />
                <Area
                  type="monotone"
                  dataKey="requests"
                  stroke="#5d2a1a"
                  strokeWidth={2}
                  fillOpacity={1}
                  fill="url(#reqGradient)"
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </Card>

        {/* Right side: Operations distribution & Tenant callout */}
        <div className="flex flex-col gap-6">
          <Card variant="elevated" className="flex-1 flex flex-col justify-between">
            <div>
              <h2 className="text-base font-medium text-ink-black">Operations Breakdown</h2>
              <p className="text-xs text-slate-gray mt-0.5">Distribution by KV opcode</p>
            </div>

            <div className="h-40 w-full my-2">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={opData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                  <XAxis dataKey="name" stroke="#979799" fontSize={12} tickLine={false} axisLine={false} />
                  <YAxis stroke="#979799" fontSize={12} tickLine={false} axisLine={false} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#ffffff',
                      borderColor: '#ececec',
                      borderRadius: '12px',
                      fontSize: '12px',
                    }}
                  />
                  <Bar dataKey="count" fill="#17191c" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>

            <div className="grid grid-cols-4 gap-2 pt-3 border-t border-[#ececec] text-center text-xs">
              <div>
                <span className="text-slate-gray block">GET</span>
                <span className="font-medium text-ink-black">{usage?.by_op?.get || 0}</span>
              </div>
              <div>
                <span className="text-slate-gray block">PUT</span>
                <span className="font-medium text-ink-black">{usage?.by_op?.put || 0}</span>
              </div>
              <div>
                <span className="text-slate-gray block">CAS</span>
                <span className="font-medium text-ink-black">{usage?.by_op?.cas || 0}</span>
              </div>
              <div>
                <span className="text-slate-gray block">DEL</span>
                <span className="font-medium text-ink-black">{usage?.by_op?.del || 0}</span>
              </div>
            </div>
          </Card>

          {/* Accent Warm Card */}
          <Card variant="accent" className="space-y-2">
            <div className="text-xs font-semibold uppercase tracking-wider opacity-85">
              Tenant Architecture
            </div>
            <div className="text-sm font-medium leading-snug">
              Tenant <span className="font-mono font-semibold">{tenant?.id || user?.tenant_id || 'default'}</span> is operating on dedicated token-bucket isolation.
            </div>
            <div className="text-xs opacity-75 pt-1">
              Max TTL: {tenant?.max_key_ttl_hours || 720} hours · Max concurrent streams: {tenant?.max_concurrent || 50}
            </div>
          </Card>
        </div>
      </div>

      {/* Recent Keys Section */}
      <Card variant="default">
        <div className="flex items-center justify-between mb-5">
          <div>
            <h2 className="text-base font-medium text-ink-black">Recent API Keys</h2>
            <p className="text-xs text-slate-gray mt-0.5">Active cryptographic credentials for client SDKs</p>
          </div>
          <Link
            to="/keys"
            className="text-sm font-medium text-slate-gray hover:text-ink-black inline-flex items-center gap-1"
          >
            All keys →
          </Link>
        </div>

        {isKeysLoading ? (
          <div className="space-y-3 py-3">
            {[1, 2, 3].map((i) => (
              <div key={i} className="flex items-center justify-between py-2 border-b border-[#ececec]">
                <div className="h-4 w-28 bg-smoke-gray/20 animate-pulse rounded" />
                <div className="h-4 w-32 bg-smoke-gray/20 animate-pulse rounded" />
                <div className="h-4 w-16 bg-smoke-gray/20 animate-pulse rounded-full" />
                <div className="h-4 w-20 bg-smoke-gray/20 animate-pulse rounded" />
              </div>
            ))}
          </div>
        ) : keys && keys.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse text-sm">
              <thead>
                <tr className="border-b border-[#ececec] text-xs uppercase tracking-wider text-slate-gray">
                  <th className="pb-3 font-medium">Key Prefix</th>
                  <th className="pb-3 font-medium">Name</th>
                  <th className="pb-3 font-medium">Status</th>
                  <th className="pb-3 font-medium text-right">Created</th>
                  <th className="pb-3 font-medium text-right">Action</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[#ececec]">
                {keys.slice(0, 5).map((key) => (
                  <tr key={key.prefix} className="hover:bg-fog-white/60 transition-colors">
                    <td className="py-3 font-mono text-xs text-ink-black">
                      <span className="bg-mist-gray px-2 py-1 rounded-md">{key.prefix}••••••••</span>
                    </td>
                    <td className="py-3 font-medium text-ink-black">{key.name}</td>
                    <td className="py-3">
                      <Badge status={key.status} />
                    </td>
                    <td className="py-3 text-right text-xs text-slate-gray">
                      {format(new Date(key.created_at), 'MMM d, yyyy')}
                    </td>
                    <td className="py-3 text-right">
                      <Link
                        to={`/keys/${key.prefix}`}
                        className="text-xs font-medium text-slate-gray hover:text-ink-black inline-flex items-center gap-1"
                      >
                        Inspect →
                      </Link>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="text-center py-10 space-y-3">
            <KeyIcon className="h-10 w-10 mx-auto text-ash-gray" />
            <div className="text-sm font-medium text-ink-black">No API keys created yet</div>
            <p className="text-xs text-slate-gray max-w-sm mx-auto">
              Generate an API key to begin writing and reading distributed keys with the SDK or CLI.
            </p>
            <div className="pt-2">
              <Link to="/keys?create=true">
                <Button variant="primary" size="sm">
                  Create your first key
                </Button>
              </Link>
            </div>
          </div>
        )}
      </Card>
    </div>
  )
}