import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { format } from 'date-fns'
import { api, useAuth } from '../../context/AuthContext'
import { PageHeader } from '../../components/ui/PageHeader'
import { StatCard } from '../../components/ui/StatCard'
import { Card } from '../../components/ui/Card'
import { Usage, UsageSeries, Tenant } from '../../types/api'
import {
  AreaChart,
  Area,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import {
  ArrowPathIcon,
  ShieldCheckIcon,
  BoltIcon,
  ClockIcon,
} from '@heroicons/react/24/outline'

type TimeRange = '24h' | '7d' | '30d'

export function UsagePage() {
  const { user } = useAuth()
  const [range, setRange] = useState<TimeRange>('24h')

  const { data: tenant } = useQuery({
    queryKey: ['tenant'],
    queryFn: async () => {
      const response = await api.get('/tenant')
      return response.data.tenant as Tenant
    },
    enabled: !!user,
  })

  const { data: usage, isLoading: isUsageLoading, refetch: refetchUsage } = useQuery({
    queryKey: ['usage'],
    queryFn: async () => {
      const response = await api.get('/tenant/usage')
      return response.data.usage as Usage
    },
    enabled: !!user,
  })

  const { data: series, isLoading: isSeriesLoading, refetch: refetchSeries } = useQuery({
    queryKey: ['usage-series', range],
    queryFn: async () => {
      const response = await api.get('/tenant/usage/series', { params: { range } })
      return response.data.series as UsageSeries
    },
    enabled: !!user,
  })

  const isLoading = isUsageLoading || isSeriesLoading
  const refetch = () => {
    void refetchUsage()
    void refetchSeries()
  }

  const points = series?.points || []
  const totalRequests = points.reduce((n, p) => n + (p.requests || 0), 0)
  const rateLimited = points.reduce((n, p) => n + (p.rate_limited || 0), 0)
  const concurrencyLimited = points.reduce((n, p) => n + (p.concurrency_limited || 0), 0)

  const opBreakdown = [
    { op: 'GET', count: points.reduce((n, p) => n + (p.by_op?.get || 0), 0) },
    { op: 'PUT', count: points.reduce((n, p) => n + (p.by_op?.put || 0), 0) },
    { op: 'CAS', count: points.reduce((n, p) => n + (p.by_op?.cas || 0), 0) },
    { op: 'DEL', count: points.reduce((n, p) => n + (p.by_op?.delete || p.by_op?.del || 0), 0) },
  ]

  const chartData = points.map((p) => ({
    time: format(new Date(p.t), range === '24h' ? 'HH:mm' : 'MMM d'),
    requests: p.requests || 0,
    rateLimited: p.rate_limited || 0,
  }))

  return (
    <div className="space-y-8 max-w-[1200px]">
      <PageHeader
        title="Usage Telemetry"
        subtitle="Throughput metrics, opcode latency breakdowns, and rate-limiting enforcement."
        actions={
          <div className="flex items-center gap-2 bg-mist-gray p-1 rounded-full border border-[#ececec]">
            {(['24h', '7d', '30d'] as TimeRange[]).map((r) => (
              <button
                key={r}
                onClick={() => setRange(r)}
                className={`px-4 py-1.5 text-xs font-medium rounded-full transition-all duration-150 ${
                  range === r
                    ? 'bg-paper-white text-ink-black shadow-subtle'
                    : 'text-slate-gray hover:text-ink-black'
                }`}
              >
                {r}
              </button>
            ))}
          </div>
        }
      />

      {usage?.since && (
        <div className="flex items-center justify-between text-xs text-slate-gray px-1 -mt-4">
          <span>
            Telemetry metrics recorded since{' '}
            <strong className="text-ink-black font-medium">
              {format(new Date(usage.since), 'MMM d, yyyy HH:mm')}
            </strong>
          </span>
          <button
            onClick={() => refetch()}
            className="inline-flex items-center gap-1 text-slate-gray hover:text-ink-black transition-colors"
          >
            <ArrowPathIcon className="h-3.5 w-3.5" />
            Refresh
          </button>
        </div>
      )}

      {/* Summary Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-5">
        <StatCard
          label="Total Requests"
          value={totalRequests.toLocaleString()}
          helperText={`Window: last ${range}`}
          icon={<ClockIcon className="h-5 w-5" />}
          loading={isLoading}
        />

        <StatCard
          label="Rate Limited (429)"
          value={rateLimited.toLocaleString()}
          delta={
            rateLimited > 0
              ? { value: `${((rateLimited / (totalRequests || 1)) * 100).toFixed(2)}% drop rate`, isPositive: false }
              : { value: 'Zero drops', isPositive: true }
          }
          icon={<ShieldCheckIcon className="h-5 w-5" />}
          loading={isLoading}
        />

        <StatCard
          label="Concurrency Drops"
          value={concurrencyLimited.toLocaleString()}
          helperText={`Max concurrent: ${tenant?.max_concurrent || 50}`}
          icon={<BoltIcon className="h-5 w-5" />}
          loading={isLoading}
        />

        <StatCard
          label="Quota Bandwidth"
          value={`${tenant?.effective_rps || 100} RPS`}
          helperText={`Burst capacity: ${tenant?.effective_burst || 200}`}
          icon={<ArrowPathIcon className="h-5 w-5" />}
          loading={isLoading}
        />
      </div>

      {/* Request Volume Timeline */}
      <Card variant="elevated">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-base font-medium text-ink-black">Request Volume Timeline</h2>
            <p className="text-xs text-slate-gray mt-0.5">Stored hourly buckets for this tenant (survives gateway restarts)</p>
          </div>
          <button
            onClick={() => refetch()}
            className="p-1.5 text-slate-gray hover:text-ink-black rounded-lg hover:bg-mist-gray transition-colors"
            title="Refresh metrics"
          >
            <ArrowPathIcon className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          </button>
        </div>

        <div className="h-72 w-full mt-4">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
              <defs>
                <linearGradient id="usageGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#5d2a1a" stopOpacity={0.25} />
                  <stop offset="95%" stopColor="#5d2a1a" stopOpacity={0.0} />
                </linearGradient>
              </defs>
              <XAxis dataKey="time" stroke="#979799" fontSize={12} tickLine={false} axisLine={{ stroke: '#ececec' }} />
              <YAxis stroke="#979799" fontSize={12} tickLine={false} axisLine={false} />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#ffffff',
                  borderColor: '#ececec',
                  borderRadius: '12px',
                  boxShadow: '0 4px 12px rgba(0,0,0,0.05)',
                  fontSize: '13px',
                }}
              />
              <Area
                type="monotone"
                dataKey="requests"
                stroke="#5d2a1a"
                strokeWidth={2}
                fillOpacity={1}
                fill="url(#usageGradient)"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </Card>

      {/* 2-Column: Op Breakdown & Response Statuses */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card variant="default">
          <h2 className="text-base font-medium text-ink-black mb-1">Operations Breakdown</h2>
          <p className="text-xs text-slate-gray mb-4">Count of key-value operations processed</p>

          <div className="h-52 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={opBreakdown} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                <XAxis dataKey="op" stroke="#979799" fontSize={12} tickLine={false} axisLine={false} />
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
        </Card>

        {/* Status Codes / Rate Limiting */}
        <Card variant="default" className="flex flex-col justify-between">
          <div>
            <h2 className="text-base font-medium text-ink-black mb-1">HTTP Status Codes</h2>
            <p className="text-xs text-slate-gray mb-4">Edge proxy response distribution</p>

            <div className="space-y-3">
              <div>
                <div className="flex justify-between text-xs mb-1 font-medium">
                  <span className="text-ink-black">200 OK / 201 Created</span>
                  <span className="text-slate-gray">{Math.max(0, totalRequests - rateLimited).toLocaleString()}</span>
                </div>
                <div className="h-2 rounded-full bg-mist-gray overflow-hidden">
                  <div
                    className="h-full bg-[#1a7f37] rounded-full"
                    style={{ width: `${totalRequests ? ((totalRequests - rateLimited) / totalRequests) * 100 : 100}%` }}
                  />
                </div>
              </div>

              <div>
                <div className="flex justify-between text-xs mb-1 font-medium">
                  <span className="text-ink-black">429 Too Many Requests</span>
                  <span className="text-slate-gray">{rateLimited.toLocaleString()}</span>
                </div>
                <div className="h-2 rounded-full bg-mist-gray overflow-hidden">
                  <div
                    className="h-full bg-amber-500 rounded-full"
                    style={{ width: `${totalRequests ? (rateLimited / totalRequests) * 100 : 0}%` }}
                  />
                </div>
              </div>

              <div>
                <div className="flex justify-between text-xs mb-1 font-medium">
                  <span className="text-ink-black">503 / 504 Concurrency Exceeded</span>
                  <span className="text-slate-gray">{concurrencyLimited.toLocaleString()}</span>
                </div>
                <div className="h-2 rounded-full bg-mist-gray overflow-hidden">
                  <div
                    className="h-full bg-red-500 rounded-full"
                    style={{ width: `${totalRequests ? (concurrencyLimited / totalRequests) * 100 : 0}%` }}
                  />
                </div>
              </div>
            </div>
          </div>

          <div className="pt-4 border-t border-[#ececec] text-xs text-slate-gray mt-6">
            Rate limiting is enforced at L4/L7 using in-memory atomic token-buckets backed by Raft leaseholder status.
          </div>
        </Card>
      </div>
    </div>
  )
}
