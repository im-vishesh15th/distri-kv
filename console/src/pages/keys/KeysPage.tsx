import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Link, useSearchParams } from 'react-router-dom'
import { format, formatDistanceToNow } from 'date-fns'
import {
  PlusIcon,
  TrashIcon,
  ArrowPathIcon,
  KeyIcon,
  CheckIcon,
} from '@heroicons/react/24/outline'
import { api } from '../../context/AuthContext'
import { toast } from 'react-hot-toast'
import { PageHeader } from '../../components/ui/PageHeader'
import { Button } from '../../components/ui/Button'
import { Card } from '../../components/ui/Card'
import { Badge } from '../../components/ui/Badge'
import { Modal } from '../../components/ui/Modal'
import { CopyInput } from '../../components/ui/CopyInput'
import { ConfirmDialog } from '../../components/ui/ConfirmDialog'
import { EmptyState } from '../../components/ui/EmptyState'
import { ApiKey, CreateKeyResponse } from '../../types/api'

function getExpiryInfo(expiresAt: string | null) {
  if (!expiresAt) {
    return { text: 'Never', colorClass: 'text-slate-gray' }
  }
  const date = new Date(expiresAt)
  const diffMs = date.getTime() - Date.now()
  if (diffMs <= 0) {
    return { text: 'Expired', colorClass: 'text-[#cf222e] font-semibold' }
  }
  const diffDays = diffMs / (1000 * 60 * 60 * 24)
  const distance = formatDistanceToNow(date, { addSuffix: true })

  if (diffDays < 1) {
    return { text: `Expires ${distance}`, colorClass: 'text-[#cf222e] font-semibold' }
  }
  if (diffDays < 7) {
    return { text: `Expires ${distance}`, colorClass: 'text-amber-600 font-medium' }
  }
  return { text: `Expires ${distance}`, colorClass: 'text-slate-gray' }
}

export function KeysPage() {
  const queryClient = useQueryClient()
  const [searchParams, setSearchParams] = useSearchParams()

  const [showCreateModal, setShowCreateModal] = useState(false)
  const [newKeyName, setNewKeyName] = useState('')
  const [newKeyTtl, setNewKeyTtl] = useState('720')

  // Key reveal modal state
  const [createdKeyData, setCreatedKeyData] = useState<CreateKeyResponse | null>(null)

  // Revoke confirm dialog state
  const [keyToRevoke, setKeyToRevoke] = useState<string | null>(null)

  useEffect(() => {
    if (searchParams.get('create') === 'true') {
      setShowCreateModal(true)
      searchParams.delete('create')
      setSearchParams(searchParams, { replace: true })
    }
  }, [searchParams, setSearchParams])

  const {
    data: keys = [],
    isLoading,
    error,
  } = useQuery({
    queryKey: ['tenantKeys'],
    queryFn: async () => {
      const response = await api.get('/tenant/keys')
      return response.data.keys as ApiKey[]
    },
  })

  const createKeyMutation = useMutation({
    mutationFn: async (data: { name: string; ttl_hours: number }) => {
      const response = await api.post('/tenant/keys', data)
      return response.data as CreateKeyResponse
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['tenantKeys'] })
      setShowCreateModal(false)
      setNewKeyName('')
      setCreatedKeyData(data)
      toast.success('API key generated successfully')
    },
    onError: (err: any) => {
      toast.error(err.response?.data?.error?.message || 'Failed to create key')
    },
  })

  const revokeKeyMutation = useMutation({
    mutationFn: (prefix: string) => api.delete(`/tenant/keys/${prefix}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['tenantKeys'] })
      toast.success('Key revoked')
      setKeyToRevoke(null)
    },
    onError: () => {
      toast.error('Failed to revoke key')
      setKeyToRevoke(null)
    },
  })

  const rotateKeyMutation = useMutation({
    mutationFn: async (prefix: string) => {
      const response = await api.post(`/tenant/keys/${prefix}/rotate`, {})
      return response.data as CreateKeyResponse
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['tenantKeys'] })
      setCreatedKeyData(data)
      toast.success('Key rotated successfully')
    },
    onError: () => toast.error('Failed to rotate key'),
  })

  const activeKeys = keys.filter((k) => k.status === 'active').length
  const totalKeys = keys.length

  return (
    <div className="space-y-8 max-w-[1200px]">
      <PageHeader
        title="API Keys"
        subtitle="Manage cryptographic credentials for data plane read and write operations."
        actions={
          <Button variant="primary" onClick={() => setShowCreateModal(true)}>
            <PlusIcon className="h-5 w-5 shrink-0" />
            <span>New API key</span>
          </Button>
        }
      />

      {/* Summary Row */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <Card variant="default" className="py-4">
          <div className="text-xs font-medium uppercase tracking-wider text-slate-gray">
            Total Keys
          </div>
          <div className="text-2xl font-medium text-ink-black mt-1">{totalKeys}</div>
        </Card>
        <Card variant="default" className="py-4">
          <div className="text-xs font-medium uppercase tracking-wider text-slate-gray">
            Active Keys
          </div>
          <div className="text-2xl font-medium text-[#1a7f37] mt-1">{activeKeys}</div>
        </Card>
        <Card variant="default" className="py-4">
          <div className="text-xs font-medium uppercase tracking-wider text-slate-gray">
            Revoked / Expired
          </div>
          <div className="text-2xl font-medium text-slate-gray mt-1">
            {totalKeys - activeKeys}
          </div>
        </Card>
      </div>

      {/* Main Table Card */}
      <Card variant="default" className="p-0 overflow-hidden">
        {isLoading ? (
          <div className="p-16 text-center">
            <div className="animate-spin rounded-full h-8 w-8 border-2 border-ink-black/20 border-t-ink-black mx-auto mb-4" />
            <p className="text-sm text-slate-gray">Retrieving credentials...</p>
          </div>
        ) : error ? (
          <div className="p-16 text-center">
            <p className="text-sm text-red-500 mb-4">Failed to load API keys</p>
            <Button
              variant="secondary"
              onClick={() => queryClient.invalidateQueries({ queryKey: ['tenantKeys'] })}
            >
              Retry
            </Button>
          </div>
        ) : keys.length === 0 ? (
          <div className="p-12">
            <EmptyState
              icon={<KeyIcon className="h-10 w-10 text-ash-gray" />}
              title="No API keys found"
              description="Create your first cryptographic key to authenticate your SDK or HTTP client against the DistriKV cluster."
              action={
                <Button variant="primary" onClick={() => setShowCreateModal(true)}>
                  <PlusIcon className="h-4 w-4 mr-1.5 inline" />
                  Generate API key
                </Button>
              }
            />
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse text-sm">
              <thead>
                <tr className="border-b border-[#ececec] bg-fog-white/60 text-xs uppercase tracking-wider text-slate-gray">
                  <th className="py-3 px-6 font-medium">Prefix</th>
                  <th className="py-3 px-6 font-medium">Name</th>
                  <th className="py-3 px-6 font-medium">Status</th>
                  <th className="py-3 px-6 font-medium">Created</th>
                  <th className="py-3 px-6 font-medium">Expires</th>
                  <th className="py-3 px-6 font-medium text-right">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[#ececec]">
                {keys.map((key) => (
                  <tr key={key.prefix} className="hover:bg-fog-white/40 transition-colors">
                    <td className="py-4 px-6 font-mono text-xs text-ink-black">
                      <Link
                        to={`/keys/${key.prefix}`}
                        className="bg-mist-gray hover:bg-[#eaeaea] transition-colors px-2 py-1 rounded-md font-mono"
                      >
                        {key.prefix}••••••••
                      </Link>
                    </td>
                    <td className="py-4 px-6 font-medium text-ink-black">
                      <Link to={`/keys/${key.prefix}`} className="hover:underline">
                        {key.name}
                      </Link>
                    </td>
                    <td className="py-4 px-6">
                      <Badge status={key.status} />
                    </td>
                    <td className="py-4 px-6 text-slate-gray text-xs">
                      {format(new Date(key.created_at), 'MMM d, yyyy HH:mm')}
                    </td>
                    <td className="py-4 px-6 text-xs">
                      {key.expires_at ? (
                        <div>
                          <div className={getExpiryInfo(key.expires_at).colorClass}>
                            {getExpiryInfo(key.expires_at).text}
                          </div>
                          <div className="text-[11px] text-smoke-gray mt-0.5">
                            {format(new Date(key.expires_at), 'MMM d, yyyy')}
                          </div>
                        </div>
                      ) : (
                        <span className="text-slate-gray">Never</span>
                      )}
                    </td>
                    <td className="py-4 px-6 text-right">
                      <div className="flex items-center justify-end gap-1.5">
                        {key.status === 'active' ? (
                          <>
                            <button
                              onClick={() => rotateKeyMutation.mutate(key.prefix)}
                              disabled={rotateKeyMutation.isPending}
                              className="p-2 text-slate-gray hover:text-ink-black hover:bg-mist-gray rounded-lg transition-colors"
                              title="Rotate key"
                            >
                              <ArrowPathIcon className="h-5 w-5" />
                            </button>
                            <button
                              onClick={() => setKeyToRevoke(key.prefix)}
                              className="p-2 text-slate-gray hover:text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                              title="Revoke key"
                            >
                              <TrashIcon className="h-5 w-5" />
                            </button>
                          </>
                        ) : (
                          <span className="text-xs text-slate-gray px-2">—</span>
                        )}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      {/* Create Key Modal */}
      <Modal
        open={showCreateModal}
        onClose={() => setShowCreateModal(false)}
        title="Create API Key"
        description="Keys grant full read/write access to your tenant partition."
      >
        <form
          onSubmit={(e) => {
            e.preventDefault()
            createKeyMutation.mutate({
              name: newKeyName,
              ttl_hours: parseInt(newKeyTtl, 10),
            })
          }}
          className="space-y-4 pt-2"
        >
          <div>
            <label className="block mb-1.5 text-xs font-medium text-slate-gray">
              Key Name / Description
            </label>
            <input
              type="text"
              value={newKeyName}
              onChange={(e) => setNewKeyName(e.target.value)}
              className="w-full bg-mist-gray border border-transparent rounded-[16px] px-3.5 py-2.5 text-sm text-ink-black focus:outline-none focus:bg-paper-white focus:border-ink-black"
              placeholder="e.g. production-backend-cluster"
              required
              maxLength={64}
              autoFocus
            />
          </div>

          <div>
            <label className="block mb-1.5 text-xs font-medium text-slate-gray">
              Key Expiration (TTL)
            </label>
            <select
              value={newKeyTtl}
              onChange={(e) => setNewKeyTtl(e.target.value)}
              className="w-full bg-mist-gray border border-transparent rounded-[16px] px-3.5 py-2.5 text-sm text-ink-black focus:outline-none focus:bg-paper-white focus:border-ink-black cursor-pointer"
            >
              <option value="0">Never expires</option>
              <option value="24">24 hours (1 day)</option>
              <option value="168">7 days (1 week)</option>
              <option value="720">30 days (1 month)</option>
              <option value="2160">90 days (1 quarter)</option>
              <option value="8760">365 days (1 year)</option>
            </select>
          </div>

          <div className="flex items-center justify-end gap-3 pt-4 border-t border-[#ececec]">
            <Button
              type="button"
              variant="ghost"
              onClick={() => setShowCreateModal(false)}
            >
              Cancel
            </Button>
            <Button
              type="submit"
              variant="primary"
              loading={createKeyMutation.isPending}
              disabled={!newKeyName.trim()}
            >
              Generate Key
            </Button>
          </div>
        </form>
      </Modal>

      {/* Secret Key Reveal Modal (Replaces window.alert) */}
      <Modal
        open={!!createdKeyData}
        onClose={() => setCreatedKeyData(null)}
        title="API Key Created"
      >
        <div className="space-y-4 pt-2">
          {/* Blush Peach Warning Accent Card */}
          <div className="p-4 rounded-[16px] bg-blush-peach text-sienna-brown border border-blush-peach space-y-1">
            <div className="font-semibold text-xs tracking-wider uppercase">
              Save your key now
            </div>
            <p className="text-xs leading-relaxed">
              This secret will not be displayed again. If you lose it, you will need to rotate or generate a new key.
            </p>
          </div>

          <div>
            <CopyInput
              label="Secret Key"
              value={createdKeyData?.key || ''}
              secret={true}
            />
          </div>

          <div className="pt-2 flex justify-end">
            <Button
              variant="primary"
              onClick={() => setCreatedKeyData(null)}
              className="w-full"
            >
              <CheckIcon className="h-4 w-4 mr-1.5 inline" />
              I have safely copied my key
            </Button>
          </div>
        </div>
      </Modal>

      {/* Revoke Key Confirmation Dialog */}
      <ConfirmDialog
        open={!!keyToRevoke}
        onClose={() => setKeyToRevoke(null)}
        onConfirm={() => {
          if (keyToRevoke) revokeKeyMutation.mutate(keyToRevoke)
        }}
        title="Revoke API Key"
        description="Are you sure you want to revoke this key? Any client applications or microservices actively using this credential will immediately receive 401 Unauthorized."
        confirmText="Revoke Key"
        loading={revokeKeyMutation.isPending}
      />
    </div>
  )
}