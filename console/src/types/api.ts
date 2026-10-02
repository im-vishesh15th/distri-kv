// Consolidated API type definitions for DistriKV Console
// Hand-written until Orval + OpenAPI spec is wired up

export interface User {
  id: string
  email: string
  is_admin: boolean
  tenant_id?: string
  created_at: string
}

export interface Tenant {
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

export interface ApiKey {
  prefix: string
  name: string
  status: 'active' | 'expired' | 'revoked'
  created_at: string
  expires_at: string | null
  revoked_at: string | null
}

export interface CreateKeyResponse {
  key: string
  info: ApiKey
}

export interface Usage {
  since: string
  requests: number
  by_op: Record<string, number>
  by_status: Record<string, number>
  rate_limited: number
  concurrency_limited: number
}

export interface UsagePoint {
  t: string
  requests: number
  by_op: Record<string, number>
  rate_limited: number
  concurrency_limited: number
}

export interface UsageSeries {
  range: string
  step: string
  points: UsagePoint[]
}

// Admin types
export interface AdminTenant extends Tenant {
  key_count?: number
  request_count?: number
}

export interface PaginatedResponse<T> {
  items: T[]
  total: number
  page: number
  page_size: number
}

export interface ApiError {
  error: {
    code: string
    message: string
  }
}
