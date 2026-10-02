import { useState, useEffect, useRef } from 'react'
import { Link } from 'react-router-dom'
import {
  ShieldCheckIcon,
  CheckIcon,
  ArrowRightIcon,
  ChevronDownIcon,
  ServerStackIcon,
  CommandLineIcon,
  CpuChipIcon,
  BookOpenIcon,
} from '@heroicons/react/24/outline'
import { useAuth } from '../../context/AuthContext'

const TYPED_STRINGS = [
  'microsecond speed.',
  'strict linearizability.',
  'zero-downtime scale.',
  'Raft consensus quorum.',
]

function useTypingEffect(strings: string[], speed = 55, pause = 2000) {
  const [displayed, setDisplayed] = useState('')
  const [idx, setIdx] = useState(0)
  const [charIdx, setCharIdx] = useState(0)
  const [deleting, setDeleting] = useState(false)

  useEffect(() => {
    const current = strings[idx]
    let timeout: ReturnType<typeof setTimeout>

    if (!deleting && charIdx <= current.length) {
      timeout = setTimeout(() => {
        setDisplayed(current.slice(0, charIdx))
        setCharIdx((c) => c + 1)
      }, speed)
    } else if (!deleting && charIdx > current.length) {
      timeout = setTimeout(() => setDeleting(true), pause)
    } else if (deleting && charIdx > 0) {
      timeout = setTimeout(() => {
        setDisplayed(current.slice(0, charIdx - 1))
        setCharIdx((c) => c - 1)
      }, speed / 2)
    } else {
      setDeleting(false)
      setIdx((i) => (i + 1) % strings.length)
    }

    return () => clearTimeout(timeout)
  }, [charIdx, deleting, idx, strings, speed, pause])

  return displayed
}

function StatCounter({ target, suffix = '', duration = 1200 }: { target: number; suffix?: string; duration?: number }) {
  const [val, setVal] = useState(0)
  const ref = useRef<HTMLDivElement>(null)
  const started = useRef(false)

  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting && !started.current) {
          started.current = true
          const step = target / (duration / 16)
          let current = 0
          const timer = setInterval(() => {
            current = Math.min(current + step, target)
            setVal(Math.floor(current))
            if (current >= target) clearInterval(timer)
          }, 16)
        }
      },
      { threshold: 0.2 }
    )
    if (ref.current) observer.observe(ref.current)
    return () => observer.disconnect()
  }, [target, duration])

  return <div ref={ref}>{val.toLocaleString()}{suffix}</div>
}

type LangTab = 'curl' | 'ts' | 'python' | 'go' | 'rust'

export function LandingPage() {
  const { user } = useAuth()
  const typed = useTypingEffect(TYPED_STRINGS)
  const [activeTab, setActiveTab] = useState<LangTab>('curl')
  const [copiedKey, setCopiedKey] = useState(false)
  const [openFaq, setOpenFaq] = useState<number | null>(null)

  const handleCopyCmd = () => {
    navigator.clipboard.writeText('curl -X PUT http://localhost:8080/kv/user:101 -H "Authorization: Bearer $DKV_KEY" -d \'{"value":"{\\"status\\":\\"active\\"}"}\'')
    setCopiedKey(true)
    setTimeout(() => setCopiedKey(false), 2000)
  }

  const codeSnippets: Record<LangTab, { request: string; response: string }> = {
    curl: {
      request: `# 1. Write user state (JSON or string payload)
curl -X PUT "http://localhost:8080/kv/user:profile:1001" \\
  -H "Authorization: Bearer dkv_live_9a8f2b1c..." \\
  -H "Content-Type: application/json" \\
  -d '{"value":"{\\"name\\":\\"Alice\\",\\"role\\":\\"engineer\\"}"}'

# 2. Read back with strict linearizability
curl -X GET "http://localhost:8080/kv/user:profile:1001" \\
  -H "Authorization: Bearer dkv_live_9a8f2b1c..."

# 3. Atomic Compare-And-Swap (CAS) lock
curl -X POST "http://localhost:8080/kv" \\
  -H "Authorization: Bearer dkv_live_9a8f2b1c..." \\
  -H "Content-Type: application/json" \\
  -d '{"op":"cas","key":"lock:deploy","expected":"idle","value":"in_progress"}'`,
      response: `// HTTP/1.1 200 OK
{
  "ok": true
}

// GET response:
{
  "value": "{\\"name\\":\\"Alice\\",\\"role\\":\\"engineer\\"}"
}

// CAS response:
{
  "ok": true
}`,
    },
    ts: {
      request: `import { DistriKV } from '@distrikv/client'

const dkv = new DistriKV({
  endpoint: 'http://localhost:8080',
  apiKey: 'dkv_live_9a8f2b1c...',
})

// Store JSON payload
await dkv.put('user:profile:1001', {
  name: 'Alice',
  role: 'engineer',
})

// Linearizable Read
const profile = await dkv.get<UserProfile>('user:profile:1001')

// Atomic Compare-And-Swap (returns false if expected mismatch)
const acquired = await dkv.cas('lock:deploy', 'idle', 'in_progress')
console.log('Lock acquired:', acquired)`,
      response: `// Console output:
{
  name: "Alice",
  role: "engineer"
}
Lock acquired: true`,
    },
    python: {
      request: `import requests, json

BASE = "http://localhost:8080"
HEADERS = {
    "Authorization": "Bearer dkv_live_9a8f2b1c...",
    "Content-Type": "application/json"
}

# 1. Put key
val = json.dumps({"name": "Alice", "role": "engineer"})
requests.put(f"{BASE}/kv/user:profile:1001", json={"value": val}, headers=HEADERS)

# 2. Get key
r = requests.get(f"{BASE}/kv/user:profile:1001", headers=HEADERS)
print(json.loads(r.json()["value"]))

# 3. Atomic CAS
cas_payload = {"op": "cas", "key": "lock:deploy", "expected": "idle", "value": "in_progress"}
requests.post(f"{BASE}/kv", json=cas_payload, headers=HEADERS)`,
      response: `{"name": "Alice", "role": "engineer"}
HTTP 200 OK: {"ok": true}`,
    },
    go: {
      request: `package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "net/http"
)

const gateway = "http://localhost:8080"
const token = "Bearer dkv_live_9a8f2b1c..."

func main() {
    // Write key
    body, _ := json.Marshal(map[string]string{"value": "{\\"name\\":\\"Alice\\"}"})
    req, _ := http.NewRequest("PUT", gateway+"/kv/user:profile:1001", bytes.NewReader(body))
    req.Header.Set("Authorization", token)
    req.Header.Set("Content-Type", "application/json")
    resp, _ := http.DefaultClient.Do(req)
    defer resp.Body.Close()

    fmt.Println("PUT Status:", resp.Status)
}`,
      response: `PUT Status: 200 OK
GET Status: 200 OK (p99 0.42ms)`,
    },
    rust: {
      request: `use reqwest::header::{HeaderMap, AUTHORIZATION, CONTENT_TYPE};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), reqwest::Error> {
    let client = reqwest::Client::new();
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Bearer dkv_live_9a8f2b1c...".parse().unwrap());
    headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

    // Atomic PUT
    let res = client.put("http://localhost:8080/kv/user:profile:1001")
        .headers(headers.clone())
        .json(&json!({"value": "{\\"name\\":\\"Alice\\"}"}))
        .send().await?;

    println!("Status: {}", res.status());
    Ok(())
}`,
      response: `Status: 200 OK
Response: {"ok": true}`,
    },
  }

  const faqs = [
    {
      q: 'How does DistriKV differ from Redis and DynamoDB?',
      a: 'DistriKV unites the sub-millisecond p99 speed of in-memory caching with strict Raft linearizable consensus and hardware-isolated multi-tenant rate limiting. Unlike Redis, writes are verified by a replicated quorum before returning 200 OK. Unlike DynamoDB, tail latencies stay below 1ms with predictable token-bucket hardware quotas and zero noisy-neighbor interference.',
    },
    {
      q: 'How does multi-tenancy work across tenants?',
      a: 'Every customer namespace is cryptographically partitioned by tenant ID at the edge gateway. Token-bucket rate limiters enforce sustained RPS and burst ceilings per tenant with sub-microsecond overhead. Even under severe saturation by one tenant, other tenants experience zero tail latency degradation.',
    },
    {
      q: 'What happens if a Raft leader node crashes?',
      a: 'The remaining follower nodes trigger an immediate randomized election under the Raft consensus protocol. A new leader is elected in under 150ms. Client requests to the edge gateway automatically route to the new quorum leader with zero data loss.',
    },
    {
      q: 'How do I authenticate client applications?',
      a: 'Every tenant generates API keys in the console (e.g., dkv_live_...). Secrets are hashed using Argon2/SHA-256 and verified at the gateway. Requests pass the standard Authorization: Bearer <KEY> header.',
    },
  ]

  return (
    <div className="min-h-screen bg-paper-white text-ink-black flex flex-col selection:bg-blush-peach selection:text-sienna-brown overflow-x-hidden">
      {/* ─── Top Header Navigation ──────────────────────────── */}
      <header
        className="sticky top-0 z-50"
        style={{
          background: 'rgba(255,255,255,0.93)',
          backdropFilter: 'blur(16px)',
          WebkitBackdropFilter: 'blur(16px)',
          borderBottom: '1px solid #ececec',
        }}
      >
        <div className="max-w-[1240px] mx-auto px-6 sm:px-8 flex items-center justify-between" style={{ height: '64px' }}>
          {/* Logo */}
          <Link to="/" className="inline-flex items-center gap-2" style={{ textDecoration: 'none' }}>
            <span style={{ fontFamily: "'Georgia', ui-serif, serif", fontSize: '20px', fontWeight: 400, letterSpacing: '-0.02em', color: '#17191c' }}>
              Distri<span style={{ fontStyle: 'italic', color: '#5d2a1a' }}>KV</span>
            </span>
            <span
              style={{
                fontSize: '10.5px',
                fontWeight: 600,
                textTransform: 'uppercase',
                letterSpacing: '0.06em',
                padding: '2px 8px',
                borderRadius: '9999px',
                background: '#f2f2f3',
                color: '#777b86',
                border: '1px solid #e4e4e6',
              }}
            >
              v1.0
            </span>
          </Link>

          {/* Center nav links */}
          <nav className="hidden md:flex items-center gap-6">
            {[
              { label: 'Architecture', href: '#architecture' },
              { label: 'Benchmarks', href: '#benchmarks' },
              { label: 'API Reference', href: '#api' },
            ].map(({ label, href }) => (
              <a
                key={href}
                href={href}
                style={{ fontSize: '13.5px', fontWeight: 400, color: '#777b86', textDecoration: 'none', transition: 'color 120ms' }}
                onMouseEnter={(e) => { e.currentTarget.style.color = '#17191c' }}
                onMouseLeave={(e) => { e.currentTarget.style.color = '#777b86' }}
              >
                {label}
              </a>
            ))}
            <Link
              to="/docs"
              style={{ fontSize: '13.5px', fontWeight: 400, color: '#777b86', textDecoration: 'none', display: 'inline-flex', alignItems: 'center', gap: '5px', transition: 'color 120ms' }}
              onMouseEnter={(e) => { e.currentTarget.style.color = '#17191c' }}
              onMouseLeave={(e) => { e.currentTarget.style.color = '#777b86' }}
            >
              Docs
              <span style={{ fontSize: '10px', fontWeight: 600, background: '#dcfce7', color: '#15803d', padding: '1px 5px', borderRadius: '4px' }}>public</span>
            </Link>
          </nav>

          {/* Right CTAs */}
          <div className="flex items-center gap-2">
            {user ? (
              <Link to={user.is_admin ? '/admin' : '/dashboard'} style={{ textDecoration: 'none' }}>
                <button
                  style={{ fontSize: '13.5px', fontWeight: 500, color: '#fff', background: '#17191c', padding: '9px 18px', borderRadius: '9999px', border: 'none', cursor: 'pointer', transition: 'opacity 140ms' }}
                  onMouseEnter={(e) => { e.currentTarget.style.opacity = '0.8' }}
                  onMouseLeave={(e) => { e.currentTarget.style.opacity = '1' }}
                >
                  Console →
                </button>
              </Link>
            ) : (
              <>
                <Link to="/login" style={{ textDecoration: 'none' }}>
                  <button
                    style={{ fontSize: '13.5px', fontWeight: 400, color: '#777b86', background: 'transparent', padding: '9px 16px', borderRadius: '9999px', border: 'none', cursor: 'pointer', transition: 'background 140ms, color 140ms' }}
                    onMouseEnter={(e) => { e.currentTarget.style.background = '#f2f2f3'; e.currentTarget.style.color = '#17191c' }}
                    onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = '#777b86' }}
                  >
                    Sign in
                  </button>
                </Link>
                <Link to="/signup" style={{ textDecoration: 'none' }}>
                  <button
                    style={{ fontSize: '13.5px', fontWeight: 500, color: '#fff', background: '#17191c', padding: '9px 18px', borderRadius: '9999px', border: 'none', cursor: 'pointer', transition: 'opacity 140ms' }}
                    onMouseEnter={(e) => { e.currentTarget.style.opacity = '0.82' }}
                    onMouseLeave={(e) => { e.currentTarget.style.opacity = '1' }}
                  >
                    Get started →
                  </button>
                </Link>
              </>
            )}
          </div>
        </div>
      </header>

      {/* ─── Hero Section ───────────────────────────────────── */}
      <section className="relative pt-16 sm:pt-24 pb-28 px-6 sm:px-8 overflow-hidden">
        {/* Subtle grid pattern background */}
        <div className="absolute inset-0 pointer-events-none" aria-hidden="true">
          <div
            className="absolute inset-0 opacity-[0.03]"
            style={{
              backgroundImage: 'linear-gradient(#000 1px, transparent 1px), linear-gradient(90deg, #000 1px, transparent 1px)',
              backgroundSize: '40px 40px',
            }}
          />
          <div className="absolute top-[-100px] left-1/2 -translate-x-1/2 w-[750px] h-[550px] rounded-full bg-blush-peach/30 blur-[130px]" />
          <div className="absolute bottom-[-60px] right-[-100px] w-[500px] h-[500px] rounded-full bg-sienna-brown/10 blur-[110px]" />
        </div>

        <div className="max-w-[1100px] mx-auto text-center relative space-y-7">
          {/* Status Badge */}
          <div className="inline-flex items-center gap-2.5 px-4 py-1.5 rounded-full bg-paper-white border border-[#e2e2e4] shadow-subtle text-xs font-medium text-slate-gray">
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
              <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
            </span>
            <span>DistriKV Engine v1.0 · Multi-Raft Quorum · Token-Bucket Isolation</span>
          </div>

          {/* Main Headline */}
          <h1 className="font-serif text-[42px] sm:text-[68px] lg:text-[84px] leading-[1.08] text-ink-black font-normal tracking-[-2px] max-w-[960px] mx-auto">
            Distributed key-value storage at{' '}
            <span className="italic text-sienna-brown block sm:inline">
              {typed}
              <span className="animate-pulse font-light text-ink-black">|</span>
            </span>
          </h1>

          {/* Subtitle */}
          <p className="max-w-[660px] mx-auto text-base sm:text-lg text-slate-gray font-normal leading-relaxed">
            Hardware-isolated multi-tenancy, sub-millisecond p99 latency, and strict linearizability. 
            Engineered in Go for mission-critical session hierarchies, distributed locking, and fast operational caches.
          </p>

          {/* CTA Buttons */}
          <div className="flex flex-wrap items-center justify-center gap-4 pt-3">
            <Link to="/signup">
              <button className="inline-flex items-center gap-2 text-sm sm:text-base font-medium text-paper-white bg-ink-black hover:bg-ink-black/85 transition-all px-8 py-3.5 rounded-full shadow-md hover:shadow-lg hover:-translate-y-0.5 active:translate-y-0">
                <span>Start building free</span>
                <ArrowRightIcon className="h-4 w-4" />
              </button>
            </Link>
            <Link to="/docs">
              <button className="inline-flex items-center gap-2 text-sm sm:text-base font-medium text-ink-black bg-paper-white hover:bg-mist-gray transition-all px-7 py-3.5 rounded-full border border-[#d8d8d8] shadow-subtle">
                <BookOpenIcon className="h-4 w-4 text-slate-gray" />
                <span>Read documentation</span>
              </button>
            </Link>
          </div>

          {/* Quick Copy Command Badge */}
          <div className="pt-2">
            <button
              onClick={handleCopyCmd}
              className="inline-flex items-center gap-2 text-xs font-mono bg-mist-gray hover:bg-[#e6e6e8] border border-[#dddddf] text-ink-black px-4 py-2 rounded-full transition-all group"
              title="Click to copy curl command"
            >
              <CommandLineIcon className="h-3.5 w-3.5 text-slate-gray group-hover:text-ink-black" />
              <span>curl -X PUT http://localhost:8080/kv/hello -H "Authorization: Bearer $KEY"</span>
              <span className="ml-1 text-[11px] text-slate-gray">
                {copiedKey ? '✓ Copied!' : 'Copy'}
              </span>
            </button>
          </div>

          {/* 3 Metric Highlight Cards */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-5 pt-8 text-left max-w-[1000px] mx-auto">
            <div className="bg-paper-white border border-[#e5e5e7] rounded-cards p-6 shadow-subtle hover:border-ink-black/30 transition-all">
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray mb-1">
                Cluster Consensus
              </div>
              <div className="text-2xl font-serif text-ink-black font-normal mt-1">
                Raft Protocol
              </div>
              <p className="text-xs text-slate-gray mt-2 leading-relaxed">
                Atomic log replication with automated leader election, continuous heartbeats, and zero split-brain guarantees.
              </p>
              <div className="mt-4 pt-3 border-t border-[#ececec] flex items-center gap-1.5 text-xs text-[#1a7f37] font-medium">
                <CheckIcon className="h-3.5 w-3.5" />
                <span>Quorum durability (2N/2+1)</span>
              </div>
            </div>

            <div className="bg-blush-peach/40 border border-blush-peach rounded-cards p-6 shadow-subtle hover:border-sienna-brown/40 transition-all">
              <div className="text-xs font-semibold uppercase tracking-wider text-sienna-brown mb-1">
                Benchmark Telemetry
              </div>
              <div className="text-3xl font-serif text-ink-black font-normal mt-1 flex items-baseline gap-1">
                <StatCounter target={420} suffix=" µs" />
              </div>
              <p className="text-xs text-slate-gray mt-2 leading-relaxed">
                Deterministic sub-millisecond p99 latency across geo-distributed nodes under continuous saturation.
              </p>
              <div className="mt-4 pt-3 border-t border-blush-peach flex items-center gap-1.5 text-xs text-sienna-brown font-semibold">
                <span>100,000+ sustained QPS per node</span>
              </div>
            </div>

            <div className="bg-paper-white border border-[#e5e5e7] rounded-cards p-6 shadow-subtle hover:border-ink-black/30 transition-all">
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray mb-1">
                Tenant Isolation
              </div>
              <div className="text-2xl font-serif text-ink-black font-normal mt-1">
                Token-Bucket Quotas
              </div>
              <p className="text-xs text-slate-gray mt-2 leading-relaxed">
                Hardware-isolated namespaces prevent noisy neighbors from degrading latency or bandwidth.
              </p>
              <div className="mt-4 pt-3 border-t border-[#ececec] flex items-center gap-1.5 text-xs text-slate-gray">
                <ShieldCheckIcon className="h-3.5 w-3.5 text-ink-black" />
                <span>Layer-7 RPS & Burst Enforced</span>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* ─── Interactive Code & API Playground Section ──────── */}
      <section id="api" className="py-24 px-6 sm:px-8 bg-mist-gray border-y border-[#ececec]">
        <div className="max-w-[1100px] mx-auto">
          <div className="text-center max-w-xl mx-auto mb-12 space-y-3">
            <h2 className="font-serif text-3xl sm:text-4xl text-ink-black font-normal tracking-tight">
              One standard API across all languages
            </h2>
            <p className="text-sm text-slate-gray">
              Direct edge data plane calls on port <code className="font-mono text-xs bg-paper-white px-2 py-0.5 rounded border border-[#e0e0e0]">8080</code>.
              Authenticate with your cryptographic bearer token.
            </p>
          </div>

          {/* Code Window Container */}
          <div className="bg-[#121316] text-[#eaeaea] rounded-[24px] shadow-2xl overflow-hidden border border-[#26282e]">
            {/* Window header with language tabs */}
            <div className="flex flex-wrap items-center justify-between border-b border-[#26282e] px-6 py-3 bg-[#181a1f]">
              <div className="flex items-center gap-2">
                <span className="h-3 w-3 rounded-full bg-[#ff5f56]" />
                <span className="h-3 w-3 rounded-full bg-[#ffbd2e]" />
                <span className="h-3 w-3 rounded-full bg-[#27c93f]" />
                <span className="ml-3 text-xs font-mono text-[#8a8f98]">DistriKV Data Plane — :8080</span>
              </div>

              {/* Language Switcher Tabs */}
              <div className="flex items-center gap-1 bg-[#121316] p-1 rounded-xl border border-[#2a2c33]">
                {(['curl', 'ts', 'python', 'go', 'rust'] as LangTab[]).map((tab) => (
                  <button
                    key={tab}
                    onClick={() => setActiveTab(tab)}
                    className={`px-3 py-1 rounded-lg text-xs font-medium transition-all ${
                      activeTab === tab
                        ? 'bg-[#2b2d35] text-paper-white shadow-sm font-semibold'
                        : 'text-[#8a8f98] hover:text-[#eaeaea]'
                    }`}
                  >
                    {tab === 'curl' ? 'cURL' : tab === 'ts' ? 'TypeScript' : tab === 'python' ? 'Python' : tab === 'go' ? 'Go' : 'Rust'}
                  </button>
                ))}
              </div>
            </div>

            {/* Code Body Grid */}
            <div className="grid grid-cols-1 lg:grid-cols-12 divide-y lg:divide-y-0 lg:divide-x divide-[#26282e]">
              {/* Request Snippet */}
              <div className="lg:col-span-7 p-6 overflow-x-auto">
                <div className="text-[11px] uppercase tracking-wider text-[#8a8f98] font-mono mb-3 font-semibold">
                  Request Implementation ({activeTab.toUpperCase()})
                </div>
                <pre className="font-mono text-xs sm:text-sm text-[#e2e4e8] leading-relaxed">
                  <code>{codeSnippets[activeTab].request}</code>
                </pre>
              </div>

              {/* Response Preview */}
              <div className="lg:col-span-5 p-6 bg-[#0f1013] overflow-x-auto">
                <div className="text-[11px] uppercase tracking-wider text-[#8a8f98] font-mono mb-3 font-semibold">
                  Live Gateway Response
                </div>
                <pre className="font-mono text-xs sm:text-sm text-[#38d47b] leading-relaxed">
                  <code>{codeSnippets[activeTab].response}</code>
                </pre>
              </div>
            </div>
          </div>

          <div className="mt-6 flex items-center justify-between text-xs text-slate-gray px-2">
            <span>Sub-millisecond p99 latency guaranteed by quorum consensus.</span>
            <Link to="/docs" className="text-ink-black font-semibold hover:underline inline-flex items-center gap-1">
              Explore all language SDK guides & error references →
            </Link>
          </div>
        </div>
      </section>

      {/* ─── Architecture Topology Section ─────────────────── */}
      <section id="architecture" className="py-24 px-6 sm:px-8">
        <div className="max-w-[1100px] mx-auto">
          <div className="text-center max-w-xl mx-auto mb-16 space-y-3">
            <h2 className="font-serif text-3xl sm:text-4xl text-ink-black font-normal tracking-tight">
              Hardware-Isolated Distributed Architecture
            </h2>
            <p className="text-sm text-slate-gray">
              Engineered with an intelligent edge gateway and a replicated Raft state machine.
            </p>
          </div>

          {/* Interactive Topology Cards */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6 relative">
            <div className="bg-paper-white border border-[#e4e4e6] rounded-cards p-7 shadow-subtle flex flex-col justify-between space-y-4">
              <div>
                <div className="h-10 w-10 rounded-xl bg-mist-gray flex items-center justify-center mb-4 text-ink-black">
                  <CommandLineIcon className="h-5 w-5" />
                </div>
                <h3 className="font-serif text-xl font-normal text-ink-black">1. Edge Gateway</h3>
                <p className="text-xs text-slate-gray mt-2 leading-relaxed">
                  The single published host interface (:8080). Performs zero-allocation HTTP routing, Argon2 cryptographic authentication, and namespace isolation.
                </p>
              </div>
              <div className="pt-3 border-t border-[#ececec] text-xs font-mono text-sienna-brown font-medium">
                Token-Bucket Rate Limiter
              </div>
            </div>

            <div className="bg-paper-white border border-[#e4e4e6] rounded-cards p-7 shadow-subtle flex flex-col justify-between space-y-4">
              <div>
                <div className="h-10 w-10 rounded-xl bg-blush-peach/40 flex items-center justify-center mb-4 text-sienna-brown">
                  <ServerStackIcon className="h-5 w-5" />
                </div>
                <h3 className="font-serif text-xl font-normal text-ink-black">2. Multi-Node Raft</h3>
                <p className="text-xs text-slate-gray mt-2 leading-relaxed">
                  Cluster nodes maintain an append-only fsynced Raft write-ahead log. Automatic leader election completes in &lt;150ms during node failure.
                </p>
              </div>
              <div className="pt-3 border-t border-[#ececec] text-xs font-mono text-[#1a7f37] font-medium">
                Quorum Log Replication
              </div>
            </div>

            <div className="bg-paper-white border border-[#e4e4e6] rounded-cards p-7 shadow-subtle flex flex-col justify-between space-y-4">
              <div>
                <div className="h-10 w-10 rounded-xl bg-mist-gray flex items-center justify-center mb-4 text-ink-black">
                  <CpuChipIcon className="h-5 w-5" />
                </div>
                <h3 className="font-serif text-xl font-normal text-ink-black">3. Linearizable Engine</h3>
                <p className="text-xs text-slate-gray mt-2 leading-relaxed">
                  State machines apply committed logs sequentially. Supports atomic Compare-and-Swap (CAS), atomic counters, and key lifecycle management.
                </p>
              </div>
              <div className="pt-3 border-t border-[#ececec] text-xs font-mono text-slate-gray font-medium">
                Strict Order Consistency
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* ─── Benchmarks & Metrics Section ──────────────────── */}
      <section id="benchmarks" className="py-24 px-6 sm:px-8 bg-mist-gray border-t border-[#ececec]">
        <div className="max-w-[1100px] mx-auto">
          <div className="text-center max-w-xl mx-auto mb-16 space-y-3">
            <h2 className="font-serif text-3xl sm:text-4xl text-ink-black font-normal tracking-tight">
              Benchmarked for Extreme Saturation
            </h2>
            <p className="text-sm text-slate-gray">
              Zero allocation critical paths, pipelined network buffers, and predictable tail latencies.
            </p>
          </div>

          <div className="grid grid-cols-2 lg:grid-cols-4 gap-6">
            <div className="bg-paper-white p-6 rounded-cards border border-[#e5e5e7] shadow-subtle text-center">
              <div className="text-3xl sm:text-4xl font-serif text-ink-black font-normal">
                0.42 ms
              </div>
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray mt-2">
                P99 Latency
              </div>
              <p className="text-[11px] text-slate-gray mt-1">Read & write under sustained load</p>
            </div>

            <div className="bg-paper-white p-6 rounded-cards border border-[#e5e5e7] shadow-subtle text-center">
              <div className="text-3xl sm:text-4xl font-serif text-ink-black font-normal">
                100k+
              </div>
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray mt-2">
                Ops / Sec
              </div>
              <p className="text-[11px] text-slate-gray mt-1">Single commodity node throughput</p>
            </div>

            <div className="bg-paper-white p-6 rounded-cards border border-[#e5e5e7] shadow-subtle text-center">
              <div className="text-3xl sm:text-4xl font-serif text-ink-black font-normal">
                &lt; 150 ms
              </div>
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray mt-2">
                Leader Failover
              </div>
              <p className="text-[11px] text-slate-gray mt-1">Automated leader election</p>
            </div>

            <div className="bg-paper-white p-6 rounded-cards border border-[#e5e5e7] shadow-subtle text-center">
              <div className="text-3xl sm:text-4xl font-serif text-ink-black font-normal text-[#1a7f37]">
                99.999%
              </div>
              <div className="text-xs font-semibold uppercase tracking-wider text-slate-gray mt-2">
                Availability SLA
              </div>
              <p className="text-[11px] text-slate-gray mt-1">Multi-AZ quorum deployment</p>
            </div>
          </div>
        </div>
      </section>

      {/* ─── Comparison Table ──────────────────────────────── */}
      <section className="py-24 px-6 sm:px-8 border-t border-[#ececec]">
        <div className="max-w-[1000px] mx-auto">
          <div className="text-center max-w-xl mx-auto mb-16 space-y-3">
            <h2 className="font-serif text-3xl sm:text-4xl text-ink-black font-normal tracking-tight">
              Engineered Differently
            </h2>
            <p className="text-sm text-slate-gray">
              How DistriKV compares to traditional caching and managed cloud key-value stores.
            </p>
          </div>

          <div className="bg-paper-white border border-[#e5e5e7] rounded-cards overflow-hidden shadow-subtle">
            <table className="w-full text-left text-sm border-collapse">
              <thead>
                <tr className="border-b border-[#ececec] bg-fog-white/60 text-xs uppercase tracking-wider text-slate-gray">
                  <th className="py-4 px-6 font-medium">Feature</th>
                  <th className="py-4 px-6 font-semibold text-ink-black bg-mist-gray/60">DistriKV</th>
                  <th className="py-4 px-6 font-medium text-slate-gray">Redis Cluster</th>
                  <th className="py-4 px-6 font-medium text-slate-gray">Amazon DynamoDB</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[#ececec] text-xs">
                <tr>
                  <td className="py-4 px-6 font-medium text-ink-black">Consensus Protocol</td>
                  <td className="py-4 px-6 font-semibold text-[#1a7f37] bg-mist-gray/30">Raft Quorum (Strict)</td>
                  <td className="py-4 px-6 text-slate-gray">Asynchronous Replication</td>
                  <td className="py-4 px-6 text-slate-gray">Paxos Quorum</td>
                </tr>
                <tr>
                  <td className="py-4 px-6 font-medium text-ink-black">Multi-Tenant Hardware Limits</td>
                  <td className="py-4 px-6 font-semibold text-[#1a7f37] bg-mist-gray/30">Token-Bucket per Tenant</td>
                  <td className="py-4 px-6 text-slate-gray">Shared (Noisy Neighbors)</td>
                  <td className="py-4 px-6 text-slate-gray">Partition Throttling</td>
                </tr>
                <tr>
                  <td className="py-4 px-6 font-medium text-ink-black">P99 Tail Latency</td>
                  <td className="py-4 px-6 font-semibold text-ink-black bg-mist-gray/30">&lt; 0.5 ms</td>
                  <td className="py-4 px-6 text-slate-gray">&lt; 1 ms</td>
                  <td className="py-4 px-6 text-slate-gray">4 - 10 ms</td>
                </tr>
                <tr>
                  <td className="py-4 px-6 font-medium text-ink-black">Atomic CAS Primitives</td>
                  <td className="py-4 px-6 font-semibold text-[#1a7f37] bg-mist-gray/30">Native First-Class (POST /kv)</td>
                  <td className="py-4 px-6 text-slate-gray">Lua Script / WATCH</td>
                  <td className="py-4 px-6 text-slate-gray">Conditional Writes</td>
                </tr>
                <tr>
                  <td className="py-4 px-6 font-medium text-ink-black">Deployment Overhead</td>
                  <td className="py-4 px-6 font-semibold text-ink-black bg-mist-gray/30">Single Static Binary in Go</td>
                  <td className="py-4 px-6 text-slate-gray">Multiple Daemons + Sentinel</td>
                  <td className="py-4 px-6 text-slate-gray">Cloud Proprietary Lock-in</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </section>

      {/* ─── Frequently Asked Questions ───────────────────── */}
      <section className="py-24 px-6 sm:px-8 bg-mist-gray border-t border-[#ececec]">
        <div className="max-w-[800px] mx-auto space-y-12">
          <div className="text-center space-y-3">
            <h2 className="font-serif text-3xl sm:text-4xl text-ink-black font-normal tracking-tight">
              Frequently Asked Questions
            </h2>
            <p className="text-sm text-slate-gray">Everything you need to know about integrating DistriKV.</p>
          </div>

          <div className="space-y-4">
            {faqs.map((faq, i) => (
              <div
                key={i}
                className="bg-paper-white border border-[#e4e4e6] rounded-cards p-6 shadow-subtle cursor-pointer transition-all"
                onClick={() => setOpenFaq(openFaq === i ? null : i)}
              >
                <div className="flex items-center justify-between font-serif text-lg text-ink-black">
                  <span>{faq.q}</span>
                  <ChevronDownIcon
                    className={`h-5 w-5 text-slate-gray transition-transform duration-200 ${
                      openFaq === i ? 'rotate-180 text-ink-black' : ''
                    }`}
                  />
                </div>
                {openFaq === i && (
                  <p className="text-xs sm:text-sm text-slate-gray mt-3 leading-relaxed pt-2 border-t border-[#ececec]">
                    {faq.a}
                  </p>
                )}
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ─── Bottom CTA ───────────────────────────────────── */}
      <section className="py-24 px-6 sm:px-8 border-t border-[#ececec] text-center bg-paper-white">
        <div className="max-w-2xl mx-auto space-y-6">
          <h2 className="font-serif text-3xl sm:text-5xl text-ink-black font-normal tracking-tight">
            Ready for predictable distributed performance?
          </h2>
          <p className="text-slate-gray text-base sm:text-lg">
            Create your hardware tenant in seconds. No credit card required.
          </p>
          <div className="flex flex-wrap items-center justify-center gap-4 pt-2">
            <Link to="/signup">
              <button className="text-base font-medium text-paper-white bg-ink-black hover:bg-ink-black/85 transition-all px-8 py-3.5 rounded-full shadow-md hover:shadow-lg">
                Create Tenant Account →
              </button>
            </Link>
            <Link to="/docs">
              <button className="text-base font-medium text-slate-gray hover:text-ink-black transition-colors px-6 py-3.5 rounded-full hover:bg-mist-gray">
                Read the Documentation
              </button>
            </Link>
          </div>
        </div>
      </section>

      {/* ─── Footer ────────────────────────────────────────── */}
      <footer className="border-t border-[#ececec] py-12 px-6 sm:px-8 bg-mist-gray/40 text-xs text-slate-gray">
        <div className="max-w-[1240px] mx-auto flex flex-col sm:flex-row items-center justify-between gap-4">
          <div className="flex items-center gap-2 font-serif text-lg text-ink-black">
            <span>Distri<span className="italic text-sienna-brown">KV</span></span>
            <span className="text-[11px] font-sans text-slate-gray">Systems Inc.</span>
          </div>
          <div>
            &copy; {new Date().getFullYear()} DistriKV Systems Inc. All rights reserved. Open-source distributed architecture.
          </div>
          <div className="flex items-center gap-6">
            <Link to="/docs" className="hover:text-ink-black transition-colors">Docs</Link>
            <a href="https://github.com" target="_blank" rel="noreferrer" className="hover:text-ink-black transition-colors">GitHub</a>
            <Link to="/login" className="hover:text-ink-black transition-colors">Sign in</Link>
          </div>
        </div>
      </footer>
    </div>
  )
}
