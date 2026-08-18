import { useMemo, useState, useEffect, useRef } from 'react'
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  Cell,
  XAxis,
  YAxis,
  ReferenceLine,
  Tooltip,
} from 'recharts'
import { toPng } from 'html-to-image'
import { 
  Trophy, 
  Flame, 
  AlertTriangle, 
  TrendingUp, 
  Download, 
  FileSpreadsheet, 
  ExternalLink, 
  X, 
  Search, 
  Calendar, 
  User, 
  Activity,
  Zap,
  Crosshair,
  Shield
} from 'lucide-react'
import { RankBadge } from './RankBadge'
import { SpotlightCard } from './components/SpotlightCard'
import { ShinyText } from './components/ShinyText'
import './PostMatchReport.css'

// Overwolf agent codenames → display names
const AGENT_NAMES = {
  Clay: 'Raze', Pandemic: 'Viper', Wraith: 'Omen', Hunter: 'Sova', Thorne: 'Sage',
  Phoenix: 'Phoenix', Wushu: 'Jett', Gumshoe: 'Cypher', Sarge: 'Brimstone',
  Breach: 'Breach', Vampire: 'Reyna', Killjoy: 'Killjoy', Guide: 'Skye',
  Stealth: 'Yoru', Rift: 'Astra', Grenadier: 'KAY/O', Deadeye: 'Chamber',
  Sprinter: 'Neon', BountyHunter: 'Fade', Mage: 'Harbor', AggroBot: 'Gekko',
  Cable: 'Deadlock', Sequoia: 'Iso', Smonk: 'Clove', Nox: 'Vyse',
  Cashew: 'Tejo', Terra: 'Waylay',
}

const AGENT_CODE_ORDER = Object.keys(AGENT_NAMES).sort((a, b) => b.length - a.length)

function resolveAgentName(raw) {
  if (!raw) return raw
  const s = String(raw)
  for (const code of AGENT_CODE_ORDER) {
    if (s.toLowerCase().includes(code.toLowerCase())) return AGENT_NAMES[code]
  }
  return s.replace(/_PC_C$|_PostDeath$/i, '')
}

// Overwolf map codenames → display names
const MAP_NAMES = {
  Infinity: 'Abyss',
  Triad: 'Haven',
  Duality: 'Bind',
  Bonsai: 'Split',
  Ascent: 'Ascent',
  Port: 'Icebox',
  Foxtrot: 'Breeze',
  Canyon: 'Fracture',
  Pitt: 'Pearl',
  Jam: 'Lotus',
  Juliett: 'Sunset',
  Rook: 'Corrode',
  Range: 'Practice Range',
  HURM_Alley: 'District',
  HURM_Yard: 'Piazza',
  HURM_Bowl: 'Kasbah',
  HURM_Helix: 'Drift',
  HURM_HighTide: 'Glitch',
}

const MAP_CODE_ORDER = Object.keys(MAP_NAMES).sort((a, b) => b.length - a.length)

function resolveMapName(raw) {
  if (!raw) return raw
  const s = String(raw)
  for (const code of MAP_CODE_ORDER) {
    if (s.toLowerCase() === code.toLowerCase()) return MAP_NAMES[code]
  }
  return s
}

// Custom Tooltip for Recharts Win Probability Bar Chart
function CustomChartTooltip({ active, payload }) {
  if (!active || !payload || !payload.length) return null
  const data = payload[0].payload
  const isWon = data.won
  const isClutch = data.performance === 'clutch'
  const isChoke = data.performance === 'choke'

  return (
    <div className="pmr-chart-tooltip">
      <div className="tooltip-header">
        <span className="tooltip-round">ROUND {data.roundNum}</span>
        <span className={`tooltip-outcome ${isWon ? 'outcome-won' : 'outcome-lost'}`}>
          {isWon ? 'WON' : 'LOST'}
        </span>
      </div>
      <div className="tooltip-body">
        <div className="tooltip-row">
          <span className="tooltip-label">Allies Odds:</span>
          <span className="tooltip-val" style={{ color: isWon ? 'var(--enemy)' : 'var(--ally)' }}>
            {data.prob}%
          </span>
        </div>
        <div className="tooltip-row">
          <span className="tooltip-label">Side & Buy:</span>
          <span className="tooltip-val uppercase">
            {data.side === 'attack' ? 'ATK' : 'DEF'} • {data.buyType.replace('_', ' ')}
          </span>
        </div>
        <div className="tooltip-row">
          <span className="tooltip-label">Local K/D:</span>
          <span className="tooltip-val">{data.kills}K / {data.deaths}D</span>
        </div>
        {isClutch && (
          <div className="tooltip-badge badge-clutch">
            🔥 Clutch! Won against the odds
          </div>
        )}
        {isChoke && (
          <div className="tooltip-badge badge-choke">
            ⚠️ Choked — lost a favored round
          </div>
        )}
      </div>
    </div>
  )
}

function fmtSwing(s) {
  const n = Number(s)
  if (isNaN(n)) return '—'
  return `${n >= 0 ? '+' : ''}${n}%`
}

// High-performance SVG sparkline
function Sparkline({ series, won, width = 100, height = 36, stretch = true, markers = false, labels = false }) {
  const color = won ? 'var(--enemy)' : 'var(--ally)'
  const denom = Math.max(1, series.length - 1)
  const pts = series.map((v, i) => {
    const c = Math.min(100, Math.max(0, v))
    const x = (i / denom) * width
    const y = height - (c / 100) * height
    return { x, y }
  })
  const line = pts.map(p => `${p.x},${p.y}`).join(' ')
  const area = `0,${height} ${line} ${width},${height}`
  const svgProps = stretch
    ? { width: '100%', height, viewBox: `0 0 ${width} ${height}`, preserveAspectRatio: 'none' }
    : { width, height, viewBox: `0 0 ${width} ${height}` }

  return (
    <svg className="pmr-spark-svg" {...svgProps}>
      {!stretch && (
        <line x1="0" y1={height / 2} x2={width} y2={height / 2} stroke="rgba(255,255,255,0.15)" strokeDasharray="3 3" strokeWidth="1" />
      )}
      <polygon points={area} fill={color} opacity="0.1" />
      <polyline
        points={line}
        fill="none"
        stroke={color}
        strokeWidth={stretch ? 1.8 : 2}
        vectorEffect={stretch ? undefined : 'non-scaling-stroke'}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      {pts.map((p, i) => {
        if (!(markers || i === pts.length - 1)) return null
        return <circle key={i} cx={p.x} cy={p.y} r={markers ? 3 : 2} fill={color} />
      })}
      {labels &&
        series.map((v, i) => (
          <text key={`t${i}`} x={pts[i].x} y={Math.max(10, pts[i].y - 6)} fontSize="9" fill="var(--text-3)" textAnchor="middle" fontFamily="var(--font-display)" fontWeight="700">
            {Math.round(v)}%
          </text>
        ))}
    </svg>
  )
}

function sanitizeFilename(s) {
  return String(s || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
}

function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  setTimeout(() => URL.revokeObjectURL(url), 4000)
}

function buildRoundCsv(rounds) {
  const esc = (v) => {
    const s = String(v == null ? '' : v)
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s
  }
  const header = ['RND', 'SIDE', 'BUY TYPE', 'BUY ADV', 'PRE-ROUND WIN %', 'K / D', 'RESULT', 'SWING', 'KILLS']
  const lines = [header.map(esc).join(',')]

  ;(rounds || []).forEach((r) => {
    const evalStr = r.buy_eval
      ? String(r.buy_eval).toUpperCase()
      : String(r.buy_recommendation || '').toLowerCase() === String(r.buy_type || '').toLowerCase()
        ? 'MATCH'
        : 'DIFF'
    let k = r.player_kills ?? r.kills_by_local ?? r.kills
    let d = r.player_deaths ?? r.deaths_by_local ?? r.deaths
    k = Array.isArray(k) ? k.length : (Number(k) || 0)
    d = Array.isArray(d) ? d.length : (Number(d) || 0)
    const swing = Number(r.prob_swing)
    lines.push([
      r.round_number || r.round,
      String(r.side || '').toUpperCase(),
      String(r.buy_type || 'full_buy').replace('_', ' ').toUpperCase(),
      evalStr,
      r.pre_prob,
      `${k}/${d}`,
      r.won ? 'WIN' : 'LOSS',
      isNaN(swing) ? '' : `${swing >= 0 ? '+' : ''}${swing}%`,
      Array.isArray(r.kills) ? r.kills.length : 0,
    ].map(esc).join(','))
  })

  return '\uFEFF' + lines.join('\n')
}

export function PostMatchReport({ report, reportUrl, reportFile, onOpenReport }) {
  const [activeRound, setActiveRound] = useState(null)
  const [tableFilter, setTableFilter] = useState('all')
  const [searchQuery, setSearchQuery] = useState('')
  const [busy, setBusy] = useState('')
  const containerRef = useRef(null)
  const modalRef = useRef(null)

  useEffect(() => {
    if (!activeRound) return undefined
    const onKey = (e) => {
      if (e.key === 'Escape') setActiveRound(null)
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [activeRound])

  const {
    map = 'VALORANT',
    outcome = 'defeat',
    final_score = [0, 0],
    rounds = [],
    pivotal_rounds = [],
    economy = {},
    date = '',
    local_agent = '',
    model_accuracy = null,
    max_streak = 0,
    biggest_upset = null,
    team_comp = {},
  } = report || {}

  const isVictory = outcome.toLowerCase() === 'victory'
  const displayAgent = resolveAgentName(local_agent)
  const displayMap = resolveMapName(map)
  const totalRounds = rounds.length
  const wonCount = rounds.filter(r => r.won).length
  const lostCount = totalRounds - wonCount

  // Player stats
  const playerStats = useMemo(() => {
    if (!rounds.length) return { clutchCount: 0, chokeCount: 0, favoredWins: 0, favoredTotal: 0, underdogWins: 0, underdogTotal: 0 }
    let clutchCount = 0, chokeCount = 0
    let favoredWins = 0, favoredTotal = 0
    let underdogWins = 0, underdogTotal = 0
    rounds.forEach(r => {
      if (r.performance === 'clutch') clutchCount++
      if (r.performance === 'choke') chokeCount++
      if (r.pre_prob >= 50) {
        favoredTotal++
        if (r.won) favoredWins++
      } else {
        underdogTotal++
        if (r.won) underdogWins++
      }
    })
    const favoredPct = favoredTotal > 0 ? Math.round((favoredWins / favoredTotal) * 100) : 0
    const underdogPct = underdogTotal > 0 ? Math.round((underdogWins / underdogTotal) * 100) : 0
    return { clutchCount, chokeCount, favoredWins, favoredTotal, favoredPct, underdogWins, underdogTotal, underdogPct }
  }, [rounds])

  // Chart data
  const chartData = useMemo(() => {
    return rounds.map(r => {
      let k = r.player_kills ?? r.kills_by_local ?? r.kills
      let d = r.player_deaths ?? r.deaths_by_local ?? r.deaths
      k = Array.isArray(k) ? k.length : (Number(k) || 0)
      d = Array.isArray(d) ? d.length : (Number(d) || 0)

      return {
        name: `R${r.round_number || r.round}`,
        roundNum: r.round_number || r.round,
        prob: Math.round(r.pre_prob * 10) / 10,
        won: r.won,
        performance: r.performance || 'expected',
        side: r.side,
        buyType: r.buy_type || 'full_buy',
        kills: k,
        deaths: d,
      }
    })
  }, [rounds])

  // Timeline data
  const timelineData = useMemo(() => {
    if (!rounds.length) return []
    return rounds.map(r => {
      const pre = Number(r.pre_prob)
      const safePre = isNaN(pre) ? 50 : pre
      const kills = Array.isArray(r.kills) ? r.kills : []
      const series = [safePre]
      kills.forEach(k => {
        const p = Number(k.live_prob)
        if (!isNaN(p)) series.push(p)
      })
      const fp = Number(r.final_prob)
      if (!isNaN(fp) && series[series.length - 1] !== fp) series.push(fp)
      if (series.length < 2) series.push(safePre)
      return {
        round: r.round_number || r.round,
        won: !!r.won,
        performance: r.performance || 'expected',
        buyType: r.buy_type || 'full_buy',
        side: r.side,
        preProb: Math.round(safePre * 10) / 10,
        finalProb: fp,
        swing: r.prob_swing,
        kills,
        series,
      }
    })
  }, [rounds])

  // Filtered rounds for table
  const filteredRounds = useMemo(() => {
    return rounds.filter(r => {
      if (tableFilter === 'won' && !r.won) return false
      if (tableFilter === 'lost' && r.won) return false
      if (tableFilter === 'clutch' && r.performance !== 'clutch') return false
      if (tableFilter === 'choke' && r.performance !== 'choke') return false

      if (searchQuery.trim()) {
        const q = searchQuery.toLowerCase()
        const matchBuy = String(r.buy_type || '').toLowerCase().includes(q)
        const matchSide = String(r.side || '').toLowerCase().includes(q)
        const matchRound = `r${r.round_number || r.round}`.includes(q)
        return matchBuy || matchSide || matchRound
      }
      return true
    })
  }, [rounds, tableFilter, searchQuery])

  const baseName = `valo_report_${sanitizeFilename(map) || 'valorant'}_${sanitizeFilename(outcome) || 'match'}${
    date ? `_${String(date).replace(/-/g, '')}` : ''
  }`

  const exportPng = async (node, filename) => {
    if (busy || !node) return
    setBusy(filename)
    try {
      const dataUrl = await toPng(node, { pixelRatio: 2, backgroundColor: '#080b11', cacheBust: true })
      const blob = await (await fetch(dataUrl)).blob()
      downloadBlob(blob, filename)
    } catch {
      try {
        const dataUrl = await toPng(node, { pixelRatio: 1, backgroundColor: '#080b11', cacheBust: true })
        window.open(dataUrl, '_blank')
      } catch {
        /* fallback */
      }
    } finally {
      setBusy('')
    }
  }

  const exportCsv = () => {
    const blob = new Blob([buildRoundCsv(rounds)], { type: 'text/csv;charset=utf-8;' })
    downloadBlob(blob, `${baseName}.csv`)
  }

  if (!report) return null

  const econTypes = [
    { key: 'pistol',   label: 'PISTOL',    color: '#94a3b8' },
    { key: 'eco',      label: 'ECO',       color: 'var(--success)' },
    { key: 'force',    label: 'FORCE BUY', color: 'var(--gold)' },
    { key: 'half_buy', label: 'HALF BUY',  color: 'var(--warning)' },
    { key: 'full_buy', label: 'FULL BUY',  color: 'var(--enemy)' },
    { key: 'bonus',    label: 'BONUS',     color: 'var(--purple)' },
    { key: 'anti_eco', label: 'ANTI-ECO',  color: 'var(--warning)' },
    { key: 'broken',   label: 'BROKEN',    color: '#e879f9' },
  ]

  return (
    <div className="pmr-container" ref={containerRef}>
      {/* 1. Header Hero Card */}
      <div className={`pmr-card pmr-header-card ${isVictory ? 'theme-victory' : 'theme-defeat'}`}>
        <div className="header-left">
          <div className="result-badge-row">
            <span className={`result-pill ${isVictory ? 'pill-victory' : 'pill-defeat'}`}>
              <ShinyText 
                text={isVictory ? 'VICTORY' : 'DEFEAT'} 
                color={isVictory ? 'var(--enemy)' : 'var(--ally)'} 
                shineColor="#ffffff" 
              />
            </span>
            <span className="mode-pill">COMPETITIVE</span>
          </div>
          <h1 className="map-title">{(displayMap && displayMap.trim()) ? displayMap.toUpperCase() : 'VALORANT'}</h1>
          {(local_agent || date) && (
            <div className="header-sub-row">
              {displayAgent && (
                <span className="header-sub-chip header-sub-agent">
                  <User size={11} /> {displayAgent}
                </span>
              )}
              {date && (
                <span className="header-sub-chip">
                  <Calendar size={11} /> {date}
                </span>
              )}
            </div>
          )}
        </div>

        <div className="header-score-box">
          <span className={`score-digit ${isVictory ? 'score-ally' : 'score-muted'}`}>
            {final_score[0]}
          </span>
          <span className="score-divider">:</span>
          <span className={`score-digit ${!isVictory ? 'score-enemy' : 'score-muted'}`}>
            {final_score[1]}
          </span>
        </div>

        <div className="header-meta">
          <div className="meta-stat">
            <span className="meta-val">{totalRounds}</span>
            <span className="meta-lbl">TOTAL ROUNDS</span>
          </div>
          <div className="meta-divider" />
          <div className="meta-stat">
            <span className="meta-val text-enemy">{wonCount}W</span>
            <span className="meta-lbl">WON</span>
          </div>
          <div className="meta-divider" />
          <div className="meta-stat">
            <span className="meta-val text-ally">{lostCount}L</span>
            <span className="meta-lbl">LOST</span>
          </div>
          {model_accuracy != null && (
            <>
              <div className="meta-divider" />
              <div className="meta-stat">
                <span className="meta-val text-gold">{Math.round(model_accuracy * 100)}%</span>
                <span className="meta-lbl">MODEL A ACC</span>
              </div>
            </>
          )}
          {max_streak > 0 && (
            <>
              <div className="meta-divider" />
              <div className="meta-stat">
                <span className="meta-val">{max_streak}W</span>
                <span className="meta-lbl">MAX STREAK</span>
              </div>
            </>
          )}
        </div>

        <div className="pmr-header-actions">
          {rounds.length > 0 && (
            <button
              type="button"
              className="pmr-header-action-btn pmr-dl-btn"
              onClick={() => exportPng(containerRef.current, `${baseName}.png`)}
              disabled={!!busy}
              title="Download full post-match report as PNG image"
            >
              <Download size={12} /> {busy ? 'Exporting…' : 'PNG Export'}
            </button>
          )}
          {rounds.length > 0 && (
            <button
              type="button"
              className="pmr-header-action-btn pmr-dl-csv-btn"
              onClick={exportCsv}
              disabled={!!busy}
              title="Download round data breakdown as CSV"
            >
              <FileSpreadsheet size={12} /> CSV
            </button>
          )}
          {(reportUrl || reportFile) && onOpenReport && (
            <button
              type="button"
              className="pmr-header-action-btn pmr-open-full-btn"
              onClick={() => onOpenReport(reportUrl, reportFile)}
              title="Open full interactive HTML dashboard in browser"
            >
              <ExternalLink size={12} /> Full Dashboard
            </button>
          )}
        </div>
      </div>

      {/* 2. Win Probability Timeline Bar Chart */}
      <div className="pmr-card pmr-chart-card">
        <div className="card-header">
          <div className="card-title-group">
            <TrendingUp size={16} className="text-enemy" />
            <h2 className="card-title">WIN PROBABILITY BY ROUND</h2>
          </div>
          <div className="chart-legend">
            <span className="legend-item">
              <span className="legend-box box-won" /> Won
            </span>
            <span className="legend-item">
              <span className="legend-box box-lost" /> Lost
            </span>
            <span className="legend-item">
              <span className="legend-marker marker-clutch">🔥</span> Clutch
            </span>
            <span className="legend-item">
              <span className="legend-marker marker-choke">⚠️</span> Choke
            </span>
            <span className="legend-item">
              <span className="legend-line" /> 50% Baseline
            </span>
          </div>
        </div>

        <div className="chart-wrapper">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart
              data={chartData}
              margin={{ top: 20, right: 10, left: -20, bottom: 10 }}
            >
              <XAxis
                dataKey="name"
                stroke="var(--text-4)"
                tick={{ fill: 'var(--text-3)', fontSize: 11, fontWeight: 600, fontFamily: 'var(--font-display)' }}
                axisLine={{ stroke: 'var(--border)' }}
                tickLine={false}
              />
              <YAxis
                domain={[0, 100]}
                ticks={[0, 25, 50, 75, 100]}
                stroke="var(--text-4)"
                tick={{ fill: 'var(--text-4)', fontSize: 10, fontFamily: 'var(--font-mono)' }}
                axisLine={{ stroke: 'var(--border)' }}
                tickLine={false}
                unit="%"
              />
              <ReferenceLine
                y={50}
                stroke="var(--text-4)"
                strokeDasharray="4 4"
                strokeWidth={1}
              />
              <Tooltip content={<CustomChartTooltip />} cursor={{ fill: 'rgba(255, 255, 255, 0.03)' }} />
              <Bar dataKey="prob" radius={[4, 4, 0, 0]}>
                {chartData.map((entry, index) => {
                  const fillColor = entry.won ? 'var(--enemy)' : 'var(--ally)'
                  return <Cell key={`cell-${index}`} fill={fillColor} />
                })}
              </Bar>
            </BarChart>
          </ResponsiveContainer>

          {/* Clutch / Choke floating markers */}
          <div className="chart-markers-row">
            {chartData.map((d, i) => (
              <div key={i} className="chart-marker-cell">
                {d.performance === 'clutch' && (
                  <span className="chart-perf-clutch" title={`R${d.roundNum}: Clutch win (only ${d.prob}% odds)`}>
                    🔥
                  </span>
                )}
                {d.performance === 'choke' && (
                  <span className="chart-perf-choke" title={`R${d.roundNum}: Choked round (${d.prob}% odds)`}>
                    ⚠️
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* 3. Live Probability Timeline (Model B Combat Evolution) */}
      <div className="pmr-card pmr-timeline-card">
        <div className="card-header">
          <div className="card-title-group">
            <Activity size={16} className="text-gold" />
            <h2 className="card-title">LIVE COMBAT PROGRESSION TIMELINE</h2>
          </div>
          <span className="card-subtitle">Model B Combat Evolution • Click round to expand</span>
        </div>

        <div className="pmr-timeline-grid">
          {timelineData.map((t, i) => (
            <button
              key={i}
              type="button"
              className={`pmr-spark-card ${t.won ? 'spark-won' : 'spark-lost'}`}
              onClick={() => setActiveRound(t)}
              title={`R${t.round}: Click to inspect kill-by-kill combat progression`}
            >
              <div className="spark-top">
                <span className="spark-rnd">R{t.round}</span>
                <span className={`spark-res ${t.won ? 'spark-w' : 'spark-l'}`}>{t.won ? 'W' : 'L'}</span>
              </div>
              <Sparkline series={t.series} won={t.won} height={32} />
              <div className="spark-bottom">
                <span className="spark-kills">{t.kills.length} Kills</span>
                <span className={`spark-swing ${t.won ? 'swing-up' : 'swing-down'}`}>{fmtSwing(t.swing)}</span>
              </div>
            </button>
          ))}
        </div>
      </div>

      {/* 4. Match-Swinging Rounds & Economy Matrix */}
      <div className="pmr-two-col-grid">
        {/* Left: Match-Swinging Rounds */}
        <div className="pmr-card pmr-swing-card">
          <div className="card-header">
            <div className="card-title-group">
              <Zap size={16} className="text-gold" />
              <h2 className="card-title">MATCH-SWINGING ROUNDS</h2>
            </div>
            <span className="card-subtitle">Highest Win Probability Swings</span>
          </div>

          <div className="pivotal-list">
            {pivotal_rounds && pivotal_rounds.length > 0 ? (
              pivotal_rounds.map((p, idx) => {
                const isWon = p.won
                return (
                  <SpotlightCard
                    key={idx}
                    className={`pivotal-item ${isWon ? 'piv-item-won' : 'piv-item-lost'}`}
                    spotlightColor={isWon ? 'rgba(0, 229, 204, 0.12)' : 'rgba(255, 70, 85, 0.12)'}
                  >
                    <div className="piv-top">
                      <div className="piv-round-tag">
                        <span className="piv-r-num">ROUND {p.round}</span>
                        <span className={`piv-status-badge ${isWon ? 'badge-won' : 'badge-lost'}`}>
                          {isWon ? 'WON' : 'LOST'}
                        </span>
                      </div>
                      <div className="piv-swing-badge">
                        <span className="piv-swing-val">
                          {isWon ? '+' : '-'}{p.swing}% SWING
                        </span>
                      </div>
                    </div>

                    <div className="piv-reason-text">{p.reason}</div>

                    <div className="piv-bottom-stats">
                      <span className="piv-stat-lbl">Pre-Round Probability:</span>
                      <span className="piv-stat-val">{p.pre_prob}%</span>
                    </div>
                  </SpotlightCard>
                )
              })
            ) : (
              <div className="no-data-msg">No high-swing rounds recorded for this match.</div>
            )}
          </div>
        </div>

        {/* Right: Economy Efficiency 4x2 Matrix */}
        <div className="pmr-card pmr-econ-card">
          <div className="card-header">
            <div className="card-title-group">
              <Shield size={16} className="text-enemy" />
              <h2 className="card-title">ECONOMY EFFICIENCY MATRIX</h2>
            </div>
            <span className="card-subtitle">Win Rate by Buy Category</span>
          </div>

          <div className="econ-2x2-grid">
            {econTypes.map(({ key, label, color }) => {
              const stat = economy[key] || { played: 0, won: 0 }
              const played = stat.played || 0
              const won = stat.won || 0
              const lost = Math.max(0, played - won)
              const winRate = played > 0 ? Math.round((won / played) * 100) : 0

              return (
                <div
                  key={key}
                  className={`econ-tile ${played === 0 ? 'econ-tile-empty' : ''}`}
                  style={{ '--tile-accent': color }}
                >
                  <div className="econ-tile-header">
                    <span className="econ-type-label" style={{ color }}>{label}</span>
                    {played > 0 && (
                      <span className="econ-wr-badge" style={{ background: `${color}18`, color, borderColor: `${color}33` }}>
                        {winRate}% WR
                      </span>
                    )}
                  </div>

                  <div className="econ-tile-number">
                    <span className="big-wr" style={{ color: played > 0 ? color : 'var(--text-4)' }}>
                      {played > 0 ? `${winRate}%` : '—'}
                    </span>
                  </div>

                  <div className="econ-bar-track">
                    <div
                      className="econ-bar-fill"
                      style={{
                        width: `${played > 0 ? winRate : 0}%`,
                        background: color,
                      }}
                    />
                  </div>

                  <div className="econ-tile-record">
                    {played > 0 ? (
                      <span><strong>{won}W</strong> - <strong>{lost}L</strong> <span className="text-muted">({played} played)</span></span>
                    ) : (
                      <span className="text-muted">0 rounds played</span>
                    )}
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      </div>

      {/* 5. Team & Ranks */}
      {(() => {
        const allies = team_comp.allies || []
        const enemies = team_comp.enemies || []
        if (!allies.length && !enemies.length) return null
        return (
          <div className="pmr-card pmr-comp-card">
            <div className="card-header">
              <div className="card-title-group">
                <Trophy size={16} className="text-gold" />
                <h2 className="card-title">TEAM COMPOSITION & RANKS</h2>
              </div>
            </div>
            <div className="pmr-comp-grid">
              <div className="pmr-comp-col">
                <div className="pmr-comp-label comp-ally">YOUR TEAM</div>
                {allies.map((p, i) => (
                  <div className="pmr-comp-row" key={`a${i}`}>
                    <span className="pmr-comp-agent">{p.agent}</span>
                    <span className="pmr-comp-name">{p.name}</span>
                    <span className="pmr-comp-rank"><RankBadge rank={p.rank} size={18} />{p.rank}</span>
                  </div>
                ))}
              </div>
              <div className="pmr-comp-col">
                <div className="pmr-comp-label comp-enemy">ENEMIES</div>
                {enemies.map((p, i) => (
                  <div className="pmr-comp-row" key={`e${i}`}>
                    <span className="pmr-comp-agent">{p.agent}</span>
                    <span className="pmr-comp-name">{p.name}</span>
                    <span className="pmr-comp-rank"><RankBadge rank={p.rank} size={18} />{p.rank}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )
      })()}

      {/* 6. Round Breakdown Table with Filters & Search */}
      <div className="pmr-card pmr-table-card">
        <div className="card-header table-header-flex">
          <div className="card-title-group">
            <Flame size={16} className="text-gold" />
            <h2 className="card-title">ROUND-BY-ROUND BREAKDOWN</h2>
          </div>

          <div className="table-controls">
            {/* Filter Tabs */}
            <div className="table-tabs">
              <button
                type="button"
                className={`table-tab ${tableFilter === 'all' ? 'active' : ''}`}
                onClick={() => setTableFilter('all')}
              >
                ALL ({rounds.length})
              </button>
              <button
                type="button"
                className={`table-tab ${tableFilter === 'won' ? 'active' : ''}`}
                onClick={() => setTableFilter('won')}
              >
                WON ({wonCount})
              </button>
              <button
                type="button"
                className={`table-tab ${tableFilter === 'lost' ? 'active' : ''}`}
                onClick={() => setTableFilter('lost')}
              >
                LOST ({lostCount})
              </button>
              {playerStats.clutchCount > 0 && (
                <button
                  type="button"
                  className={`table-tab ${tableFilter === 'clutch' ? 'active' : ''}`}
                  onClick={() => setTableFilter('clutch')}
                >
                  CLUTCHES ({playerStats.clutchCount})
                </button>
              )}
              {playerStats.chokeCount > 0 && (
                <button
                  type="button"
                  className={`table-tab ${tableFilter === 'choke' ? 'active' : ''}`}
                  onClick={() => setTableFilter('choke')}
                >
                  CHOKES ({playerStats.chokeCount})
                </button>
              )}
            </div>

            {/* Search Box */}
            <div className="table-search">
              <Search size={12} className="search-icon" />
              <input
                type="text"
                placeholder="Search round or buy…"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="search-input"
              />
              {searchQuery && (
                <button type="button" className="search-clear" onClick={() => setSearchQuery('')}>
                  <X size={10} />
                </button>
              )}
            </div>
          </div>
        </div>

        <div className="table-responsive-wrap">
          <table className="pmr-breakdown-table">
            <thead>
              <tr>
                <th className="th-rnd">RND</th>
                <th className="th-side">SIDE</th>
                <th className="th-buy">BUY TYPE</th>
                <th className="th-eval">BUY ADV</th>
                <th className="th-prob">PRE-ROUND ODDS</th>
                <th className="th-kd">LOCAL K/D</th>
                <th className="th-dmg">DMG</th>
                <th className="th-res">RESULT</th>
                <th className="th-swing">SWING</th>
              </tr>
            </thead>
            <tbody>
              {filteredRounds.map((r, i) => {
                const isWon = r.won
                const isClutch = r.performance === 'clutch'
                const isChoke = r.performance === 'choke'
                const buyLabel = (r.buy_type || 'full_buy').replace('_', ' ').toUpperCase()
                const isAtk = r.side === 'attack'
                const rec = String(r.buy_recommendation || '').toLowerCase()
                const act = String(r.buy_type || 'full_buy').toLowerCase()
                const isMatch = r.buy_eval
                  ? String(r.buy_eval).toUpperCase() === 'MATCH'
                  : (rec && rec === act)
                const swingVal = Number(r.prob_swing)
                
                let k = r.player_kills ?? r.kills_by_local ?? r.kills
                let d = r.player_deaths ?? r.deaths_by_local ?? r.deaths
                k = Array.isArray(k) ? k.length : (Number(k) || 0)
                d = Array.isArray(d) ? d.length : (Number(d) || 0)

                return (
                  <tr
                    key={i}
                    className={`table-row ${isWon ? 'row-won' : 'row-lost'} ${
                      isClutch ? 'row-clutch' : isChoke ? 'row-choke' : ''
                    }`}
                    onClick={() => {
                      const t = timelineData.find(item => item.round === (r.round_number || r.round))
                      if (t) setActiveRound(t)
                    }}
                    title="Click to view kill-by-kill progression"
                  >
                    <td className="td-rnd">
                      <span className="rnd-badge">R{r.round_number || r.round}</span>
                    </td>
                    <td className="td-side">
                      <span className={`side-badge ${isAtk ? 'side-atk' : 'side-def'}`}>
                        {isAtk ? 'ATK' : 'DEF'}
                      </span>
                    </td>
                    <td className="td-buy">
                      <span className="buy-badge">
                        {buyLabel}
                      </span>
                    </td>
                    <td className="td-eval">
                      <span className={`buyev-badge ${isMatch ? 'ev-match' : 'ev-diff'}`}>
                        {isMatch ? 'MATCH' : 'DIFF'}
                      </span>
                    </td>
                    <td className="td-prob">
                      <div className="prob-cell">
                        <div className="prob-meter-track">
                          <div
                            className={`prob-meter-fill ${isWon ? 'fill-enemy' : 'fill-ally'}`}
                            style={{ width: `${Math.min(100, Math.max(0, r.pre_prob))}%` }}
                          />
                        </div>
                        <span className="prob-cell-text">{r.pre_prob}%</span>
                      </div>
                    </td>
                    <td className="td-kd">
                      <span className="kd-text">
                        <strong>{k}</strong> / <span className="text-muted">{d}</span>
                      </span>
                    </td>
                    <td className="td-dmg">
                      <span className="dmg-text">
                        {r.round_report ? Math.round(Number(r.round_report.damage) || 0) : '—'}
                      </span>
                    </td>
                    <td className="td-res">
                      <div className="res-cell">
                        <span className={`res-badge ${isWon ? 'badge-w' : 'badge-l'}`}>
                          {isWon ? 'WIN' : 'LOSS'}
                        </span>
                        {isClutch && (
                          <span className="perf-tag tag-clutch">
                            🔥 CLUTCH
                          </span>
                        )}
                        {isChoke && (
                          <span className="perf-tag tag-choke">
                            ⚠️ CHOKE
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="td-swing">
                      <span className={`swing-val ${!isNaN(swingVal) && swingVal >= 0 ? 'text-enemy' : 'text-ally'}`}>
                        {fmtSwing(r.prob_swing)}
                      </span>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Combat Progression Modal */}
      {activeRound && (
        <div className="pmr-modal-backdrop" onClick={() => setActiveRound(null)}>
          <div className="pmr-modal" ref={modalRef} onClick={(e) => e.stopPropagation()}>
            <div className="pmr-modal-header">
              <div className="pmr-modal-title">
                <span className="pmr-modal-rnd">ROUND {activeRound.round} DRILLDOWN</span>
                <span className={`res-badge ${activeRound.won ? 'badge-w' : 'badge-l'}`}>
                  {activeRound.won ? 'WIN' : 'LOSS'}
                </span>
                {activeRound.performance === 'clutch' && <span className="perf-tag tag-clutch">🔥 CLUTCH</span>}
                {activeRound.performance === 'choke' && <span className="perf-tag tag-choke">⚠️ CHOKE</span>}
              </div>
              <button
                type="button"
                className="pmr-modal-export"
                onClick={() => exportPng(modalRef.current, `${baseName}_r${activeRound.round}.png`)}
                disabled={!!busy}
                title="Download this round's combat chart as PNG"
              >
                <Download size={11} /> {busy ? '…' : 'PNG'}
              </button>
              <button type="button" className="pmr-modal-close" onClick={() => setActiveRound(null)} aria-label="Close modal">
                <X size={14} />
              </button>
            </div>

            <div className="pmr-modal-meta">
              <span className={`side-badge ${activeRound.side === 'attack' ? 'side-atk' : 'side-def'}`}>
                {String(activeRound.side || '').toUpperCase().slice(0, 3)}
              </span>
              <span className="buy-badge">{String(activeRound.buyType).replace('_', ' ').toUpperCase()}</span>
              <span className="pmr-modal-stat">PRE {activeRound.preProb}%</span>
              <span className="pmr-modal-stat">FINAL {!isNaN(activeRound.finalProb) ? Math.round(activeRound.finalProb) : '—'}%</span>
              <span className={`pmr-modal-stat ${activeRound.won ? 'text-enemy' : 'text-ally'}`}>
                SWING {fmtSwing(activeRound.swing)}
              </span>
            </div>

            <div className="pmr-modal-chart">
              <Sparkline series={activeRound.series} won={activeRound.won} width={460} height={130} stretch={false} markers labels />
            </div>

            <div className="pmr-kill-list">
              {activeRound.kills.length === 0 && (
                <div className="no-data-msg">No combat kill events recorded for this round.</div>
              )}
              {activeRound.kills.map((k, i) => {
                const prev = i === 0 ? activeRound.preProb : Number(activeRound.kills[i - 1].live_prob)
                const live = Number(k.live_prob)
                const delta = Math.round((live - prev) * 10) / 10
                const allyKill = !!k.is_attacker_teammate
                return (
                  <div key={i} className={`pmr-kill-row ${allyKill ? 'kill-ally' : 'kill-enemy'}`}>
                    <span className="kill-idx">#{i + 1}</span>
                    <span className="kill-event">
                      <span className="kill-icon">{allyKill ? '🗡️' : '💀'}</span>
                      <strong>{k.attacker}</strong> <span className="kill-arrow">→</span> {k.victim}
                      {k.headshot && <span className="kill-hs"> (HS)</span>}
                    </span>
                    <span className="kill-alive">{k.att_alive}v{k.def_alive}</span>
                    <span className="kill-prob">
                      {live}% <span className={`kill-delta ${delta >= 0 ? 'text-enemy' : 'text-ally'}`}>{delta >= 0 ? '+' : ''}{delta}%</span>
                    </span>
                  </div>
                )
              })}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
