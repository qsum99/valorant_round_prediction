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
import { RankBadge } from './RankBadge'
import './PostMatchReport.css'

// Overwolf agent codenames → display names (fallback if backend sends raw codes)
const AGENT_NAMES = {
  Clay: 'Raze', Pandemic: 'Viper', Wraith: 'Omen', Hunter: 'Sova', Thorne: 'Sage',
  Phoenix: 'Phoenix', Wushu: 'Jett', Gumshoe: 'Cypher', Sarge: 'Brimstone',
  Breach: 'Breach', Vampire: 'Reyna', Killjoy: 'Killjoy', Guide: 'Skye',
  Stealth: 'Yoru', Rift: 'Astra', Grenadier: 'KAY/O', Deadeye: 'Chamber',
  Sprinter: 'Neon', BountyHunter: 'Fade', Mage: 'Harbor', AggroBot: 'Gekko',
  Cable: 'Deadlock', Sequoia: 'Iso', Smonk: 'Clove', Nox: 'Vyse',
  Cashew: 'Tejo', Terra: 'Waylay',
}

// Longest-first so "BountyHunter" never matches "Hunter"
const AGENT_CODE_ORDER = Object.keys(AGENT_NAMES).sort((a, b) => b.length - a.length)

function resolveAgentName(raw) {
  if (!raw) return raw
  const s = String(raw)
  for (const code of AGENT_CODE_ORDER) {
    if (s.toLowerCase().includes(code.toLowerCase())) return AGENT_NAMES[code]
  }
  return s.replace(/_PC_C$|_PostDeath$/i, '')
}

// Overwolf map codenames → display names (fallback if backend sends raw codes)
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
          <span className="tooltip-label">Your Win Odds:</span>
          <span className="tooltip-val" style={{ color: isWon ? '#00e5cc' : '#ff4655' }}>
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
          <span className="tooltip-label">Round K/D:</span>
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

// String formatting helpers
function fmtSwing(s) {
  const n = Number(s)
  if (isNaN(n)) return '—'
  return `${n >= 0 ? '+' : ''}${n}%`
}

// Zero-dependency SVG sparkline of a round's live win-probability curve
function Sparkline({ series, won, width = 100, height = 40, stretch = true, markers = false, labels = false }) {
  const color = won ? '#00e5cc' : '#ff4655'
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
        <line x1="0" y1={height / 2} x2={width} y2={height / 2} stroke="#334155" strokeDasharray="4 4" strokeWidth="1" />
      )}
      <polygon points={area} fill={color} opacity="0.12" />
      <polyline
        points={line}
        fill="none"
        stroke={color}
        strokeWidth={stretch ? 1.6 : 1.8}
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
          <text key={`t${i}`} x={pts[i].x} y={Math.max(10, pts[i].y - 7)} fontSize="9" fill="#94a3b8" textAnchor="middle">
            {Math.round(v)}
          </text>
        ))}
    </svg>
  )
}

// ── Download / export helpers ────────────────────────────────────────────────
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

  // Player performance stats: clutch/choke counts + favored/underdog win rates
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

  // Chart data formatting for Recharts
  const chartData = useMemo(() => {
    return rounds.map(r => {
      // Safely resolve kills/deaths to a number to prevent React crash if backend sends raw arrays
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

  // Live probability curve series per round (Model B combat evolution)
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

  // ── Download handlers ──────────────────────────────────────────────────────
  const baseName = `valo_report_${sanitizeFilename(map) || 'valorant'}_${sanitizeFilename(outcome) || 'match'}${
    date ? `_${String(date).replace(/-/g, '')}` : ''
  }`

  const exportPng = async (node, filename) => {
    if (busy || !node) return
    setBusy(filename)
    try {
      const dataUrl = await toPng(node, { pixelRatio: 2, backgroundColor: '#0f1923', cacheBust: true })
      const blob = await (await fetch(dataUrl)).blob()
      downloadBlob(blob, filename)
    } catch {
      try {
        const dataUrl = await toPng(node, { pixelRatio: 1, backgroundColor: '#0f1923', cacheBust: true })
        window.open(dataUrl, '_blank')
      } catch {
        /* give up silently */
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

  // Economy types list with accent colors matching BuyAdvisor palette
  const econTypes = [
    { key: 'pistol',   label: 'PISTOL',    color: '#94a3b8', icon: '🔫' },
    { key: 'eco',      label: 'ECO',       color: '#22c55e', icon: '💰' },
    { key: 'force',    label: 'FORCE BUY', color: '#e8c468', icon: '⚔️' },
    { key: 'half_buy', label: 'HALF BUY',  color: '#fb923c', icon: '🥋' },
    { key: 'full_buy', label: 'FULL BUY',  color: '#4fc3f7', icon: '🛡️' },
    { key: 'bonus',    label: 'BONUS',     color: '#a78bfa', icon: '💸' },
    { key: 'anti_eco', label: 'ANTI-ECO',  color: '#fb923c', icon: '⚡' },
    { key: 'broken',   label: 'BROKEN',    color: '#e879f9', icon: '🧩' },
  ]

  return (
    <div className="pmr-container" ref={containerRef}>
      {/* 1. Header Card */}
      <div className={`pmr-card pmr-header-card ${isVictory ? 'theme-victory' : 'theme-defeat'}`}>
        <div className="header-glow" />
        <div className="header-left">
          <div className="result-badge-row">
            <span className={`result-pill ${isVictory ? 'pill-victory' : 'pill-defeat'}`}>
              {isVictory ? 'VICTORY' : 'DEFEAT'}
            </span>
            <span className="mode-pill">COMPETITIVE</span>
          </div>
          <div className="map-title">{(displayMap && displayMap.trim()) ? displayMap.toUpperCase() : 'VALORANT'}</div>
          {(local_agent || date) && (
            <div className="header-sub-row">
              {displayAgent && <span className="header-sub-chip header-sub-agent">🎮 {displayAgent}</span>}
              {date && <span className="header-sub-chip">📅 {date}</span>}
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
            <span className="meta-val text-ally">{wonCount}W</span>
            <span className="meta-lbl">WON</span>
          </div>
          <div className="meta-divider" />
          <div className="meta-stat">
            <span className="meta-val text-enemy">{lostCount}L</span>
            <span className="meta-lbl">LOST</span>
          </div>
          {model_accuracy != null && (
            <>
              <div className="meta-divider" />
              <div className="meta-stat">
                <span className="meta-val text-ally">{Math.round(model_accuracy * 100)}%</span>
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
          {biggest_upset && (
            <>
              <div className="meta-divider" />
              <div className="meta-stat"
                title={`Biggest upset: won R${biggest_upset.round} with only ${biggest_upset.pre_prob}% odds (+${biggest_upset.swing}% swing)`}>
                <span className="meta-val text-gold">R{biggest_upset.round}</span>
                <span className="meta-lbl">BIGGEST UPSET</span>
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
              title="Download this report as a PNG image"
            >
              {busy ? '…' : '⭳ PNG'}
            </button>
          )}
          {rounds.length > 0 && (
            <button
              type="button"
              className="pmr-header-action-btn pmr-dl-csv-btn"
              onClick={exportCsv}
              disabled={!!busy}
              title="Download round-by-round breakdown as CSV"
            >
              ⭳ CSV
            </button>
          )}
          {(reportUrl || reportFile) && onOpenReport && (
            <button
              className="pmr-header-action-btn"
              onClick={() => onOpenReport(reportUrl, reportFile)}
              title="Open standalone interactive dashboard in browser"
            >
              📊 FULL REPORT ↗
            </button>
          )}
        </div>
      </div>

      {/* 2. Win Probability Timeline Bar Chart */}
      <div className="pmr-card pmr-chart-card">
        <div className="card-header">
          <div className="card-title-group">
            <span className="card-icon">📊</span>
            <h3 className="card-title">WIN PROBABILITY BY ROUND</h3>
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
              <span className="legend-line" /> 50% Odds
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
                stroke="#64748b"
                tick={{ fill: '#94a3b8', fontSize: 11, fontWeight: 500 }}
                axisLine={{ stroke: '#334155' }}
                tickLine={false}
              />
              <YAxis
                domain={[0, 100]}
                ticks={[0, 25, 50, 75, 100]}
                stroke="#64748b"
                tick={{ fill: '#64748b', fontSize: 10 }}
                axisLine={{ stroke: '#334155' }}
                tickLine={false}
                unit="%"
              />
              <ReferenceLine
                y={50}
                stroke="#64748b"
                strokeDasharray="4 4"
                strokeWidth={1.5}
              />
              <Tooltip content={<CustomChartTooltip />} cursor={{ fill: 'rgba(255, 255, 255, 0.04)' }} />
              <Bar dataKey="prob" radius={[3, 3, 0, 0]}>
                {chartData.map((entry, index) => {
                  const fillColor = entry.won ? '#00e5cc' : '#ff4655'
                  return <Cell key={`cell-${index}`} fill={fillColor} />
                })}
              </Bar>
            </BarChart>
          </ResponsiveContainer>

          {/* Clutch / Choke floating markers above bars */}
          <div className="chart-markers-row">
            {chartData.map((d, i) => (
              <div key={i} className="chart-marker-cell">
                {d.performance === 'clutch' && (
                  <span className="chart-perf-clutch" title={`R${d.roundNum}: Clutch win (only ${d.prob}% odds)`}>
                    🔥
                  </span>
                )}
                {d.performance === 'choke' && (
                  <span className="chart-perf-choke" title={`R${d.roundNum}: Choked (had ${d.prob}% odds)`}>
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
            <span className="card-icon">📈</span>
            <h3 className="card-title">LIVE PROBABILITY TIMELINE</h3>
          </div>
          <span className="card-subtitle">Model B combat evolution per round</span>
        </div>

        <div className="timeline-hint">
          <span>▶ Click any round card to open the kill-by-kill combat progression</span>
        </div>

        <div className="pmr-timeline-grid">
          {timelineData.map((t, i) => (
            <button
              key={i}
              type="button"
              className={`pmr-spark-card ${t.won ? 'spark-won' : 'spark-lost'}`}
              onClick={() => setActiveRound(t)}
              title={`R${t.round}: click to inspect combat progression`}
            >
              <div className="spark-top">
                <span className="spark-rnd">R{t.round}</span>
                <span className={`spark-res ${t.won ? 'spark-w' : 'spark-l'}`}>{t.won ? 'W' : 'L'}</span>
              </div>
              <Sparkline series={t.series} won={t.won} height={34} />
              <div className="spark-bottom">
                <span className="spark-kills">{t.kills.length} K</span>
                <span className={`spark-swing ${t.won ? 'swing-up' : 'swing-down'}`}>{fmtSwing(t.swing)}</span>
              </div>
            </button>
          ))}
        </div>
      </div>

      {/* 4. Two Columns: Match-Swinging Rounds & Economy Efficiency 2x2 */}
      <div className="pmr-two-col-grid">
        {/* Left Column: Match-Swinging Rounds */}
        <div className="pmr-card pmr-swing-card">
          <div className="card-header">
            <div className="card-title-group">
              <span className="card-icon">⚡</span>
              <h3 className="card-title">MATCH-SWINGING ROUNDS</h3>
            </div>
            <span className="card-subtitle">Highest Win Probability Swings</span>
          </div>

          <div className="pivotal-list">
            {pivotal_rounds && pivotal_rounds.length > 0 ? (
              pivotal_rounds.map((p, idx) => {
                const isWon = p.won
                return (
                  <div
                    key={idx}
                    className={`pivotal-item ${isWon ? 'piv-item-won' : 'piv-item-lost'}`}
                  >
                    <div className="piv-item-glow" />
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
                  </div>
                )
              })
            ) : (
              <div className="no-data-msg">No high-swing rounds recorded for this match.</div>
            )}
          </div>
        </div>

        {/* Right Column: Economy Efficiency 2x2 Grid */}
        <div className="pmr-card pmr-econ-card">
          <div className="card-header">
            <div className="card-title-group">
              <span className="card-icon">💰</span>
              <h3 className="card-title">ECONOMY EFFICIENCY</h3>
            </div>
            <span className="card-subtitle">Win Rate by Buy Category</span>
          </div>

          <div className="econ-2x2-grid">
            {econTypes.map(({ key, label, color, icon }) => {
              const stat = economy[key] || { played: 0, won: 0 }
              const played = stat.played || 0
              const won = stat.won || 0
              const lost = Math.max(0, played - won)
              const winRate = played > 0 ? Math.round((won / played) * 100) : 0
              const isHigh = winRate >= 50

              return (
                <div
                  key={key}
                  className={`econ-tile ${played === 0 ? 'econ-tile-empty' : ''}`}
                  style={{ '--tile-accent': color, '--tile-accent-dim': `${color}22`, '--tile-accent-glow': `${color}33` }}
                >
                  <div className="econ-tile-header">
                    <span className="econ-type-label">
                      <span className="econ-tile-icon">{icon}</span>
                      {label}
                    </span>
                    {played > 0 && (
                      <span className="econ-wr-badge" style={{ background: `${color}22`, color, borderColor: `${color}44` }}>
                        {winRate}% WR
                      </span>
                    )}
                  </div>

                  <div className="econ-tile-number">
                    <span className="big-wr" style={{ color: played > 0 ? color : '#64748b' }}>
                      {played > 0 ? `${winRate}%` : '—'}
                    </span>
                  </div>

                  <div className="econ-bar-track">
                    <div
                      className="econ-bar-fill"
                      style={{
                        width: `${played > 0 ? winRate : 0}%`,
                        background: `linear-gradient(90deg, ${color}66, ${color})`,
                        boxShadow: `0 0 6px ${color}44`,
                      }}
                    />
                  </div>

                  <div className="econ-tile-record">
                    {played > 0 ? (
                      <span><strong style={{ color }}>{won}W</strong> - <strong>{lost}L</strong> <span className="econ-tile-played">({played} played)</span></span>
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
          <div className="pmr-card">
            <div className="card-header">
              <div className="card-title-group">
                <span className="card-icon">🏅</span>
                <h3 className="card-title">TEAM & RANKS</h3>
              </div>
            </div>
            <div className="pmr-comp-grid">
              <div className="pmr-comp-col">
                <div className="pmr-comp-label comp-ally">YOUR TEAM</div>
                {allies.map((p, i) => (
                  <div className="pmr-comp-row" key={`a${i}`}>
                    <span className="pmr-comp-agent">{p.agent}</span>
                    <span className="pmr-comp-name">{p.name}</span>
                    <span className="pmr-comp-rank"><RankBadge rank={p.rank} />{p.rank}</span>
                  </div>
                ))}
              </div>
              <div className="pmr-comp-col">
                <div className="pmr-comp-label comp-enemy">ENEMIES</div>
                {enemies.map((p, i) => (
                  <div className="pmr-comp-row" key={`e${i}`}>
                    <span className="pmr-comp-agent">{p.agent}</span>
                    <span className="pmr-comp-name">{p.name}</span>
                    <span className="pmr-comp-rank"><RankBadge rank={p.rank} />{p.rank}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )
      })()}

      {/* 6. Round Breakdown Table */}
      <div className="pmr-card pmr-table-card">
        <div className="card-header">
          <div className="card-title-group">
            <span className="card-icon">📋</span>
            <h3 className="card-title">ROUND-BY-ROUND BREAKDOWN</h3>
          </div>
          <span className="card-subtitle">{totalRounds} total rounds</span>
        </div>

        <div className="table-responsive-wrap">
          <table className="pmr-breakdown-table">
            <thead>
              <tr>
                <th className="th-rnd">RND</th>
                <th className="th-side">SIDE</th>
                <th className="th-buy">BUY TYPE</th>
                <th className="th-eval">BUY ADV</th>
                <th className="th-prob">PRE-ROUND WIN %</th>
                <th className="th-kd">K / D</th>
                <th className="th-dmg">DMG</th>
                <th className="th-res">RESULT</th>
                <th className="th-swing">SWING</th>
              </tr>
            </thead>
            <tbody>
              {rounds.map((r, i) => {
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
                  >
                    <td className="td-rnd">
                      <span className="rnd-badge">R{r.round}</span>
                    </td>
                    <td className="td-side">
                      <span className={`side-badge ${isAtk ? 'side-atk' : 'side-def'}`}>
                        {isAtk ? 'ATK' : 'DEF'}
                      </span>
                    </td>
                    <td className="td-buy">
                      <span className={`buy-badge buy-${r.buy_type || 'full'}`}>
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
                            className={`prob-meter-fill ${isWon ? 'fill-ally' : 'fill-enemy'}`}
                            style={{ width: `${Math.min(100, Math.max(0, r.pre_prob))}%` }}
                          />
                        </div>
                        <span className="prob-cell-text">{r.pre_prob}%</span>
                      </div>
                    </td>
                    <td className="td-kd">
                      <span className="kd-text">
                        <strong className="text-white">{k}</strong> / <span className="text-muted">{d}</span>
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
                          <span className="perf-tag tag-clutch" title="Won against the odds">
                            🔥 CLUTCH
                          </span>
                        )}
                        {isChoke && (
                          <span className="perf-tag tag-choke" title="Lost a favored round">
                            ⚠️ CHOKE
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="td-swing">
                      <span className={`swing-val ${!isNaN(swingVal) && swingVal >= 0 ? 'text-ally' : 'text-enemy'}`}>
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
                <span className="pmr-modal-rnd">ROUND {activeRound.round}</span>
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
                {busy ? '…' : '⭳ PNG'}
              </button>
              <button type="button" className="pmr-modal-close" onClick={() => setActiveRound(null)} aria-label="Close">
                ✕
              </button>
            </div>

            <div className="pmr-modal-meta">
              <span className={`side-badge ${activeRound.side === 'attack' ? 'side-atk' : 'side-def'}`}>
                {String(activeRound.side || '').toUpperCase().slice(0, 3)}
              </span>
              <span className="buy-badge">{String(activeRound.buyType).replace('_', ' ').toUpperCase()}</span>
              <span className="pmr-modal-stat">PRE {activeRound.preProb}%</span>
              <span className="pmr-modal-stat">FINAL {!isNaN(activeRound.finalProb) ? Math.round(activeRound.finalProb) : '—'}%</span>
              <span className={`pmr-modal-stat ${activeRound.won ? 'text-ally' : 'text-enemy'}`}>
                SWING {fmtSwing(activeRound.swing)}
              </span>
            </div>

            <div className="pmr-modal-chart">
              <Sparkline series={activeRound.series} won={activeRound.won} width={460} height={140} stretch={false} markers labels />
            </div>

            <div className="pmr-kill-list">
              {activeRound.kills.length === 0 && (
                <div className="no-data-msg">No kills captured this round.</div>
              )}
              {activeRound.kills.map((k, i) => {
                const prev = i === 0 ? activeRound.preProb : Number(activeRound.kills[i - 1].live_prob)
                const live = Number(k.live_prob)
                const delta = Math.round((live - prev) * 10) / 10
                const allyKill = !!k.is_attacker_teammate
                return (
                  <div key={i} className={`pmr-kill-row ${allyKill ? 'kill-ally' : 'kill-enemy'}`}>
                    <span className="kill-idx">K{i + 1}</span>
                    <span className="kill-event">
                      <span className="kill-icon">{allyKill ? '🗡️' : '💀'}</span>
                      {k.attacker} <span className="kill-arrow">→</span> {k.victim}
                      {k.headshot && <span className="kill-hs"> 💥 HS</span>}
                    </span>
                    <span className="kill-alive">{k.att_alive}v{k.def_alive}</span>
                    <span className="kill-prob">
                      {live}% <span className={`kill-delta ${delta >= 0 ? 'text-ally' : 'text-enemy'}`}>{delta >= 0 ? '+' : ''}{delta}%</span>
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
