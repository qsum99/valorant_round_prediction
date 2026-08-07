import { useMemo } from 'react'
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
import './PostMatchReport.css'

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

export function PostMatchReport({ report }) {
  if (!report) return null

  const {
    map = 'VALORANT',
    outcome = 'defeat',
    final_score = [0, 0],
    rounds = [],
    pivotal_rounds = [],
    economy = {},
  } = report

  const isVictory = outcome.toLowerCase() === 'victory'
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
    return rounds.map(r => ({
      name: `R${r.round}`,
      roundNum: r.round,
      prob: Math.round(r.pre_prob * 10) / 10,
      won: r.won,
      performance: r.performance || 'expected',
      side: r.side,
      buyType: r.buy_type || 'full_buy',
      kills: r.kills ?? 0,
      deaths: r.deaths ?? 0,
    }))
  }, [rounds])

  // Economy types list
  const econTypes = [
    { key: 'pistol', label: 'PISTOL' },
    { key: 'eco', label: 'ECO' },
    { key: 'force', label: 'FORCE BUY' },
    { key: 'full_buy', label: 'FULL BUY' },
  ]

  return (
    <div className="pmr-container">
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
          <div className="map-title">{(map && map.trim()) ? map.toUpperCase() : 'VALORANT'}</div>
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

      {/* 3. Two Columns: Match-Swinging Rounds & Economy Efficiency 2x2 */}
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
            {econTypes.map(({ key, label }) => {
              const stat = economy[key] || { played: 0, won: 0 }
              const played = stat.played || 0
              const won = stat.won || 0
              const lost = Math.max(0, played - won)
              const winRate = played > 0 ? Math.round((won / played) * 100) : 0
              const isHigh = winRate >= 50

              return (
                <div key={key} className={`econ-tile ${played === 0 ? 'econ-tile-empty' : ''}`}>
                  <div className="econ-tile-header">
                    <span className="econ-type-label">{label}</span>
                    {played > 0 && (
                      <span className={`econ-wr-badge ${isHigh ? 'badge-teal' : 'badge-red'}`}>
                        {winRate}% WR
                      </span>
                    )}
                  </div>

                  <div className="econ-tile-number">
                    <span className={`big-wr ${played === 0 ? 'text-muted' : isHigh ? 'text-ally' : 'text-enemy'}`}>
                      {played > 0 ? `${winRate}%` : '—'}
                    </span>
                  </div>

                  {/* Mini Progress Bar */}
                  <div className="econ-bar-track">
                    <div
                      className={`econ-bar-fill ${isHigh ? 'fill-ally' : 'fill-enemy'}`}
                      style={{ width: `${played > 0 ? winRate : 0}%` }}
                    />
                  </div>

                  <div className="econ-tile-record">
                    {played > 0 ? (
                      <span><strong>{won}W</strong> - <strong>{lost}L</strong> ({played} played)</span>
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

      {/* 4. Round Breakdown Table */}
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
                <th className="th-prob">PRE-ROUND WIN %</th>
                <th className="th-kd">K / D</th>
                <th className="th-res">RESULT</th>
              </tr>
            </thead>
            <tbody>
              {rounds.map((r, i) => {
                const isWon = r.won
                const isClutch = r.performance === 'clutch'
                const isChoke = r.performance === 'choke'
                const buyLabel = (r.buy_type || 'full_buy').replace('_', ' ').toUpperCase()
                const isAtk = r.side === 'attack'

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
                        <strong className="text-white">{r.kills ?? 0}</strong> / <span className="text-muted">{r.deaths ?? 0}</span>
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
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* 5. Footer Card: Player Performance Summary */}
      <div className="pmr-card pmr-footer-card">
        <div className="footer-stats-grid">
          <div className="footer-stat">
            <div className="footer-stat-icon">🔥</div>
            <div className="footer-stat-info">
              <span className="footer-stat-val text-gold">{playerStats.clutchCount}</span>
              <span className="footer-stat-label">CLUTCH ROUNDS</span>
              <span className="footer-stat-desc">Won with &lt;40% odds</span>
            </div>
          </div>
          <div className="footer-stat">
            <div className="footer-stat-icon">⚠️</div>
            <div className="footer-stat-info">
              <span className="footer-stat-val text-enemy">{playerStats.chokeCount}</span>
              <span className="footer-stat-label">CHOKE ROUNDS</span>
              <span className="footer-stat-desc">Lost with &gt;60% odds</span>
            </div>
          </div>
          <div className="footer-stat">
            <div className="footer-stat-icon">✅</div>
            <div className="footer-stat-info">
              <span className="footer-stat-val text-ally">{playerStats.favoredPct}%</span>
              <span className="footer-stat-label">WIN RATE WHEN FAVORED</span>
              <span className="footer-stat-desc">{playerStats.favoredWins}W / {playerStats.favoredTotal - playerStats.favoredWins}L ({playerStats.favoredTotal} rounds)</span>
            </div>
          </div>
          <div className="footer-stat">
            <div className="footer-stat-icon">🎲</div>
            <div className="footer-stat-info">
              <span className="footer-stat-val" style={{ color: playerStats.underdogPct > 30 ? '#e8c468' : '#94a3b8' }}>{playerStats.underdogPct}%</span>
              <span className="footer-stat-label">WIN RATE AS UNDERDOG</span>
              <span className="footer-stat-desc">{playerStats.underdogWins}W / {playerStats.underdogTotal - playerStats.underdogWins}L ({playerStats.underdogTotal} rounds)</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
