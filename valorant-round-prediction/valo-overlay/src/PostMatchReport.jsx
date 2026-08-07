import './PostMatchReport.css'

export function PostMatchReport({ report }) {
  if (!report) return null

  const { map, outcome, final_score, rounds, pivotal_rounds, economy } = report
  const won = outcome === 'victory'
  const totalRounds = rounds.length
  const halfRound = Math.ceil(totalRounds / 2)

  // Find max probability for chart scaling
  const maxProb = 100

  return (
    <div className="pmr-root">
      {/* Header */}
      <div className={`pmr-header ${won ? 'victory' : 'defeat'}`}>
        <div className="pmr-result">{won ? 'VICTORY' : 'DEFEAT'}</div>
        <div className="pmr-score">{final_score[0]} — {final_score[1]}</div>
        <div className="pmr-map">{map || 'VALORANT'}</div>
      </div>

      {/* Win Probability Timeline */}
      <div className="pmr-section">
        <div className="pmr-section-title">WIN PROBABILITY BY ROUND</div>
        <div className="pmr-chart">
          <div className="pmr-chart-midline" />
          <div className="pmr-chart-bars">
            {rounds.map((r, i) => {
              const prob = r.pre_prob
              const barHeight = Math.max(4, (prob / maxProb) * 100)
              const isOverHalf = i >= halfRound - 1 && i < halfRound && totalRounds > 12
              return (
                <div key={i} className="pmr-bar-col">
                  {isOverHalf && <div className="pmr-half-marker" />}
                  <div
                    className={`pmr-bar ${r.won ? 'bar-won' : 'bar-lost'} ${
                      r.performance === 'overperformed' ? 'bar-over' :
                      r.performance === 'underperformed' ? 'bar-under' : ''
                    }`}
                    style={{ height: `${barHeight}%` }}
                    title={`R${r.round}: ${prob}% → ${r.won ? 'Won' : 'Lost'}`}
                  >
                    <span className="pmr-bar-label">{Math.round(prob)}</span>
                  </div>
                  <span className="pmr-bar-round">R{r.round}</span>
                </div>
              )
            })}
          </div>
          <div className="pmr-chart-legend">
            <span className="legend-item"><span className="dot won" />Won</span>
            <span className="legend-item"><span className="dot lost" />Lost</span>
            <span className="legend-item"><span className="dot over" />Overperformed</span>
            <span className="legend-item"><span className="dot under" />Underperformed</span>
          </div>
        </div>
      </div>

      {/* Pivotal Rounds */}
      {pivotal_rounds && pivotal_rounds.length > 0 && (
        <div className="pmr-section">
          <div className="pmr-section-title">MATCH-SWINGING ROUNDS</div>
          <div className="pmr-pivotal-list">
            {pivotal_rounds.map((p, i) => (
              <div key={i} className={`pmr-pivotal-card ${p.won ? 'piv-won' : 'piv-lost'}`}>
                <div className="piv-header">
                  <span className="piv-round">ROUND {p.round}</span>
                  <span className={`piv-badge ${p.won ? 'badge-won' : 'badge-lost'}`}>
                    {p.won ? 'WON' : 'LOST'}
                  </span>
                </div>
                <div className="piv-reason">{p.reason}</div>
                <div className="piv-stats">
                  <span className="piv-pre">Pre-round: {p.pre_prob}%</span>
                  <span className="piv-swing">Swing: {p.swing}%</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Economy Efficiency */}
      <div className="pmr-section">
        <div className="pmr-section-title">ECONOMY EFFICIENCY</div>
        <div className="pmr-econ-grid">
          {['pistol', 'eco', 'force', 'full_buy'].map(bt => {
            const stats = economy[bt]
            if (!stats || stats.played === 0) return null
            const winRate = Math.round((stats.won / stats.played) * 100)
            const label = bt === 'full_buy' ? 'FULL BUY' : bt.toUpperCase()
            return (
              <div key={bt} className="pmr-econ-card">
                <div className="econ-label">{label}</div>
                <div className="econ-bar-track">
                  <div
                    className="econ-bar-fill"
                    style={{ width: `${winRate}%` }}
                  />
                </div>
                <div className="econ-stats">
                  <span className="econ-wr">{winRate}%</span>
                  <span className="econ-detail">{stats.won}W / {stats.played - stats.won}L</span>
                </div>
              </div>
            )
          })}
        </div>
      </div>

      {/* Round Performance Table */}
      <div className="pmr-section">
        <div className="pmr-section-title">ROUND BREAKDOWN</div>
        <div className="pmr-table-wrap">
          <table className="pmr-table">
            <thead>
              <tr>
                <th>RND</th>
                <th>SIDE</th>
                <th>BUY</th>
                <th>PRE%</th>
                <th>K/D</th>
                <th>RESULT</th>
              </tr>
            </thead>
            <tbody>
              {rounds.map((r, i) => (
                <tr key={i} className={`${r.won ? 'row-won' : 'row-lost'} ${
                  r.performance === 'overperformed' ? 'row-over' :
                  r.performance === 'underperformed' ? 'row-under' : ''
                }`}>
                  <td className="td-round">{r.round}</td>
                  <td className={`td-side ${r.side === 'attack' ? 'side-atk' : 'side-def'}`}>
                    {r.side === 'attack' ? 'ATK' : 'DEF'}
                  </td>
                  <td className="td-buy">{r.buy_type === 'full_buy' ? 'FULL' : r.buy_type.toUpperCase()}</td>
                  <td className="td-prob">{r.pre_prob}%</td>
                  <td className="td-kd">{r.kills}/{r.deaths}</td>
                  <td className={`td-result ${r.won ? 'res-won' : 'res-lost'}`}>
                    {r.won ? 'W' : 'L'}
                    {r.performance === 'overperformed' && <span className="perf-tag over-tag">▲</span>}
                    {r.performance === 'underperformed' && <span className="perf-tag under-tag">▼</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
