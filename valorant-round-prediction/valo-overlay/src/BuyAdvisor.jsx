import './BuyAdvisor.css'

const URGENCY_COLORS = {
  high:   { bg: 'rgba(255, 70, 85, 0.2)', border: '#ff4655', text: '#ff4655' },
  medium: { bg: 'rgba(232, 196, 104, 0.2)', border: '#e8c468', text: '#e8c468' },
  low:    { bg: 'rgba(34, 197, 94, 0.2)', border: '#22c55e', text: '#22c55e' },
}

const BUY_LABELS = {
  eco:      'ECO / SAVE',
  force:    'FORCE BUY',
  full_buy: 'FULL BUY',
  pistol:   'PISTOL ROUND',
}

const BUY_ICONS = {
  eco:      '💰',
  force:    '⚔️',
  full_buy: '🛡️',
  pistol:   '🔫',
}

const ENEMY_COLORS = {
  eco:      { color: '#22c55e', border: 'rgba(34, 197, 94, 0.4)', bg: 'rgba(34, 197, 94, 0.12)' },
  force:    { color: '#e8c468', border: 'rgba(232, 196, 104, 0.4)', bg: 'rgba(232, 196, 104, 0.12)' },
  full_buy: { color: '#ff4655', border: 'rgba(255, 70, 85, 0.4)', bg: 'rgba(255, 70, 85, 0.12)' },
  pistol:   { color: '#94a3b8', border: 'rgba(148, 163, 184, 0.4)', bg: 'rgba(148, 163, 184, 0.12)' },
}

// Mirrors backend classify_buy thresholds (fallback if server is outdated)
function classifyEnemy(enemy_money, round) {
  const m = Number(enemy_money) || 0
  const r = Number(round) || 0
  if (r === 1 || r === 13) return 'pistol'
  if (m < 10000) return 'eco'
  if (m < 14000) return 'force'
  return 'full_buy'
}

function EnemyBadge({ enemy_buy, enemy_money, round }) {
  const key = enemy_buy || classifyEnemy(enemy_money, round)
  const c = ENEMY_COLORS[key] || ENEMY_COLORS.pistol
  const label = BUY_LABELS[key]
    ? `ENEMY: ${BUY_LABELS[key]}`
    : `ENEMY: ${String(key).toUpperCase().replace('_', ' ')}`
  return (
    <span className="enemy-badge" style={{ color: c.color, background: c.bg, borderColor: c.border }}>
      {label}
    </span>
  )
}

export function BuyAdvisor({ recommendation, round }) {
  if (!recommendation) return null

  const { recommendation: rec, reason, scenarios, urgency, context,
          ally_money, enemy_money } = recommendation

  const label = BUY_LABELS[rec] || rec.toUpperCase()
  const icon = BUY_ICONS[rec] || '💰'
  const urg = URGENCY_COLORS[urgency] || URGENCY_COLORS.low
  const isPistol = rec === 'pistol'

  // Find the max this_round value for scaling bars
  const maxProb = scenarios
    ? Math.max(
        scenarios.eco?.this_round || 0,
        scenarios.force?.this_round || 0,
        scenarios.full_buy?.this_round || 0,
        1
      )
    : 100

  return (
    <div className="buy-advisor" style={{ borderLeftColor: urg.border }}>
      {/* Header row: recommendation + urgency */}
      <div className="ba-header">
        <div className="ba-rec-group">
          <span className="ba-icon">{icon}</span>
          <span className={`ba-rec-label ba-rec-${rec}`}>{label}</span>
        </div>
        <span
          className="ba-urgency"
          style={{ background: urg.bg, color: urg.text, borderColor: urg.border }}
        >
          {urgency.toUpperCase()}
        </span>
      </div>

      {/* Reason */}
      <div className="ba-reason">{reason}</div>

      {/* Scenario comparison bars (skip for pistol) */}
      {!isPistol && scenarios && (
        <div className="ba-scenarios">
          {['eco', 'force', 'full_buy'].map(key => {
            const sc = scenarios[key]
            if (!sc) return null
            const isSelected = key === rec
            const barLabel = key === 'full_buy' ? 'FULL' : key.toUpperCase()
            return (
              <div
                key={key}
                className={`ba-scenario-row ${isSelected ? 'ba-selected' : ''}`}
              >
                <span className="ba-sc-label">{barLabel}</span>
                <div className="ba-sc-bar-track">
                  <div
                    className={`ba-sc-bar-fill ${isSelected ? 'fill-selected' : ''}`}
                    style={{ width: `${Math.min(100, sc.this_round)}%` }}
                  />
                </div>
                <span className="ba-sc-pct">{sc.this_round}%</span>
                <span className="ba-sc-ev" title="2-round expected value">
                  EV {sc.two_round_ev}%
                </span>
              </div>
            )
          })}
        </div>
      )}

      {/* Context line */}
      {context && (
        <div className="ba-context">{context}</div>
      )}

      {/* Economy display */}
      <div className="ba-econ-row">
        <span className="ba-econ-ally">
          Your team: <strong>{(ally_money || 0).toLocaleString()}</strong>
        </span>
        <EnemyBadge enemy_buy={recommendation.enemy_buy} enemy_money={enemy_money} round={round} />
      </div>
    </div>
  )
}
