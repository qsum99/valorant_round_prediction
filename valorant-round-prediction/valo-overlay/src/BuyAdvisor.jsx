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

export function BuyAdvisor({ recommendation }) {
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
        <span className="ba-econ-enemy">
          Enemy: <strong>{(enemy_money || 0).toLocaleString()}</strong>
        </span>
      </div>
    </div>
  )
}
