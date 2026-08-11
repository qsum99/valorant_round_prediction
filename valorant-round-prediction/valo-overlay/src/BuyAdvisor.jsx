import './BuyAdvisor.css'

const URGENCY_COLORS = {
  high:   { bg: 'rgba(255, 70, 85, 0.18)', border: '#ff4655', text: '#ff4655', glow: 'rgba(255, 70, 85, 0.25)' },
  medium: { bg: 'rgba(232, 196, 104, 0.18)', border: '#e8c468', text: '#e8c468', glow: 'rgba(232, 196, 104, 0.25)' },
  low:    { bg: 'rgba(34, 197, 94, 0.18)', border: '#22c55e', text: '#22c55e', glow: 'rgba(34, 197, 94, 0.25)' },
}

const BUY_LABELS = {
  eco:      'ECO / SAVE',
  force:    'FORCE BUY',
  half_buy: 'HALF BUY',
  full_buy: 'FULL BUY',
  bonus:    'BONUS ROUND',
  anti_eco: 'ANTI-ECO',
  broken:   'BROKEN ECON',
  pistol:   'PISTOL ROUND',
}

const BUY_ICONS = {
  eco:      '💰',
  force:    '⚔️',
  half_buy: '🥋',
  full_buy: '🛡️',
  bonus:    '💸',
  anti_eco: '⚡',
  broken:   '🧩',
  pistol:   '🔫',
}

const BUY_ACCENT = {
  eco:      { color: '#22c55e', bg: 'rgba(34, 197, 94, 0.12)', border: 'rgba(34, 197, 94, 0.4)', glow: '0 0 12px rgba(34, 197, 94, 0.15)' },
  force:    { color: '#e8c468', bg: 'rgba(232, 196, 104, 0.12)', border: 'rgba(232, 196, 104, 0.4)', glow: '0 0 12px rgba(232, 196, 104, 0.15)' },
  half_buy: { color: '#fb923c', bg: 'rgba(251, 146, 60, 0.12)', border: 'rgba(251, 146, 60, 0.4)', glow: '0 0 12px rgba(251, 146, 60, 0.15)' },
  full_buy: { color: '#4fc3f7', bg: 'rgba(79, 195, 247, 0.12)', border: 'rgba(79, 195, 247, 0.4)', glow: '0 0 12px rgba(79, 195, 247, 0.15)' },
  bonus:    { color: '#a78bfa', bg: 'rgba(167, 139, 250, 0.12)', border: 'rgba(167, 139, 250, 0.4)', glow: '0 0 12px rgba(167, 139, 250, 0.15)' },
  anti_eco: { color: '#fb923c', bg: 'rgba(251, 146, 60, 0.12)', border: 'rgba(251, 146, 60, 0.4)', glow: '0 0 12px rgba(251, 146, 60, 0.15)' },
  broken:   { color: '#e879f9', bg: 'rgba(232, 121, 249, 0.12)', border: 'rgba(232, 121, 249, 0.4)', glow: '0 0 12px rgba(232, 121, 249, 0.15)' },
  pistol:   { color: '#94a3b8', bg: 'rgba(148, 163, 184, 0.12)', border: 'rgba(148, 163, 184, 0.4)', glow: '0 0 12px rgba(148, 163, 184, 0.1)' },
}

const SCENARIO_LABELS = {
  eco:      'ECO',
  force:    'FORCE',
  half_buy: 'HALF',
  full_buy: 'FULL',
}

const SCENARIO_COLORS = {
  eco:      '#22c55e',
  force:    '#e8c468',
  half_buy: '#fb923c',
  full_buy: '#4fc3f7',
}

const ENEMY_COLORS = {
  eco:      { color: '#22c55e', border: 'rgba(34, 197, 94, 0.4)', bg: 'rgba(34, 197, 94, 0.12)' },
  force:    { color: '#e8c468', border: 'rgba(232, 196, 104, 0.4)', bg: 'rgba(232, 196, 104, 0.12)' },
  half_buy: { color: '#fb923c', border: 'rgba(251, 146, 60, 0.4)', bg: 'rgba(251, 146, 60, 0.12)' },
  full_buy: { color: '#ff4655', border: 'rgba(255, 70, 85, 0.4)', bg: 'rgba(255, 70, 85, 0.12)' },
  bonus:    { color: '#a78bfa', border: 'rgba(167, 139, 250, 0.4)', bg: 'rgba(167, 139, 250, 0.12)' },
  anti_eco: { color: '#fb923c', border: 'rgba(251, 146, 60, 0.4)', bg: 'rgba(251, 146, 60, 0.12)' },
  broken:   { color: '#e879f9', border: 'rgba(232, 121, 249, 0.4)', bg: 'rgba(232, 121, 249, 0.12)' },
  pistol:   { color: '#94a3b8', border: 'rgba(148, 163, 184, 0.4)', bg: 'rgba(148, 163, 184, 0.12)' },
}

function EnemyBadge({ enemy_buy, enemy_money, round }) {
  const key = enemy_buy || 'pistol'
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

  const { recommendation: rec, reason, scenarios, urgency, context, plan,
          ally_money, enemy_money } = recommendation

  const label = BUY_LABELS[rec] || rec.toUpperCase()
  const icon = BUY_ICONS[rec] || '💰'
  const urg = URGENCY_COLORS[urgency] || URGENCY_COLORS.low
  const accent = BUY_ACCENT[rec] || BUY_ACCENT.pistol
  const isPistol = rec === 'pistol'

  const maxProb = scenarios
    ? Math.max(1, ...Object.values(scenarios).map(s => s.this_round || 0))
    : 100

  return (
    <div className="buy-advisor" style={{ '--ba-accent': accent.color, '--ba-accent-bg': accent.bg, '--ba-accent-border': accent.border, '--ba-accent-glow': accent.glow }}>
      {/* Header row */}
      <div className="ba-header">
        <div className="ba-rec-group">
          <span className="ba-icon">{icon}</span>
          <span className="ba-rec-label">{label}</span>
        </div>
        <span
          className="ba-urgency"
          style={{ background: urg.bg, color: urg.text, borderColor: urg.border, boxShadow: `0 0 8px ${urg.glow}` }}
        >
          {urgency.toUpperCase()}
        </span>
      </div>

      {/* Reason */}
      <div className="ba-reason">{reason}</div>

      {/* Scenario bars */}
      {!isPistol && scenarios && Object.keys(scenarios).length > 0 && (
        <div className="ba-scenarios">
          <div className="ba-sc-header">WIN PROBABILITY</div>
          {Object.keys(scenarios).map(key => {
            const sc = scenarios[key]
            if (!sc) return null
            const isSelected = key === rec
            const barLabel = SCENARIO_LABELS[key] || key.toUpperCase()
            const barColor = SCENARIO_COLORS[key] || '#6b7280'
            return (
              <div
                key={key}
                className={`ba-scenario-row ${isSelected ? 'ba-selected' : ''}`}
              >
                <span className="ba-sc-label" style={isSelected ? { color: barColor } : {}}>{barLabel}</span>
                <div className="ba-sc-bar-track">
                  <div
                    className="ba-sc-bar-fill"
                    style={{
                      width: `${Math.min(100, sc.this_round)}%`,
                      background: isSelected
                        ? `linear-gradient(90deg, ${barColor}88, ${barColor})`
                        : `${barColor}33`,
                      boxShadow: isSelected ? `0 0 8px ${barColor}44` : 'none',
                    }}
                  />
                </div>
                <span className="ba-sc-pct" style={isSelected ? { color: barColor } : {}}>
                  {sc.this_round}%
                </span>
                <span className="ba-sc-ev" style={isSelected ? { color: barColor } : {}}>
                  EV {sc.two_round_ev}%
                </span>
              </div>
            )
          })}
        </div>
      )}

      {/* Plan line */}
      {plan && (
        <div className="ba-plan">
          <span className="ba-plan-icon">→</span>
          {plan}
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
