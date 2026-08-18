import { ShoppingCart, ShieldAlert, ArrowRight, Wallet, Target, Zap } from 'lucide-react'
import './BuyAdvisor.css'

const URGENCY_COLORS = {
  high:   { bg: 'var(--ally-dim)', border: 'var(--ally)', text: 'var(--ally)' },
  medium: { bg: 'var(--gold-dim)', border: 'var(--gold)', text: 'var(--gold)' },
  low:    { bg: 'var(--success-dim)', border: 'var(--success)', text: 'var(--success)' },
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

const BUY_ACCENT = {
  eco:      { color: 'var(--success)', bg: 'var(--success-dim)', border: 'var(--success)' },
  force:    { color: 'var(--gold)', bg: 'var(--gold-dim)', border: 'var(--gold)' },
  half_buy: { color: 'var(--warning)', bg: 'var(--warning-dim)', border: 'var(--warning)' },
  full_buy: { color: 'var(--enemy)', bg: 'var(--enemy-dim)', border: 'var(--enemy)' },
  bonus:    { color: 'var(--purple)', bg: 'var(--purple-dim)', border: 'var(--purple)' },
  anti_eco: { color: 'var(--warning)', bg: 'var(--warning-dim)', border: 'var(--warning)' },
  broken:   { color: '#e879f9', bg: 'rgba(232, 121, 249, 0.16)', border: 'rgba(232, 121, 249, 0.4)' },
  pistol:   { color: 'var(--text-3)', bg: 'var(--surface3)', border: 'var(--border)' },
}

const SCENARIO_LABELS = {
  eco:      'ECO',
  force:    'FORCE',
  half_buy: 'HALF',
  full_buy: 'FULL',
}

const SCENARIO_COLORS = {
  eco:      'var(--success)',
  force:    'var(--gold)',
  half_buy: 'var(--warning)',
  full_buy: 'var(--enemy)',
}

const ENEMY_COLORS = {
  eco:      { color: 'var(--success)', border: 'var(--success)', bg: 'var(--success-dim)' },
  force:    { color: 'var(--gold)', border: 'var(--gold)', bg: 'var(--gold-dim)' },
  half_buy: { color: 'var(--warning)', border: 'var(--warning)', bg: 'var(--warning-dim)' },
  full_buy: { color: 'var(--ally)', border: 'var(--ally)', bg: 'var(--ally-dim)' },
  bonus:    { color: 'var(--purple)', border: 'var(--purple)', bg: 'var(--purple-dim)' },
  anti_eco: { color: 'var(--warning)', border: 'var(--warning)', bg: 'var(--warning-dim)' },
  broken:   { color: '#e879f9', border: 'rgba(232, 121, 249, 0.4)', bg: 'rgba(232, 121, 249, 0.16)' },
  pistol:   { color: 'var(--text-3)', border: 'var(--border)', bg: 'var(--surface3)' },
}

function EnemyBadge({ enemy_buy }) {
  const key = enemy_buy || 'pistol'
  const c = ENEMY_COLORS[key] || ENEMY_COLORS.pistol
  const label = BUY_LABELS[key] || String(key).toUpperCase().replace('_', ' ')
  return (
    <span className="enemy-badge" style={{ color: c.color, background: c.bg, borderColor: c.border }}>
      ENEMY: {label}
    </span>
  )
}

export function BuyAdvisor({ recommendation, round }) {
  if (!recommendation) return null

  const { recommendation: rec, reason, scenarios, urgency, context, plan,
          ally_money, enemy_money } = recommendation

  const label = BUY_LABELS[rec] || rec.toUpperCase()
  const urg = URGENCY_COLORS[urgency] || URGENCY_COLORS.low
  const accent = BUY_ACCENT[rec] || BUY_ACCENT.pistol
  const isPistol = rec === 'pistol'
  const isHighUrgency = urgency === 'high'

  return (
    <div
      className={`buy-advisor ${isHighUrgency ? 'ba-urgent' : ''}`}
      style={{
        '--ba-accent': accent.color,
        '--ba-accent-bg': accent.bg,
        '--ba-accent-border': accent.border
      }}
    >
      {/* Header */}
      <div className="ba-header">
        <div className="ba-rec-group">
          <ShoppingCart size={15} className="ba-icon" style={{ color: accent.color }} />
          <span className="ba-rec-label">{label}</span>
        </div>
        <span
          className="ba-urgency"
          style={{ background: urg.bg, color: urg.text, borderColor: urg.border }}
        >
          {urgency.toUpperCase()}
        </span>
      </div>

      {/* Rationale */}
      <div className="ba-reason">{reason}</div>

      {/* Scenario EV Comparison */}
      {!isPistol && scenarios && Object.keys(scenarios).length > 0 && (
        <div className="ba-scenarios">
          <div className="ba-sc-header">
            <span>BUY SCENARIO</span>
            <span>THIS RND / 2-RND EV</span>
          </div>
          {Object.keys(scenarios).map(key => {
            const sc = scenarios[key]
            if (!sc) return null
            const isSelected = key === rec
            const barLabel = SCENARIO_LABELS[key] || key.toUpperCase()
            const barColor = SCENARIO_COLORS[key] || 'var(--text-4)'
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
                      background: barColor,
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

      {/* Tactical Plan */}
      {plan && (
        <div className="ba-plan">
          <ArrowRight size={12} className="ba-plan-icon" />
          <span>{plan}</span>
        </div>
      )}

      {/* Context note */}
      {context && (
        <div className="ba-context">{context}</div>
      )}

      {/* Economy breakdown footer */}
      <div className="ba-econ-row">
        <div className="ba-econ-ally">
          <Wallet size={12} />
          <span>Team: <strong>{(ally_money || 0).toLocaleString()}</strong></span>
        </div>
        <EnemyBadge enemy_buy={recommendation.enemy_buy} />
      </div>
    </div>
  )
}
