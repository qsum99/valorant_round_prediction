import './RoundSummary.css'

export function RoundSummary({ summary }) {
  if (!summary) return null
  const dmg = Math.round(summary.damage || 0)
  const hs = Math.round((summary.headshot || 0) + (summary.final_headshot || 0))
  const ability = Math.round(summary.ability_damage || 0)
  const received = Math.round(summary.damage_received || 0)

  return (
    <div className="rs-wrap" role="region" aria-label="Round summary">
      <div className="rs-title">ROUND PERFORMANCE</div>
      <div className="rs-grid">
        <div className="rs-stat">
          <span className="rs-stat-value text-gold">{dmg}</span>
          <span className="rs-stat-label">DMG DEALT</span>
        </div>
        <div className="rs-stat">
          <span className="rs-stat-value text-ally">{hs}</span>
          <span className="rs-stat-label">HEADSHOTS</span>
        </div>
        <div className="rs-stat">
          <span className="rs-stat-value text-purple">{ability}</span>
          <span className="rs-stat-label">ABILITY</span>
        </div>
        <div className="rs-stat">
          <span className="rs-stat-value text-enemy">{received}</span>
          <span className="rs-stat-label">TAKEN</span>
        </div>
      </div>
    </div>
  )
}
