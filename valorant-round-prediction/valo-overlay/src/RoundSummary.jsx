import './RoundSummary.css'

export function RoundSummary({ summary }) {
  if (!summary) return null
  const dmg = Math.round(summary.damage || 0)
  const hs = Math.round((summary.headshot || 0) + (summary.final_headshot || 0))
  const ability = Math.round(summary.ability_damage || 0)
  const received = Math.round(summary.damage_received || 0)

  return (
    <div className="rs-wrap">
      <div className="rs-title">ROUND SUMMARY</div>
      <div className="rs-row">
        <span className="rs-stat"><b>{dmg}</b> dmg</span>
        <span className="rs-sep">•</span>
        <span className="rs-stat"><b>{hs}</b> HS</span>
        <span className="rs-sep">•</span>
        <span className="rs-stat"><b>{ability}</b> ability</span>
        <span className="rs-sep">•</span>
        <span className="rs-stat"><b>{received}</b> taken</span>
      </div>
    </div>
  )
}
