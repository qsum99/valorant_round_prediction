import './KillFeed.css'

export function KillFeed({ kills }) {
  if (!kills.length) return null
  return (
    <div className="kf-wrap">
      {kills.map((k, i) => (
        <div
          key={i}
          className={`kf-row ${k.isAllyKill ? 'ally-kill' : 'enemy-kill'} ${i === 0 ? 'newest' : ''}`}
        >
          <span className="kf-attacker">{k.attacker}</span>
          <span className="kf-icon">{k.headshot ? '💥' : '⚔️'}</span>
          <span className="kf-victim">{k.victim}</span>
          <span className="kf-alive">{k.attAlive}v{k.defAlive}</span>
          <span className={`kf-prob ${k.probDelta > 0 ? 'up' : k.probDelta < 0 ? 'down' : ''}`}>
            {k.prob}%
            {k.probDelta !== 0 && (
              <span className="kf-delta">
                {k.probDelta > 0 ? `+${k.probDelta}` : k.probDelta}
              </span>
            )}
          </span>
        </div>
      ))}
    </div>
  )
}
