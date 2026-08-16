import { Crosshair, Swords } from 'lucide-react'
import './KillFeed.css'

export function KillFeed({ kills }) {
  if (!kills || !kills.length) return null

  return (
    <div className="kf-wrap" role="list" aria-label="Recent kills">
      {kills.map((k, i) => (
        <div
          key={i}
          className={`kf-row ${k.isAllyKill ? 'ally-kill' : 'enemy-kill'} ${i === 0 ? 'newest' : ''}`}
          role="listitem"
        >
          <span className="kf-attacker" title={k.attacker}>{k.attacker}</span>
          <span className="kf-icon" title={k.headshot ? 'Headshot' : 'Kill'}>
            {k.headshot ? <Crosshair size={11} className="text-gold" /> : <Swords size={11} />}
          </span>
          <span className="kf-victim" title={k.victim}>{k.victim}</span>
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
