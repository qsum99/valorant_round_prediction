import { Shield, Swords, Bomb } from 'lucide-react'
import './ScoreBar.css'

export function ScoreBar({ 
  scoreWon, 
  scoreLost, 
  round, 
  map, 
  side, 
  spikePlanted, 
  spikeSite, 
  spikeCarrier, 
  actions 
}) {
  const isAttack = side === 'attack'

  return (
    <div className="score-bar">
      <div className="sb-left">
        <span className="sb-score ally-text" title="Your team rounds">{scoreWon}</span>
        <span className="sb-divider" aria-hidden="true">:</span>
        <span className="sb-score enemy-text" title="Enemy rounds">{scoreLost}</span>
      </div>

      <div className="sb-center">
        {spikePlanted ? (
          <div className="sb-spike" role="status" aria-live="polite">
            <Bomb size={13} className="sb-spike-icon" />
            <span className="sb-spike-text">SITE {spikeSite || '?'}</span>
            {spikeCarrier && <span className="sb-spike-carrier">({spikeCarrier})</span>}
          </div>
        ) : map ? (
          <div className="sb-map-pill">
            <span className="sb-map-name">{map.toUpperCase()}</span>
          </div>
        ) : null}
      </div>

      <div className="sb-right">
        {actions}
        <div className={`sb-side-pill ${isAttack ? 'atk' : 'def'}`}>
          {isAttack ? <Swords size={11} /> : <Shield size={11} />}
          <span>{isAttack ? 'ATK' : 'DEF'}</span>
        </div>
        <div className="sb-round-pill">
          <span>R{round}</span>
        </div>
      </div>
    </div>
  )
}
