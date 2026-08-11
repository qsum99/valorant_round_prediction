import './ScoreBar.css'

export function ScoreBar({ scoreWon, scoreLost, round, map, side, spikePlanted, spikeSite, spikeCarrier, actions }) {
  return (
    <div className="score-bar">
      <div className="sb-left">
        <span className="sb-score ally-text">{scoreWon}</span>
        <span className="sb-divider">:</span>
        <span className="sb-score enemy-text">{scoreLost}</span>
      </div>
      <div className="sb-center">
        {spikePlanted && (
          <span className="sb-spike">
            💣 SPIKE AT {spikeSite || '?'}
            {spikeCarrier && <span className="sb-spike-carrier"> — {spikeCarrier}</span>}
          </span>
        )}
        {!spikePlanted && map && (
          <span className="sb-map">{map.toUpperCase()}</span>
        )}
      </div>
      <div className="sb-right">
        {actions}
        <span className={`sb-side ${side === 'attack' ? 'atk' : 'def'}`}>
          {side === 'attack' ? 'ATK' : 'DEF'}
        </span>
        <span className="sb-round">RND {round}</span>
      </div>
    </div>
  )
}
