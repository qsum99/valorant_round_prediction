import './ScoreBar.css'

export function ScoreBar({ scoreWon, scoreLost, round, map, side, spikePlanted }) {
  return (
    <div className="score-bar">
      <div className="sb-left">
        <span className="sb-score ally-text">{scoreWon}</span>
        <span className="sb-divider">:</span>
        <span className="sb-score enemy-text">{scoreLost}</span>
      </div>
      <div className="sb-center">
        {spikePlanted && <span className="sb-spike">💣 SPIKE PLANTED</span>}
        {!spikePlanted && map && (
          <span className="sb-map">{map.toUpperCase()}</span>
        )}
      </div>
      <div className="sb-right">
        <span className={`sb-side ${side === 'attack' ? 'atk' : 'def'}`}>
          {side === 'attack' ? 'ATK' : 'DEF'}
        </span>
        <span className="sb-round">RND {round}</span>
      </div>
    </div>
  )
}
