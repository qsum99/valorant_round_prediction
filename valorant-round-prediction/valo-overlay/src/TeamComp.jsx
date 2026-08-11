import { useState } from 'react'
import './TeamComp.css'

function Row({ p, side }) {
  return (
    <div className={`tc-row ${side}`}>
      <span className="tc-agent">{p.agent}</span>
      <span className="tc-name">{p.name}</span>
      <span className="tc-rank">{p.rank}</span>
    </div>
  )
}

export function TeamComp({ allies = [], enemies = [] }) {
  const [open, setOpen] = useState(false)
  if (!allies.length && !enemies.length) return null

  return (
    <div className="tc-wrap">
      <button className="tc-toggle" onClick={() => setOpen(o => !o)} title="Team & ranks">
        <span className="tc-toggle-icon">🏅</span>
        <span className="tc-toggle-badge">{enemies.length}</span>
      </button>
      {open && (
        <div className="tc-panel">
          <div className="tc-title">TEAM & RANKS</div>
          <div className="tc-cols">
            <div className="tc-col">
              <div className="tc-col-label ally">YOUR TEAM</div>
              {allies.map((p, i) => <Row key={`a${i}`} p={p} side="ally" />)}
            </div>
            <div className="tc-col">
              <div className="tc-col-label enemy">ENEMIES</div>
              {enemies.map((p, i) => <Row key={`e${i}`} p={p} side="enemy" />)}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
