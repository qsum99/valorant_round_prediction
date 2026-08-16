import { useState } from 'react'
import { Users, X } from 'lucide-react'
import { RankBadge } from './RankBadge'
import './TeamComp.css'

function Row({ p, side }) {
  return (
    <div className={`tc-row ${side}`}>
      <span className="tc-agent">{p.agent}</span>
      <span className="tc-name">{p.name}</span>
      <span className="tc-rank">
        <RankBadge rank={p.rank} size={18} />
        <span className="tc-rank-label">{p.rank}</span>
      </span>
    </div>
  )
}

export function TeamComp({ allies = [], enemies = [] }) {
  const [open, setOpen] = useState(false)
  if (!allies.length && !enemies.length) return null

  return (
    <div className="tc-wrap">
      <button 
        className="tc-toggle" 
        onClick={() => setOpen(o => !o)} 
        title="View team composition & ranks"
        aria-expanded={open}
        aria-controls="team-panel"
      >
        <Users size={13} className="tc-toggle-icon" />
        <span className="tc-toggle-badge">{enemies.length}</span>
      </button>

      {open && (
        <div className="tc-panel" id="team-panel" role="dialog" aria-label="Team composition">
          <div className="tc-header">
            <span className="tc-title">MATCH COMPOSITION</span>
            <button className="tc-close" onClick={() => setOpen(false)} aria-label="Close panel">
              <X size={12} />
            </button>
          </div>
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
