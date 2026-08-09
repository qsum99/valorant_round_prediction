import { useState, useCallback, useRef } from 'react'
import { useGameSocket } from './useGameSocket'
import { ProbBar }   from './ProbBar'
import { KillFeed }  from './KillFeed'
import { ScoreBar }  from './ScoreBar'
import './App.css'

const MAX_KILLS = 9
const INITIAL = {
  connected: false, inMatch: false, round: 0, map: '', side: '',
  scoreWon: 0, scoreLost: 0, preProb: 50, liveProb: 50,
  spikePlanted: false, kills: [], phase: 'waiting', matchOutcome: null,
}

export default function App() {
  const [state, setState] = useState(INITIAL)
  const [animating, setAnim] = useState(false)
  const flashTimer = useRef(null)

  const triggerFlash = () => {
    setAnim(false)
    clearTimeout(flashTimer.current)
    requestAnimationFrame(() => {
      setAnim(true)
      flashTimer.current = setTimeout(() => setAnim(false), 500)
    })
  }

  const onMessage = useCallback((msg) => {
    switch (msg.type) {
      case 'socket_open':
      case 'connected':
        setState(s => ({ ...s, connected: true }))
        break
      case 'socket_closed':
        setState(s => ({ ...s, connected: false }))
        break
      case 'match_start':
        setState({ ...INITIAL, connected: true, inMatch: true, phase: 'pre_round' })
        break
      case 'pre_round':
        setState(s => ({
          ...s, inMatch: true, phase: 'pre_round',
          round: msg.round, map: msg.map || s.map, side: msg.side || s.side,
          scoreWon: msg.score_won ?? s.scoreWon,
          scoreLost: msg.score_lost ?? s.scoreLost,
          preProb: msg.prob, liveProb: msg.prob,
          spikePlanted: false, kills: [],
        }))
        break
      case 'live_update': {
        const next = msg.live_prob
        triggerFlash()
        setState(s => {
          const delta = Math.round(next - s.liveProb)
          const kill = {
            attacker: msg.attacker, victim: msg.victim, headshot: msg.headshot,
            attAlive: msg.att_alive, defAlive: msg.def_alive,
            prob: Math.round(next), probDelta: delta,
            isAllyKill: msg.is_attacker_teammate ?? false,
          }
          return {
            ...s, phase: 'combat', liveProb: next,
            spikePlanted: msg.spike_planted || s.spikePlanted,
            kills: [kill, ...s.kills].slice(0, MAX_KILLS),
          }
        })
        break
      }
      case 'spike_planted':
        setState(s => ({ ...s, spikePlanted: true }))
        break
      case 'round_end':
        setState(s => ({ ...s, phase: 'round_end',
          scoreWon: msg.score_won ?? s.scoreWon,
          scoreLost: msg.score_lost ?? s.scoreLost }))
        break
      case 'match_end':
        setState(s => ({ ...s, phase: 'match_end', matchOutcome: msg.outcome,
          scoreWon: msg.score_won ?? s.scoreWon,
          scoreLost: msg.score_lost ?? s.scoreLost }))
        break
      default: break
    }
  }, [])

  useGameSocket(onMessage)

  const { connected, inMatch, phase, round, map, side,
          scoreWon, scoreLost, preProb, liveProb,
          spikePlanted, kills, matchOutcome } = state

  if (!connected) return (
    <div className="overlay-root">
      <div className="overlay-panel status-panel">
        <div className="status-dot disconnected" />
        <span className="status-text">Connecting to backend…</span>
      </div>
    </div>
  )

  if (!inMatch || phase === 'waiting') return (
    <div className="overlay-root">
      <div className="overlay-panel status-panel">
        <div className="status-dot connected" />
        <span className="status-text">Waiting for match to start</span>
      </div>
    </div>
  )

  if (phase === 'match_end') {
    const won = matchOutcome === 'victory'
    return (
      <div className="overlay-root">
        <div className={`overlay-panel end-panel ${won ? 'victory' : 'defeat'}`}>
          <span className="end-result">{won ? 'VICTORY' : 'DEFEAT'}</span>
          <span className="end-score">{scoreWon} — {scoreLost}</span>
        </div>
      </div>
    )
  }

  return (
    <div className="overlay-root">
      <div className="overlay-panel main-panel">
        <ScoreBar scoreWon={scoreWon} scoreLost={scoreLost}
          round={round} map={map} side={side} spikePlanted={spikePlanted} />
        <ProbBar pre={preProb} live={liveProb} animating={animating} />
        {kills.length > 0 && (
          <div className="kf-section">
            <div className="section-label">ROUND KILLS</div>
            <KillFeed kills={kills} />
          </div>
        )}
      </div>
    </div>
  )
}
