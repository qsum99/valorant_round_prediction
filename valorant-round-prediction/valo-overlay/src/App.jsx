import { useState, useCallback, useRef, useEffect } from 'react'
import { useGameSocket } from './useGameSocket'
import { ProbBar }      from './ProbBar'
import { KillFeed }     from './KillFeed'
import { ScoreBar }     from './ScoreBar'
import { BuyAdvisor }   from './BuyAdvisor'
import { RoundSummary } from './RoundSummary'
import { TeamComp }     from './TeamComp'
import { PostMatchReport } from './PostMatchReport'
import './App.css'

const MAX_KILLS = 9
const INITIAL = {
  connected: false, inMatch: false, round: 0, map: '', side: '',
  scoreWon: 0, scoreLost: 0, preProb: 50, liveProb: 50,
  spikePlanted: false, spikeSite: '', spikeCarrier: '', spikeEvent: null,
  roundSummary: null, teamComp: { allies: [], enemies: [] },
  kills: [], phase: 'waiting', matchOutcome: null,
  matchReport: null, buyRecommendation: null,
  reportFile: null, reportUrl: null, showToast: false,
}

export default function App() {
  const [state, setState] = useState(INITIAL)
  const [animating, setAnim] = useState(false)
  const flashTimer = useRef(null)
  const toastTimer = useRef(null)
  const summaryTimer = useRef(null)
  const spikeTimer = useRef(null)

  const triggerFlash = () => {
    setAnim(false)
    clearTimeout(flashTimer.current)
    requestAnimationFrame(() => {
      setAnim(true)
      flashTimer.current = setTimeout(() => setAnim(false), 500)
    })
  }

  const openReport = useCallback((url, filePath) => {
    const targetUrl = url || (filePath ? `file:///${filePath.replace(/\\/g, '/')}` : '')
    if (!targetUrl) return
    try {
      if (window.overwolf?.utils?.openUrlInDefaultBrowser) {
        window.overwolf.utils.openUrlInDefaultBrowser(targetUrl)
      } else {
        window.open(targetUrl, '_blank')
      }
    } catch (e) {
      window.open(targetUrl, '_blank')
    }
  }, [])

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
        clearTimeout(summaryTimer.current)
        clearTimeout(spikeTimer.current)
        setState(s => ({
          ...s, inMatch: true, phase: 'pre_round',
          round: msg.round, map: msg.map || s.map, side: msg.side || s.side,
          scoreWon: msg.score_won ?? s.scoreWon,
          scoreLost: msg.score_lost ?? s.scoreLost,
          preProb: msg.prob, liveProb: msg.prob,
          spikePlanted: false, spikeSite: '', spikeCarrier: '', spikeEvent: null,
          roundSummary: null, kills: [],
          buyRecommendation: msg.buy_recommendation || s.buyRecommendation || null,
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
            buyRecommendation: null,  // hide buy card on combat
          }
        })
        break
      }
      case 'spike_planted':
        setState(s => ({ ...s, spikePlanted: true, spikeSite: msg.site || s.spikeSite, spikeCarrier: msg.carrier || '' }))
        break
      case 'spike_defused':
      case 'spike_detonated':
        clearTimeout(spikeTimer.current)
        setState(s => ({ ...s, spikePlanted: false, spikeEvent: { type: msg.type, site: msg.site || '' } }))
        spikeTimer.current = setTimeout(() => setState(s => ({ ...s, spikeEvent: null })), 4000)
        break
      case 'team_comp':
        setState(s => ({ ...s, teamComp: { allies: msg.allies || [], enemies: msg.enemies || [] } }))
        break
      case 'round_end':
        clearTimeout(summaryTimer.current)
        setState(s => ({
          ...s, phase: 'round_end',
          scoreWon: msg.score_won ?? s.scoreWon,
          scoreLost: msg.score_lost ?? s.scoreLost,
          roundSummary: msg.summary || null,
        }))
        if (msg.summary) {
          summaryTimer.current = setTimeout(() => setState(s => ({ ...s, roundSummary: null })), 7000)
        }
        break
      case 'match_end':
        clearTimeout(toastTimer.current)
        toastTimer.current = setTimeout(() => {
          setState(s => ({ ...s, showToast: false }))
        }, 15000)
        setState(s => ({
          ...s, inMatch: true, phase: 'match_end', matchOutcome: msg.outcome,
          scoreWon: msg.score_won ?? s.scoreWon,
          scoreLost: msg.score_lost ?? s.scoreLost,
          reportFile: msg.report_file || s.reportFile,
          reportUrl: msg.report_url || s.reportUrl,
          showToast: true,
        }))
        break
      case 'match_report':
        setState(s => ({
          ...s,
          inMatch: true,
          phase: 'match_end',
          matchReport: msg,
          reportFile: msg.report_file || s.reportFile,
          reportUrl: msg.report_url || s.reportUrl,
          showToast: true,
        }))
        break
      default: break
    }
  }, [])

  useGameSocket(onMessage)

  const { connected, inMatch, phase, round, map, side,
          scoreWon, scoreLost, preProb, liveProb,
          spikePlanted, spikeSite, spikeCarrier, spikeEvent,
          roundSummary, teamComp, kills, matchOutcome, matchReport,
          buyRecommendation, reportFile, reportUrl, showToast } = state

  const teamCompWidget = (teamComp.allies.length || teamComp.enemies.length)
    ? <TeamComp allies={teamComp.allies} enemies={teamComp.enemies} />
    : null

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
    if (matchReport) {
      return (
        <div className="overlay-root post-match-root">
          <div className="overlay-panel pmr-panel">
            <PostMatchReport report={matchReport} />
          </div>
        </div>
      )
    }

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
          round={round} map={map} side={side} spikePlanted={spikePlanted}
          spikeSite={spikeSite} spikeCarrier={spikeCarrier} actions={teamCompWidget} />
        <ProbBar pre={preProb} live={liveProb} animating={animating} />
        {spikeEvent && (
          <div className={`spike-banner ${spikeEvent.type === 'spike_detonated' ? 'det' : 'def'}`}>
            {spikeEvent.type === 'spike_detonated'
              ? `💥 SPIKE DETONATED${spikeEvent.site ? ` — ${spikeEvent.site}` : ''}`
              : `🛡️ SPIKE DEFUSED${spikeEvent.site ? ` — ${spikeEvent.site}` : ''}`}
          </div>
        )}
        {buyRecommendation && (phase === 'pre_round' || kills.length === 0) && (
          <BuyAdvisor recommendation={buyRecommendation} round={round} />
        )}
        {kills.length > 0 && (
          <div className="kf-section">
            <div className="section-label">ROUND KILLS</div>
            <KillFeed kills={kills} />
          </div>
        )}
        <RoundSummary summary={roundSummary} />
      </div>
    </div>
  )
}

