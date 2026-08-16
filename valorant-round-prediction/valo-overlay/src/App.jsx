import { useState, useCallback, useRef, useEffect } from 'react'
import { useGameSocket } from './useGameSocket'
import { ProbBar }      from './ProbBar'
import { KillFeed }     from './KillFeed'
import { ScoreBar }     from './ScoreBar'
import { BuyAdvisor }   from './BuyAdvisor'
import { RoundSummary } from './RoundSummary'
import { TeamComp }     from './TeamComp'
import { PostMatchReport } from './PostMatchReport'
import { Bomb, ShieldCheck, Zap, Wifi, WifiOff } from 'lucide-react'
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
  const [swapping, setSwapping] = useState(false)
  const [displaySide, setDisplaySide] = useState('')
  const flashTimer = useRef(null)
  const toastTimer = useRef(null)
  const summaryTimer = useRef(null)
  const spikeTimer = useRef(null)
  const swapTimer = useRef(null)
  const colorTimer = useRef(null)
  const prevSide = useRef('')

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
        prevSide.current = ''
        setDisplaySide('')
        setState({ ...INITIAL, connected: true, inMatch: true, phase: 'pre_round' })
        break
      case 'pre_round':
        clearTimeout(summaryTimer.current)
        clearTimeout(spikeTimer.current)
        if (msg.side && msg.side !== prevSide.current) {
          setSwapping(true)
          clearTimeout(swapTimer.current)
          clearTimeout(colorTimer.current)
          colorTimer.current = setTimeout(() => setDisplaySide(msg.side), 150)
          swapTimer.current = setTimeout(() => setSwapping(false), 650)
        }
        prevSide.current = msg.side || prevSide.current
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
            buyRecommendation: null,
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
          roundSummary, teamComp, kills, matchReport,
          buyRecommendation, reportFile, reportUrl } = state

  const teamCompWidget = (teamComp.allies.length || teamComp.enemies.length)
    ? <TeamComp allies={teamComp.allies} enemies={teamComp.enemies} />
    : null

  // Render post match report view
  const isPostMatch = phase === 'match_end'

  return (
    <>
      <div className={`overlay-root ${isPostMatch ? 'post-match-root' : ''} ${swapping ? 'flipping' : ''}`} data-side={displaySide || 'attack'}>
        {/* Disconnected / Waiting status when not in match */}
        {!connected && (
          <div className="overlay-panel status-panel">
            <WifiOff size={14} className="text-muted" />
            <span className="status-text">Connecting to backend…</span>
          </div>
        )}

        {connected && (!inMatch || phase === 'waiting') && (
          <div className="overlay-panel status-panel">
            <Wifi size={14} className="text-enemy" />
            <span className="status-text">Waiting for match to start</span>
          </div>
        )}

        {/* Post-Match Report Dashboard */}
        {isPostMatch && (
          <div className="overlay-panel pmr-panel">
            {matchReport ? (
              <PostMatchReport report={matchReport} reportUrl={reportUrl} reportFile={reportFile} onOpenReport={openReport} />
            ) : (
              <div className="end-panel">
                <span className="end-status">GENERATING REPORT…</span>
              </div>
            )}
          </div>
        )}

        {/* Live In-Match HUD Overlay */}
        {inMatch && !isPostMatch && (
          <div className="overlay-panel main-panel">
            <ScoreBar
              scoreWon={scoreWon}
              scoreLost={scoreLost}
              round={round}
              map={map}
              side={displaySide || side}
              spikePlanted={spikePlanted}
              spikeSite={spikeSite}
              spikeCarrier={spikeCarrier}
              actions={teamCompWidget}
            />

            {spikePlanted && (
              <div className={`spike-planted-banner ${side === 'defense' ? 'enemy-plant' : 'ally-plant'}`}>
                <Bomb size={14} className="spb-icon" />
                <span className="spb-text">
                  {side === 'defense' ? 'ENEMY SPIKE' : 'SPIKE PLANTED'}
                </span>
                <span className="spb-site">SITE {spikeSite || '?'}</span>
                {spikeCarrier && <span className="spb-carrier">{spikeCarrier}</span>}
              </div>
            )}

            <ProbBar pre={preProb} live={liveProb} animating={animating} />

            {spikeEvent && (
              <div className={`spike-banner ${spikeEvent.type === 'spike_detonated' ? 'det' : 'def'}`}>
                {spikeEvent.type === 'spike_detonated' ? <Zap size={13} /> : <ShieldCheck size={13} />}
                <span>
                  {spikeEvent.type === 'spike_detonated'
                    ? `SPIKE DETONATED${spikeEvent.site ? ` — ${spikeEvent.site}` : ''}`
                    : `SPIKE DEFUSED${spikeEvent.site ? ` — ${spikeEvent.site}` : ''}`}
                </span>
              </div>
            )}

            {buyRecommendation && (phase === 'pre_round' || kills.length === 0) && (
              <BuyAdvisor recommendation={buyRecommendation} round={round} />
            )}

            {kills.length > 0 && (
              <div className="kf-section">
                <div className="section-label">COMBAT EVENTS</div>
                <KillFeed kills={kills} />
              </div>
            )}

            <RoundSummary summary={roundSummary} />
          </div>
        )}
      </div>
    </>
  )
}
