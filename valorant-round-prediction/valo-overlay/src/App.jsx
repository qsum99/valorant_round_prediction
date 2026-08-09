import { useState, useCallback, useRef, useEffect } from 'react'
import { useGameSocket } from './useGameSocket'
import { ProbBar }      from './ProbBar'
import { KillFeed }     from './KillFeed'
import { ScoreBar }     from './ScoreBar'
import { BuyAdvisor }   from './BuyAdvisor'
import { PostMatchReport } from './PostMatchReport'
import './App.css'

const MAX_KILLS = 9
const INITIAL = {
  connected: false, inMatch: false, round: 0, map: '', side: '',
  scoreWon: 0, scoreLost: 0, preProb: 50, liveProb: 50,
  spikePlanted: false, kills: [], phase: 'waiting', matchOutcome: null,
  matchReport: null, buyRecommendation: null,
  reportFile: null, reportUrl: null, showToast: false,
}

export default function App() {
  const [state, setState] = useState(INITIAL)
  const [animating, setAnim] = useState(false)
  const flashTimer = useRef(null)
  const toastTimer = useRef(null)

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
        setState(s => ({
          ...s, inMatch: true, phase: 'pre_round',
          round: msg.round, map: msg.map || s.map, side: msg.side || s.side,
          scoreWon: msg.score_won ?? s.scoreWon,
          scoreLost: msg.score_lost ?? s.scoreLost,
          preProb: msg.prob, liveProb: msg.prob,
          spikePlanted: false, kills: [],
          buyRecommendation: msg.buy_recommendation || null,
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
        setState(s => ({ ...s, spikePlanted: true }))
        break
      case 'round_end':
        setState(s => ({ ...s, phase: 'round_end',
          scoreWon: msg.score_won ?? s.scoreWon,
          scoreLost: msg.score_lost ?? s.scoreLost }))
        break
      case 'match_end':
        clearTimeout(toastTimer.current)
        toastTimer.current = setTimeout(() => {
          setState(s => ({ ...s, showToast: false }))
        }, 10000)
        setState(s => ({
          ...s, phase: 'match_end', matchOutcome: msg.outcome,
          scoreWon: msg.score_won ?? s.scoreWon,
          scoreLost: msg.score_lost ?? s.scoreLost,
          reportFile: msg.report_file || s.reportFile,
          reportUrl: msg.report_url || s.reportUrl,
          showToast: true,
        }))
        break
      case 'match_report':
        setState(s => ({ ...s, phase: 'match_end', matchReport: msg }))
        break
      default: break
    }
  }, [])

  useGameSocket(onMessage)

  const { connected, inMatch, phase, round, map, side,
          scoreWon, scoreLost, preProb, liveProb,
          spikePlanted, kills, matchOutcome, matchReport,
          buyRecommendation, reportFile, reportUrl, showToast } = state

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
          {showToast && (reportUrl || reportFile) && (
            <div className="report-toast">
              <div className="toast-content">
                <span className="toast-icon">📊</span>
                <div className="toast-text">
                  <div className="toast-title">Interactive Report Saved</div>
                  <div className="toast-sub">Self-contained dashboard with round-by-round charts</div>
                </div>
              </div>
              <div className="toast-actions">
                <button
                  className="toast-btn"
                  onClick={() => openReport(reportUrl, reportFile)}
                >
                  OPEN REPORT ↗
                </button>
                <button
                  className="toast-close"
                  onClick={() => setState(s => ({ ...s, showToast: false }))}
                >
                  ✕
                </button>
              </div>
            </div>
          )}
          <div className="overlay-panel pmr-panel">
            <PostMatchReport
              report={matchReport}
              reportUrl={reportUrl}
              reportFile={reportFile}
              onOpenReport={openReport}
            />
          </div>
        </div>
      )
    }

    const won = matchOutcome === 'victory'
    return (
      <div className="overlay-root">
        {showToast && (reportUrl || reportFile) && (
          <div className="report-toast" style={{ pointerEvents: 'auto', marginBottom: 10 }}>
            <div className="toast-content">
              <span className="toast-icon">📊</span>
              <div className="toast-text">
                <div className="toast-title">Post-Match Report Ready</div>
              </div>
            </div>
            <button
              className="toast-btn"
              onClick={() => openReport(reportUrl, reportFile)}
            >
              OPEN ↗
            </button>
          </div>
        )}
        <div className={`overlay-panel end-panel ${won ? 'victory' : 'defeat'}`}>
          <span className="end-result">{won ? 'VICTORY' : 'DEFEAT'}</span>
          <span className="end-score">{scoreWon} — {scoreLost}</span>
          {(reportUrl || reportFile) && (
            <button
              className="pmr-open-report-btn"
              style={{ pointerEvents: 'auto', marginTop: 12 }}
              onClick={() => openReport(reportUrl, reportFile)}
            >
              📊 VIEW FULL REPORT
            </button>
          )}
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
        {phase === 'pre_round' && buyRecommendation && (
          <BuyAdvisor recommendation={buyRecommendation} />
        )}
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

