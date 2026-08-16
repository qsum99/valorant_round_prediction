import { useState, useCallback, useRef, useEffect } from 'react'
import { useGameSocket } from './useGameSocket'
import { ProbBar }      from './ProbBar'
import { KillFeed }     from './KillFeed'
import { ScoreBar }     from './ScoreBar'
import { BuyAdvisor }   from './BuyAdvisor'
import { RoundSummary } from './RoundSummary'
import { TeamComp }     from './TeamComp'
import { PostMatchReport } from './PostMatchReport'
import { Bomb, ShieldCheck, Zap, Eye, Wifi, WifiOff } from 'lucide-react'
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

// Rich mock data for previewing in dev mode
const MOCK_REPORT = {
  map: 'Ascent',
  outcome: 'victory',
  final_score: [13, 11],
  local_agent: 'Jett',
  date: '2026-08-16',
  model_accuracy: 0.88,
  max_streak: 5,
  biggest_upset: { round: 14, pre_prob: 28.4, swing: 71.6 },
  economy: {
    pistol:   { played: 2, won: 2 },
    eco:      { played: 3, won: 1 },
    force:    { played: 4, won: 3 },
    half_buy: { played: 2, won: 1 },
    full_buy: { played: 11, won: 6 },
    bonus:    { played: 2, won: 0 },
  },
  pivotal_rounds: [
    { round: 14, won: true, swing: 72, pre_prob: 28, reason: '4v2 retake on A-site with Jett 3K entry.' },
    { round: 19, won: true, swing: 58, pre_prob: 34, reason: 'Eco thrifty win against full buy.' },
    { round: 22, won: false, swing: 44, pre_prob: 68, reason: 'Lost 2v1 post-plant defuse to enemy Sova.' },
  ],
  team_comp: {
    allies: [
      { name: 'RadiantAce', agent: 'Jett', rank: 'Ascendant 2' },
      { name: 'ViperMain', agent: 'Viper', rank: 'Ascendant 1' },
      { name: 'SovaDarts', agent: 'Sova', rank: 'Diamond 3' },
      { name: 'FlashGod', agent: 'KAY/O', rank: 'Ascendant 2' },
      { name: 'SmokeKing', agent: 'Omen', rank: 'Diamond 2' },
    ],
    enemies: [
      { name: 'ShadowOp', agent: 'Reyna', rank: 'Ascendant 3' },
      { name: 'TrapQueen', agent: 'Killjoy', rank: 'Ascendant 1' },
      { name: 'BlindFury', agent: 'Breach', rank: 'Diamond 3' },
      { name: 'ArrowSniper', agent: 'Sova', rank: 'Ascendant 2' },
      { name: 'SpectreGod', agent: 'Fade', rank: 'Diamond 3' },
    ],
  },
  rounds: Array.from({ length: 24 }, (_, idx) => {
    const round = idx + 1
    const isWon = [1, 2, 4, 7, 8, 9, 10, 11, 14, 15, 18, 19, 24].includes(round)
    const side = round <= 12 ? 'attack' : 'defense'
    const pre_prob = round === 14 ? 28.4 : Math.round((45 + (Math.sin(round) * 25)) * 10) / 10
    const performance = round === 14 ? 'clutch' : (round === 22 ? 'choke' : 'expected')
    const buy_type = [1, 13].includes(round) ? 'pistol' : (round === 2 ? 'force' : (round === 3 ? 'eco' : 'full_buy'))
    return {
      round,
      won: isWon,
      side,
      pre_prob,
      final_prob: isWon ? 100 : 0,
      prob_swing: Math.round((isWon ? 100 - pre_prob : -pre_prob)),
      performance,
      buy_type,
      buy_recommendation: buy_type,
      player_kills: isWon ? Math.floor(Math.random() * 3) + 1 : Math.floor(Math.random() * 2),
      player_deaths: isWon ? (Math.random() > 0.6 ? 1 : 0) : 1,
      round_report: { damage: Math.round(80 + Math.random() * 220) },
      kills: [
        { attacker: 'RadiantAce', victim: 'ShadowOp', headshot: true, att_alive: 5, def_alive: 4, live_prob: 62, is_attacker_teammate: true },
        { attacker: 'TrapQueen', victim: 'SmokeKing', headshot: false, att_alive: 4, def_alive: 4, live_prob: 48, is_attacker_teammate: false },
        { attacker: 'RadiantAce', victim: 'TrapQueen', headshot: true, att_alive: 4, def_alive: 3, live_prob: 74, is_attacker_teammate: true },
      ],
    }
  }),
}

const MOCK_HUD = {
  connected: true,
  inMatch: true,
  phase: 'combat',
  round: 14,
  map: 'Ascent',
  side: 'defense',
  scoreWon: 8,
  scoreLost: 5,
  preProb: 42,
  liveProb: 68,
  spikePlanted: false,
  spikeSite: 'A',
  spikeCarrier: 'ShadowOp',
  spikeEvent: null,
  teamComp: MOCK_REPORT.team_comp,
  kills: [
    { attacker: 'RadiantAce', victim: 'ShadowOp', headshot: true, attAlive: 4, defAlive: 2, prob: 68, probDelta: 16, isAllyKill: true },
    { attacker: 'TrapQueen', victim: 'SmokeKing', headshot: false, attAlive: 4, defAlive: 3, prob: 52, probDelta: -12, isAllyKill: false },
    { attacker: 'ViperMain', victim: 'BlindFury', headshot: false, attAlive: 5, defAlive: 3, prob: 64, probDelta: 14, isAllyKill: true },
  ],
  buyRecommendation: {
    recommendation: 'full_buy',
    urgency: 'high',
    reason: 'Full buy available. Enemies likely on force buy; armor + Vandal advantage is decisive.',
    plan: 'Play standard defaults and deny A-main orb control.',
    ally_money: 19400,
    enemy_money: 11200,
    enemy_buy: 'force',
    scenarios: {
      full_buy: { this_round: 68, two_round_ev: 54 },
      half_buy: { this_round: 45, two_round_ev: 48 },
      eco:      { this_round: 22, two_round_ev: 41 },
    },
  },
  roundSummary: { damage: 160, headshot: 2, final_headshot: 1, ability_damage: 40, damage_received: 75 },
}

export default function App() {
  const [state, setState] = useState(INITIAL)
  const [animating, setAnim] = useState(false)
  const [demoMode, setDemoMode] = useState(null) // null | 'hud_combat' | 'hud_shopping' | 'spike_planted' | 'post_match'
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

  // Active state (real vs demo)
  const activeState = demoMode ? (
    demoMode === 'post_match'
      ? { ...INITIAL, connected: true, inMatch: true, phase: 'match_end', matchOutcome: 'victory', matchReport: MOCK_REPORT }
      : demoMode === 'hud_shopping'
      ? { ...MOCK_HUD, phase: 'pre_round', kills: [], spikePlanted: false }
      : demoMode === 'spike_planted'
      ? { ...MOCK_HUD, spikePlanted: true, spikeSite: 'A', spikeCarrier: 'ShadowOp' }
      : MOCK_HUD
  ) : state

  const { connected, inMatch, phase, round, map, side,
          scoreWon, scoreLost, preProb, liveProb,
          spikePlanted, spikeSite, spikeCarrier, spikeEvent,
          roundSummary, teamComp, kills, matchOutcome, matchReport,
          buyRecommendation, reportFile, reportUrl } = activeState

  const teamCompWidget = (teamComp.allies.length || teamComp.enemies.length)
    ? <TeamComp allies={teamComp.allies} enemies={teamComp.enemies} />
    : null

  // Render post match report view
  const isPostMatch = phase === 'match_end'

  return (
    <>
      <div className={`overlay-root ${isPostMatch ? 'post-match-root' : ''}`}>
        {/* Disconnected / Waiting status when not in match */}
        {!connected && !demoMode && (
          <div className="overlay-panel status-panel">
            <WifiOff size={14} className="text-muted" />
            <span className="status-text">Connecting to backend…</span>
          </div>
        )}

        {connected && (!inMatch || phase === 'waiting') && !demoMode && (
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
              <div className={`end-panel ${matchOutcome === 'victory' ? 'victory' : 'defeat'}`}>
                <span className="end-result">{matchOutcome === 'victory' ? 'VICTORY' : 'DEFEAT'}</span>
                <span className="end-score">{scoreWon} — {scoreLost}</span>
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
              side={side}
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

      {/* Dev / Preview Controls Floating Bar */}
      <div className="dev-preview-bar" role="toolbar" aria-label="UI Preview Switcher">
        <span className="dev-preview-label">
          <Eye size={12} /> PREVIEW:
        </span>
        <button
          type="button"
          className={`dev-btn ${demoMode === null ? 'active' : ''}`}
          onClick={() => setDemoMode(null)}
          title="Listen to live backend WebSocket events"
        >
          LIVE {connected ? '🟢' : '⚪'}
        </button>
        <button
          type="button"
          className={`dev-btn ${demoMode === 'hud_combat' ? 'active' : ''}`}
          onClick={() => setDemoMode('hud_combat')}
        >
          HUD Combat
        </button>
        <button
          type="button"
          className={`dev-btn ${demoMode === 'hud_shopping' ? 'active' : ''}`}
          onClick={() => setDemoMode('hud_shopping')}
        >
          HUD Shopping
        </button>
        <button
          type="button"
          className={`dev-btn ${demoMode === 'spike_planted' ? 'active' : ''}`}
          onClick={() => setDemoMode('spike_planted')}
        >
          HUD Spike
        </button>
        <button
          type="button"
          className={`dev-btn ${demoMode === 'post_match' ? 'active' : ''}`}
          onClick={() => setDemoMode('post_match')}
        >
          Post-Match Report
        </button>
      </div>
    </>
  )
}
