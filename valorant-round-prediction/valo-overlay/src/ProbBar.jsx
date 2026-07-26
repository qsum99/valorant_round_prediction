import { useEffect, useRef } from 'react'
import './ProbBar.css'

export function ProbBar({ pre, live, animating }) {
  const liveRef = useRef(null)

  /* Flash effect on each kill update */
  useEffect(() => {
    if (!animating || !liveRef.current) return
    liveRef.current.classList.remove('flash')
    void liveRef.current.offsetWidth   // reflow
    liveRef.current.classList.add('flash')
  }, [live, animating])

  const allies  = Math.round(live)
  const enemies = 100 - allies
  const label   = allies >= 65 ? 'ALLIES FAVORED'
                : enemies >= 65 ? 'ENEMIES FAVORED'
                : 'EVEN MATCH'
  const labelCls = allies >= 65 ? 'label ally'
                 : enemies >= 65 ? 'label enemy'
                 : 'label neutral'

  return (
    <div className="prob-bar-wrap">
      {/* Header row */}
      <div className="pb-header">
        <span className="pb-team ally-text">YOUR TEAM</span>
        <span className={labelCls}>{label}</span>
        <span className="pb-team enemy-text">ENEMIES</span>
      </div>

      {/* Main bar */}
      <div className="pb-track">
        {/* Ally side */}
        <div
          className="pb-fill ally-fill"
          style={{ width: `${allies}%` }}
        />
        {/* Enemy side */}
        <div
          className="pb-fill enemy-fill"
          style={{ width: `${enemies}%` }}
        />
        {/* Center notch */}
        <div className="pb-center" />
      </div>

      {/* Percentage labels */}
      <div className="pb-pcts">
        <span className="pct ally-text">{allies}%</span>
        <span className="pct enemy-text">{enemies}%</span>
      </div>

      {/* Pre-round baseline marker */}
      <div className="pb-baseline-row">
        <div
          className="pb-baseline-marker"
          style={{ left: `${pre}%` }}
          title={`Pre-round: ${Math.round(pre)}%`}
        />
        <span className="pb-baseline-label" style={{ left: `${pre}%` }}>
          PRE {Math.round(pre)}%
        </span>
      </div>

      {/* Live indicator */}
      <div className="pb-live-row" ref={liveRef}>
        <div className="pb-live-dot" />
        <span className="pb-live-text">LIVE</span>
      </div>
    </div>
  )
}
