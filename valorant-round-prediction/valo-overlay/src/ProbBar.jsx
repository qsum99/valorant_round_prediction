import { useEffect, useRef } from 'react'
import { Activity } from 'lucide-react'
import './ProbBar.css'

export function ProbBar({ pre, live, animating }) {
  const trackRef = useRef(null)

  useEffect(() => {
    if (!animating || !trackRef.current) return
    trackRef.current.classList.remove('pulse-flash')
    void trackRef.current.offsetWidth
    trackRef.current.classList.add('pulse-flash')
  }, [live, animating])

  const allies = Math.round(live)
  const enemies = 100 - allies

  const label = allies >= 65 ? 'ALLIES FAVORED'
              : enemies >= 65 ? 'ENEMIES FAVORED'
              : 'EVEN MATCH'

  const labelCls = allies >= 65 ? 'label ally'
                 : enemies >= 65 ? 'label enemy'
                 : 'label neutral'

  return (
    <div className="prob-bar-wrap" role="img" aria-label={`Win probability: Allies ${allies}%, Enemies ${enemies}%`}>
      {/* Top Header */}
      <div className="pb-header">
        <span className="pb-team ally-text">ALLIES</span>
        <span className={labelCls}>{label}</span>
        <span className="pb-team enemy-text">ENEMIES</span>
      </div>

      {/* Main Track */}
      <div className="pb-track" ref={trackRef}>
        <div
          className="pb-fill ally-fill"
          style={{ width: `${allies}%` }}
          role="progressbar"
          aria-valuenow={allies}
          aria-valuemin={0}
          aria-valuemax={100}
        />
        <div
          className="pb-fill enemy-fill"
          style={{ width: `${enemies}%` }}
          role="progressbar"
          aria-valuenow={enemies}
          aria-valuemin={0}
          aria-valuemax={100}
        />
        <div className="pb-center-line" aria-hidden="true" />
      </div>

      {/* Probability Numbers */}
      <div className="pb-pcts">
        <span className="pct ally-text">{allies}%</span>
        <span className="pct enemy-text">{enemies}%</span>
      </div>

      {/* Pre-Round Baseline */}
      <div className="pb-baseline-row">
        <div
          className="pb-baseline-marker"
          style={{ left: `${pre}%` }}
          title={`Pre-round odds: ${Math.round(pre)}%`}
          aria-hidden="true"
        />
        <span className="pb-baseline-label" style={{ left: `${pre}%` }}>
          PRE-ROUND {Math.round(pre)}%
        </span>
      </div>

      {/* Live Activity Sensor */}
      <div className="pb-live-row">
        <div className="pb-live-dot" aria-hidden="true" />
        <span className="pb-live-text">LIVE MODEL B ENGINE</span>
      </div>
    </div>
  )
}
