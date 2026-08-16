import { useRef } from 'react'
import './SpotlightCard.css'

export function SpotlightCard({ 
  children, 
  className = '', 
  spotlightColor = 'rgba(0, 229, 204, 0.12)',
  onClick,
  style = {}
}) {
  const cardRef = useRef(null)

  const handleMouseMove = (e) => {
    if (!cardRef.current) return
    const rect = cardRef.current.getBoundingClientRect()
    const x = e.clientX - rect.left
    const y = e.clientY - rect.top
    cardRef.current.style.setProperty('--mouse-x', `${x}px`)
    cardRef.current.style.setProperty('--mouse-y', `${y}px`)
  }

  return (
    <div
      ref={cardRef}
      onMouseMove={handleMouseMove}
      onClick={onClick}
      className={`spotlight-card ${className}`}
      style={{
        '--spotlight-color': spotlightColor,
        ...style
      }}
    >
      <div className="spotlight-overlay" aria-hidden="true" />
      <div className="spotlight-content">
        {children}
      </div>
    </div>
  )
}
