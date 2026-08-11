const TIER_COLORS = {
  Iron: '#6b7684',
  Bronze: '#b0692f',
  Silver: '#b7c3cf',
  Gold: '#e6bd5c',
  Platinum: '#31c8b5',
  Diamond: '#5a8df7',
  Ascendant: '#4fd97e',
  Immortal: '#f0505e',
  Radiant: '#e8c468',
  Unranked: '#7a8694',
}

// Upward-pointing chevron (Valorant-style emblem)
const CHEVRON = 'M12 9.5 L22 17 L18.5 20 L12 15.2 L5.5 20 L2 17 Z'

function pipPath(i) {
  const y = 2 + i * 5.5
  return `M12 ${y} L14.2 ${y + 5} L9.8 ${y + 5} Z`
}

export function RankBadge({ rank, size = 15 }) {
  const s = String(rank || '')
  const parts = s.split(' ')
  const tier = parts[0]
  const color = TIER_COLORS[tier] || TIER_COLORS.Unranked
  const division = Number(parts[1]) || 0
  const pips = Math.max(0, Math.min(2, division - 1))

  if (tier === 'Radiant') {
    return (
      <svg className="rank-badge" width={size} height={size} viewBox="0 0 24 24" aria-hidden="true">
        <polygon points="12,1.5 21.5,12 12,22.5 2.5,12" fill={color} opacity="0.35" />
        <path d={CHEVRON} fill={color} />
      </svg>
    )
  }

  return (
    <svg className="rank-badge" width={size} height={size} viewBox="0 0 24 24" aria-hidden="true">
      {pips > 0 && (
        <g fill={color} opacity="0.9">
          {Array.from({ length: pips }, (_, i) => (
            <path key={i} d={pipPath(i)} />
          ))}
        </g>
      )}
      <path d={CHEVRON} fill={color} />
    </svg>
  )
}
