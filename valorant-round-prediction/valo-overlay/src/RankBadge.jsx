const TIER_COLORS = {
  Iron: '#7a8594',
  Bronze: '#c47d3b',
  Silver: '#d0d8e0',
  Gold: '#f0c040',
  Platinum: '#3de8d5',
  Diamond: '#6aa0ff',
  Ascendant: '#5ae88a',
  Immortal: '#ff5a68',
  Radiant: '#f0c040',
  Unranked: '#606a78',
}

// Hexagonal outer border (consistent across all tiers except Radiant)
const OUTER_HEX = 'M12 1.5 L22.5 7 L22.5 17 L12 22.5 L1.5 17 L1.5 7 Z'
const INNER_HEX_BORDER = 'M12 3 L21 7.8 L21 16.2 L12 21 L3 16.2 L3 7.8 Z'

// Tier-specific inner gem shapes
const GEMS = {
  Iron: 'M12 7 L16 12 L12 17 L8 12 Z',
  Bronze: 'M12 6 L17 12 L12 18 L7 12 Z',
  Silver: 'M12 6 L17 12 L12 18 L7 12 Z',
  Gold: 'M12 5.5 L17.5 12 L12 18.5 L6.5 12 Z',
  Platinum: 'M12 5.5 L17.5 12 L12 18.5 L6.5 12 Z',
  Diamond: 'M12 5.5 L17.5 12 L12 18.5 L6.5 12 Z',
  Ascendant: 'M12 5 L18 12 L12 19 L6 12 Z',
  Immortal: 'M12 5 L18 12 L12 19 L6 12 Z',
  Unranked: 'M12 7 L16 12 L12 17 L8 12 Z',
}

// Inner detail shapes (gives each tier its unique look)
const DETAILS = {
  Iron: 'M12 9 L14 12 L12 15 L10 12 Z',
  Bronze: 'M12 8 L15 12 L12 16 L9 12 Z',
  Silver: 'M12 8 L15 12 L12 16 L9 12 Z',
  Gold: 'M12 7.5 L15.5 12 L12 16.5 L8.5 12 Z',
  Platinum: 'M12 7.5 L15.5 12 L12 16.5 L8.5 12 Z',
  Diamond: 'M12 7.5 L15.5 12 L12 16.5 L8.5 12 Z',
  Ascendant: 'M12 7 L16 12 L12 17 L8 12 Z',
  Immortal: 'M12 7 L16 12 L12 17 L8 12 Z',
  Unranked: 'M12 9.5 L13.5 12 L12 14.5 L10.5 12 Z',
}

// Radiant special laurel/wreath shape
function RadiantBadge({ color }) {
  return (
    <g filter="url(#rad-glow)">
      <path d={OUTER_HEX} fill="#0d1117" stroke="#1f2a35" strokeWidth="1.5" />
      <path d={INNER_HEX_BORDER} fill="none" stroke="#2a3a45" strokeWidth="0.5" opacity="0.5" />
      {/* Laurel left */}
      <path d="M6 16 Q4 12 7 8 Q9 10 8 13 Q7 15 6 16 Z" fill={color} opacity="0.95" />
      <path d="M5 14 Q3 11 5.5 7 Q7.5 9 7 12 Q6 14 5 14 Z" fill={color} opacity="0.7" />
      {/* Laurel right */}
      <path d="M18 16 Q20 12 17 8 Q15 10 16 13 Q17 15 18 16 Z" fill={color} opacity="0.95" />
      <path d="M19 14 Q21 11 18.5 7 Q16.5 9 17 12 Q18 14 19 14 Z" fill={color} opacity="0.7" />
      {/* Center diamond */}
      <path d="M12 9 L14.5 12 L12 15 L9.5 12 Z" fill={color} />
      <path d="M12 10.5 L13.2 12 L12 13.5 L10.8 12 Z" fill="#fff" opacity="0.7" />
    </g>
  )
}

export function RankBadge({ rank, size = 20 }) {
  const s = String(rank || '')
  const parts = s.split(' ')
  const tier = parts[0]
  const color = TIER_COLORS[tier] || TIER_COLORS.Unranked
  const isRadiant = tier === 'Radiant'

  if (isRadiant) {
    return (
      <svg className="rank-badge" width={size} height={size} viewBox="0 0 24 24" aria-hidden="true" role="img" aria-label={`Rank: ${rank}`}>
        <defs>
          <filter id="rad-glow" x="-30%" y="-30%" width="160%" height="160%">
            <feGaussianBlur in="SourceGraphic" stdDeviation="1.5" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>
        <RadiantBadge color={color} />
      </svg>
    )
  }

  const gemPath = GEMS[tier] || GEMS.Unranked
  const detailPath = DETAILS[tier] || DETAILS.Unranked
  const filterId = `glow-${tier}`
  const gradId = `grad-${tier}`

  return (
    <svg className="rank-badge" width={size} height={size} viewBox="0 0 24 24" aria-hidden="true" role="img" aria-label={`Rank: ${rank}`}>
      <defs>
        <filter id={filterId} x="-20%" y="-20%" width="140%" height="140%">
          <feGaussianBlur in="SourceGraphic" stdDeviation="1.2" result="blur" />
          <feMerge>
            <feMergeNode in="blur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
        <radialGradient id={gradId} cx="40%" cy="40%" r="60%">
          <stop offset="0%" stopColor={color} stopOpacity="1" />
          <stop offset="100%" stopColor={color} stopOpacity="0.5" />
        </radialGradient>
      </defs>

      {/* Outer hexagonal border */}
      <path d={OUTER_HEX} fill="#0d1117" stroke="#1f2a35" strokeWidth="1.5" />
      <path d={INNER_HEX_BORDER} fill="none" stroke="#2a3a45" strokeWidth="0.5" opacity="0.4" />

      {/* Inner gem with glow */}
      <path d={gemPath} fill={color} filter={`url(#${filterId})`} />
      <path d={gemPath} fill={`url(#${gradId})`} />

      {/* Detail overlay */}
      <path d={detailPath} fill="#fff" opacity="0.3" />
    </svg>
  )
}