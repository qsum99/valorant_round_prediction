import './BorderBeam.css'

export function BorderBeam({
  className = '',
  size = 150,
  duration = 6,
  delay = 0,
  colorFrom = '#ff4655',
  colorTo = '#00e5cc',
  borderWidth = 1.5,
}) {
  return (
    <div
      style={{
        '--size': `${size}px`,
        '--duration': `${duration}s`,
        '--delay': `-${delay}s`,
        '--color-from': colorFrom,
        '--color-to': colorTo,
        '--border-width': `${borderWidth}px`,
      }}
      className={`border-beam ${className}`}
      aria-hidden="true"
    />
  )
}
