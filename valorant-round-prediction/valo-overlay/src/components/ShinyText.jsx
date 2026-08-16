import './ShinyText.css'

export function ShinyText({
  text,
  disabled = false,
  speed = 4,
  className = '',
  color = '#cbd5e1',
  shineColor = '#ffffff',
}) {
  return (
    <span
      className={`shiny-text ${disabled ? 'disabled' : ''} ${className}`}
      style={{
        '--shiny-speed': `${speed}s`,
        '--shiny-color': color,
        '--shiny-shine': shineColor,
      }}
    >
      {text}
    </span>
  )
}
