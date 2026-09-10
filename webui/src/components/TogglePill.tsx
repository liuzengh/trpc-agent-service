export function TogglePill({
  active,
  activeLabel,
  inactiveLabel,
  disabled = false,
  activeClassName = 'active',
  inactiveClassName = '',
  onChange,
}: {
  active: boolean
  activeLabel: string
  inactiveLabel: string
  disabled?: boolean
  activeClassName?: string
  inactiveClassName?: string
  onChange: (active: boolean) => void
}) {
  return (
    <button
      type="button"
      className={`member-status-button ${active ? activeClassName : inactiveClassName}`.trim()}
      aria-pressed={active}
      disabled={disabled}
      onClick={() => onChange(!active)}
    >
      {active ? activeLabel : inactiveLabel}
    </button>
  )
}
