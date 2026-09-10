import { SearchIcon } from './PageIcons'

export function SearchField({
  value,
  onValueChange,
  placeholder = '搜索…',
  ariaLabel = '搜索',
  onSubmit,
  onClear,
  disabled = false,
  className = '',
}: {
  value: string
  onValueChange: (value: string) => void
  placeholder?: string
  ariaLabel?: string
  onSubmit?: () => void
  onClear?: () => void
  disabled?: boolean
  className?: string
}) {
  const field = (
    <div className="search-field">
      <SearchIcon size={15} />
      <input
        type="search"
        aria-label={ariaLabel}
        placeholder={placeholder}
        value={value}
        disabled={disabled}
        onChange={(event) => onValueChange(event.target.value)}
      />
      {value && onClear && (
        <button type="button" className="search-field-clear" disabled={disabled} onClick={onClear} aria-label={`清空${ariaLabel}`}>
          清空
        </button>
      )}
    </div>
  )

  if (!onSubmit) return <div className={`search-field-wrap${className ? ` ${className}` : ''}`}>{field}</div>

  return (
    <form
      className={`search-field-form${className ? ` ${className}` : ''}`}
      onSubmit={(event) => {
        event.preventDefault()
        onSubmit()
      }}
    >
      {field}
    </form>
  )
}
