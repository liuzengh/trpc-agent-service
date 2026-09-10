import * as Tabs from '@radix-ui/react-tabs'
import type { ReactNode } from 'react'

interface Segment<Value extends string> {
  value: Value
  label: ReactNode
  ariaLabel?: string
  disabled?: boolean
}

export function SegmentedControl<Value extends string>({
  ariaLabel,
  value,
  items,
  onValueChange,
  className = '',
}: {
  ariaLabel: string
  value: Value
  items: readonly Segment<Value>[]
  onValueChange: (value: Value) => void
  className?: string
}) {
  return (
    <Tabs.Root value={value} onValueChange={(nextValue) => onValueChange(nextValue as Value)}>
      <Tabs.List className={`segmented-control ${className}`.trim()} aria-label={ariaLabel}>
        {items.map((item) => (
          <Tabs.Trigger
            key={item.value}
            value={item.value}
            aria-label={item.ariaLabel}
            disabled={item.disabled}
          >
            {item.label}
          </Tabs.Trigger>
        ))}
      </Tabs.List>
    </Tabs.Root>
  )
}
