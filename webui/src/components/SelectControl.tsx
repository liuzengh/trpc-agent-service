import * as Select from '@radix-ui/react-select'
import { Check } from 'reicon-react/icons/Check'
import { ChevronDown } from 'reicon-react/icons/ChevronDown'
import { ChevronUp } from 'reicon-react/icons/ChevronUp'
import type { ReactNode } from 'react'

export interface SelectOption {
  value: string
  label: ReactNode
  disabled?: boolean
  leading?: ReactNode
}

export interface SelectOptionGroup {
  id: string
  label: ReactNode
  options: SelectOption[]
}

interface SelectControlProps {
  value: string
  onValueChange: (value: string) => void
  options?: SelectOption[]
  groups?: SelectOptionGroup[]
  leading?: ReactNode
  valueLabel?: ReactNode
  shellClassName?: string
  disabled?: boolean
  id?: string
  name?: string
  ariaLabel?: string
  placeholder?: string
}

export function SelectControl({
  value,
  onValueChange,
  options = [],
  groups = [],
  leading,
  valueLabel,
  shellClassName = '',
  disabled = false,
  id,
  name,
  ariaLabel,
  placeholder = '请选择',
}: SelectControlProps) {
  return (
    <Select.Root value={value} onValueChange={onValueChange} disabled={disabled} name={name}>
      <Select.Trigger
        id={id}
        aria-label={ariaLabel}
        className={`select-control ${leading ? 'has-leading' : ''} ${shellClassName}`.trim()}
      >
        {leading && <span className="select-control-leading" aria-hidden="true">{leading}</span>}
        <span className="select-control-value">
          <Select.Value placeholder={placeholder}>{valueLabel}</Select.Value>
        </span>
        <Select.Icon className="select-control-icon">
          <ChevronDown size={15} />
        </Select.Icon>
      </Select.Trigger>

      <Select.Portal>
        <Select.Content
          className="select-content"
          position="popper"
          sideOffset={6}
          collisionPadding={10}
        >
          <Select.ScrollUpButton className="select-scroll-button" aria-hidden="true">
            <ChevronUp size={15} />
          </Select.ScrollUpButton>
          <Select.Viewport className="select-viewport">
            {groups.map((group) => (
              <Select.Group key={group.id}>
                <Select.Label className="select-group-label">{group.label}</Select.Label>
                {group.options.map((option) => <SelectOptionItem key={option.value} option={option} />)}
              </Select.Group>
            ))}
            {groups.length > 0 && options.length > 0 && <Select.Separator className="select-separator" />}
            {options.map((option) => <SelectOptionItem key={option.value} option={option} />)}
          </Select.Viewport>
          <Select.ScrollDownButton className="select-scroll-button" aria-hidden="true">
            <ChevronDown size={15} />
          </Select.ScrollDownButton>
        </Select.Content>
      </Select.Portal>
    </Select.Root>
  )
}

function SelectOptionItem({ option }: { option: SelectOption }) {
  return (
    <Select.Item className="select-item" value={option.value} disabled={option.disabled}>
      {option.leading && <span className="select-item-leading" aria-hidden="true">{option.leading}</span>}
      <Select.ItemText>{option.label}</Select.ItemText>
      <Select.ItemIndicator className="select-item-indicator">
        <Check size={15} />
      </Select.ItemIndicator>
    </Select.Item>
  )
}
