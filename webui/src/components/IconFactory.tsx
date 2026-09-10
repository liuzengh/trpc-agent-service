import type { IconComponent, IconProps } from 'reicon-react/createIcon'

export function uiIcon(Icon: IconComponent) {
  return function ReiconIcon({ weight = 'Outline', ...props }: IconProps) {
    return <Icon aria-hidden="true" focusable="false" weight={weight} {...props} />
  }
}
