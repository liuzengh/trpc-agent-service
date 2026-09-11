export const PRESENT_CARD_TOOL_NAME = 'platform_present_card'

export function isTenantConfigurableTool(name: string) {
  return name !== PRESENT_CARD_TOOL_NAME
}
