import { useEffect, useRef, type RefObject } from 'react'

export function useDismissibleLayer<T extends HTMLElement>({
  enabled = true,
  onDismiss,
  restoreFocus,
}: {
  enabled?: boolean
  onDismiss: (layer: T) => void
  restoreFocus?: (layer: T) => void
}): RefObject<T> {
  const layerRef = useRef<T>(null)
  const onDismissRef = useRef(onDismiss)
  const restoreFocusRef = useRef(restoreFocus)

  useEffect(() => {
    onDismissRef.current = onDismiss
    restoreFocusRef.current = restoreFocus
  }, [onDismiss, restoreFocus])

  useEffect(() => {
    if (!enabled) return
    const onPointerDown = (event: PointerEvent) => {
      const layer = layerRef.current
      if (layer && !layer.contains(event.target as Node)) onDismissRef.current(layer)
    }
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return
      const layer = layerRef.current
      if (!layer) return
      onDismissRef.current(layer)
      restoreFocusRef.current?.(layer)
    }
    document.addEventListener('pointerdown', onPointerDown)
    document.addEventListener('keydown', onKeyDown)
    return () => {
      document.removeEventListener('pointerdown', onPointerDown)
      document.removeEventListener('keydown', onKeyDown)
    }
  }, [enabled])

  return layerRef
}
