"use client";

import { useEffect, useMemo, useRef, useState, type CSSProperties, type KeyboardEvent } from "react";

import { getNodeReferences, nodeIDFromDiagnostic, type AgentSpecDiagnostic } from "../../lib/agent-spec-v1";
import type { AgentEditorAction, AgentEditorState, CanvasPoint } from "../../lib/agent-editor-state";

const NODE_WIDTH = 184;
const NODE_HEIGHT = 88;

interface DragState {
  nodeID: string;
  pointerX: number;
  pointerY: number;
  origin: CanvasPoint;
}

export function AgentCanvas({
  state,
  diagnostics,
  disabled,
  onAction,
}: {
  state: AgentEditorState;
  diagnostics: readonly AgentSpecDiagnostic[];
  disabled?: boolean;
  onAction: (action: AgentEditorAction) => void;
}) {
  const [drag, setDrag] = useState<DragState | null>(null);
  const nodeElements = useRef(new Map<string, HTMLElement>());
  const diagnosticNodeIDs = useMemo(
    () => new Set(diagnostics.map(nodeIDFromDiagnostic).filter((value): value is string => Boolean(value))),
    [diagnostics],
  );
  const dimensions = useMemo(() => {
    const points = Object.values(state.positions);
    return {
      width: Math.max(760, ...points.map((point) => point.x + NODE_WIDTH + 40)),
      height: Math.max(430, ...points.map((point) => point.y + NODE_HEIGHT + 60)),
    };
  }, [state.positions]);

  useEffect(() => {
    if (drag || !state.selectedNodeID) return;
    nodeElements.current.get(state.selectedNodeID)?.scrollIntoView?.({ block: "nearest", inline: "nearest" });
  }, [drag, state.selectedNodeID, state.focusRevision]);

  useEffect(() => {
    if (!drag) return;
    const move = (event: PointerEvent) => {
      onAction({
        type: "node.move",
        nodeID: drag.nodeID,
        position: {
          x: drag.origin.x + event.clientX - drag.pointerX,
          y: drag.origin.y + event.clientY - drag.pointerY,
        },
      });
    };
    const end = () => setDrag(null);
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", end, { once: true });
    window.addEventListener("pointercancel", end, { once: true });
    return () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", end);
      window.removeEventListener("pointercancel", end);
    };
  }, [drag, onAction]);

  const edges = Object.entries(state.spec.nodes).flatMap(([sourceID, node]) =>
    getNodeReferences(node).map((targetID, index) => ({ sourceID, targetID, index })),
  );

  return (
    <section aria-label="AgentSpec 可编辑画布" style={canvasShellStyle}>
      <div style={{ overflow: "auto", maxHeight: 660 }}>
        <div data-testid="agent-canvas" style={{ ...canvasStyle, width: dimensions.width, height: dimensions.height }}>
          <svg aria-hidden="true" width={dimensions.width} height={dimensions.height} style={{ inset: 0, position: "absolute", pointerEvents: "none" }}>
            <defs>
              <marker id="agent-edge-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--canvas-dot)" />
              </marker>
            </defs>
            {edges.map(({ sourceID, targetID, index }) => {
              const source = state.positions[sourceID];
              const target = state.positions[targetID];
              if (!source || !target) return null;
              const startX = source.x + NODE_WIDTH / 2;
              const startY = source.y + NODE_HEIGHT;
              const endX = target.x + NODE_WIDTH / 2;
              const endY = target.y;
              const bend = Math.max(28, Math.abs(endY - startY) / 2);
              return (
                <path
                  key={`${sourceID}-${targetID}-${index}`}
                  d={`M ${startX} ${startY} C ${startX} ${startY + bend}, ${endX} ${endY - bend}, ${endX} ${endY}`}
                  fill="none"
                  markerEnd="url(#agent-edge-arrow)"
                  stroke="var(--canvas-dot)"
                  strokeWidth="1.6"
                />
              );
            })}
          </svg>
          {Object.entries(state.spec.nodes).map(([nodeID, node]) => {
            const position = state.positions[nodeID] ?? { x: 34, y: 30 };
            const selected = state.selectedNodeID === nodeID;
            const hasDiagnostic = diagnosticNodeIDs.has(nodeID);
            return (
              <article
                aria-label={`节点 ${nodeID}`}
                aria-pressed={selected}
                data-node-id={nodeID}
                key={nodeID}
                ref={(element) => {
                  if (element) nodeElements.current.set(nodeID, element);
                  else nodeElements.current.delete(nodeID);
                }}
                onClick={() => onAction({ type: "node.select", nodeID })}
                onKeyDown={(event: KeyboardEvent<HTMLElement>) => {
                  if (event.key === "Enter" || event.key === " ") onAction({ type: "node.select", nodeID });
                }}
                onPointerDown={(event) => {
                  if (disabled || event.button !== 0) return;
                  event.preventDefault();
                  onAction({ type: "node.select", nodeID });
                  setDrag({ nodeID, pointerX: event.clientX, pointerY: event.clientY, origin: position });
                }}
                role="button"
                tabIndex={disabled ? -1 : 0}
                style={{
                  ...nodeStyle,
                  borderColor: hasDiagnostic ? "var(--danger-border)" : selected ? "var(--primary)" : "var(--line-strong)",
                  boxShadow: selected ? "0 0 0 3px var(--primary-soft), 0 10px 28px var(--shadow-color)" : "0 6px 18px var(--shadow-color)",
                  left: position.x,
                  top: position.y,
                }}
              >
                <div style={{ alignItems: "center", display: "flex", justifyContent: "space-between", gap: 8 }}>
                  <span style={kindStyle}>{node.kind.toUpperCase()}</span>
                  {state.spec.root === nodeID && <span style={rootStyle}>ROOT</span>}
                </div>
                <strong style={{ display: "block", fontSize: 13, marginTop: 10, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {node.name || nodeID}
                </strong>
                <small style={{ color: "var(--muted)", display: "block", fontSize: 10, marginTop: 4 }}>{nodeID}</small>
              </article>
            );
          })}
        </div>
      </div>
      <footer style={{ borderTop: "1px solid var(--line)", color: "var(--muted)", fontSize: 10, padding: "9px 12px" }}>
        拖动节点只改变浏览器画布坐标，不会写入 AgentSpec。
      </footer>
    </section>
  );
}

const canvasShellStyle: CSSProperties = {
  background: "var(--surface)",
  border: "1px solid var(--line)",
  borderRadius: 11,
  minWidth: 0,
  overflow: "hidden",
};

const canvasStyle: CSSProperties = {
  backgroundColor: "var(--surface-subtle)",
  backgroundImage: "radial-gradient(var(--canvas-dot) 1px, transparent 1px)",
  backgroundSize: "18px 18px",
  position: "relative",
};

const nodeStyle: CSSProperties = {
  background: "var(--surface)",
  border: "1px solid var(--line-strong)",
  borderRadius: 11,
  cursor: "grab",
  height: NODE_HEIGHT,
  padding: "12px 13px",
  position: "absolute",
  touchAction: "none",
  userSelect: "none",
  width: NODE_WIDTH,
};

const kindStyle: CSSProperties = {
  color: "var(--primary)",
  fontSize: 9,
  fontWeight: 800,
  letterSpacing: ".08em",
};

const rootStyle: CSSProperties = {
  background: "var(--success-soft)",
  borderRadius: 999,
  color: "var(--success)",
  fontSize: 8,
  fontWeight: 800,
  padding: "3px 6px",
};
