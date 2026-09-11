// Collapsible JSON tree matching the design system's detail-page classes.
import { useState } from 'react';

interface JsonNodeProps {
  name: string | null;
  value: unknown;
  depth: number;
  /** null = user-controlled; number = open while depth < n; boolean = forced. */
  forceOpen: number | boolean | null;
}

function valueClass(value: unknown): string {
  if (value === null) return 'json-null';
  switch (typeof value) {
    case 'string':
      return 'json-string';
    case 'number':
      return 'json-number';
    case 'boolean':
      return 'json-boolean';
    default:
      return 'json-colon';
  }
}

function formatPrimitive(value: unknown): string {
  if (value === null) return 'null';
  if (typeof value === 'string') return JSON.stringify(value);
  return String(value);
}

function JsonNode({ name, value, depth, forceOpen }: JsonNodeProps) {
  const [open, setOpen] = useState(depth < 2);
  const effectiveOpen =
    forceOpen === null ? open : typeof forceOpen === 'boolean' ? forceOpen : depth < forceOpen;
  const isObject = value !== null && typeof value === 'object';

  if (!isObject) {
    return (
      <div className="json-node" data-depth={depth}>
        {name !== null && (
          <>
            <span className="json-key">"{name}"</span>
            <span className="json-colon">: </span>
          </>
        )}
        <span className={valueClass(value)}>{formatPrimitive(value)}</span>
        <span className="json-comma">,</span>
      </div>
    );
  }

  const entries = Array.isArray(value)
    ? value.map((v, i) => [String(i), v] as const)
    : Object.entries(value as Record<string, unknown>);
  const bracket = Array.isArray(value) ? ['[', ']'] : ['{', '}'];
  const nodeClass = effectiveOpen ? 'json-node' : 'json-node collapsed';

  return (
    <div className={nodeClass} data-depth={depth}>
      <span className="json-toggle" onClick={() => setOpen(!effectiveOpen)}>
        {effectiveOpen ? '▾' : '▸'}
      </span>
      {name !== null && (
        <>
          <span className="json-key">"{name}"</span>
          <span className="json-colon">: </span>
        </>
      )}
      <span className="json-bracket">{bracket[0]}</span>
      <span className="json-preview"> {entries.length} 项</span>
      <div className="json-children">
        {entries.map(([key, child]) => (
          <JsonNode
            key={key}
            name={Array.isArray(value) ? null : key}
            value={child}
            depth={depth + 1}
            forceOpen={
              forceOpen === null
                ? null
                : typeof forceOpen === 'number'
                  ? depth + 1 < forceOpen
                  : forceOpen
            }
          />
        ))}
      </div>
      <span className="json-bracket">{bracket[1]}</span>
      <span className="json-comma">,</span>
    </div>
  );
}

export function JsonTree({ data }: { data: unknown }) {
  // forceOpen: null = user-controlled, Infinity/n = expand-all trigger.
  const [expandAll, setExpandAll] = useState<number | null>(null);
  return (
    <div className="json-tree-wrapper">
      <div className="json-tree-header">
        <span className="json-tree-title">JSON 视图</span>
        <button
          className="json-btn-expand-all"
          type="button"
          onClick={() => setExpandAll(expandAll === null ? Infinity : null)}
        >
          {expandAll === null ? '全部展开' : '恢复折叠'}
        </button>
      </div>
      <div className="json-tree">
        <JsonNode name={null} value={data} depth={0} forceOpen={expandAll} />
      </div>
    </div>
  );
}
