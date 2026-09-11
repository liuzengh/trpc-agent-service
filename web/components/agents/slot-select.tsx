"use client";

/** Requirements declare slots; a node only references them. Free-text slot
 *  entry hid that relationship, so selection is driven by the declarations. */
export function SlotSelect({ label, requirement, declared, selected, replace, disabled }: {
  label: string;
  requirement: string;
  declared: Record<string, unknown>;
  selected: string[];
  replace(next: string[]): void;
  disabled: boolean;
}) {
  const names = Object.keys(declared);
  const undeclared = selected.filter((slot) => !Object.hasOwn(declared, slot));
  const toggle = (slot: string) => replace(selected.includes(slot) ? selected.filter((item) => item !== slot) : [...selected, slot]);
  return <fieldset disabled={disabled} style={{ display: "grid", gap: 8, minWidth: 0 }}>
    <legend>{label}</legend>
    {names.length === 0 && <small role="status">尚未在 Requirements / {requirement} 声明槽位；先在那里声明，再回到此处选择。</small>}
    {names.map((name) => <label key={name}><input type="checkbox" checked={selected.includes(name)} disabled={disabled} onChange={() => toggle(name)} />{name}</label>)}
    {undeclared.map((name) => <label key={name}><input type="checkbox" checked disabled={disabled} onChange={() => toggle(name)} />{name} · 未声明</label>)}
    <small>此处只引用已声明的槽位；取消全部选择会让节点不再要求这一能力。</small>
  </fieldset>;
}
