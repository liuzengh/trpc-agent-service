"use client";

import { useEffect, useRef, useState, type InputHTMLAttributes } from "react";

type InputProps = Omit<InputHTMLAttributes<HTMLInputElement>, "value" | "onChange">;

function parseCSV(value: string): string[] {
  return [...new Set(value.split(",").map((item) => item.trim()).filter(Boolean))];
}

/** Keep the user's text/caret while publishing the parsed projection on each edit. */
export function CsvInput({ value, onValueChange, onBlur, onCompositionStart, onCompositionEnd, ...props }: InputProps & {
  value: readonly string[];
  onValueChange(value: string[]): void;
}) {
  const [draft, setDraft] = useState(() => value.join(", "));
  const composing = useRef(false);
  const valueKey = JSON.stringify(value);

  useEffect(() => {
    // Echoes of our own edits must not remove a trailing comma or space.
    // A different external value (e.g. revision reload) still replaces the buffer.
    setDraft((current) => JSON.stringify(parseCSV(current)) === valueKey ? current : value.join(", "));
  }, [valueKey]);

  return <input {...props} value={draft}
    onChange={(event) => {
      const next = event.target.value;
      setDraft(next);
      if (!composing.current) onValueChange(parseCSV(next));
    }}
    onCompositionStart={(event) => { composing.current = true; onCompositionStart?.(event); }}
    onCompositionEnd={(event) => {
      composing.current = false;
      const next = event.currentTarget.value;
      setDraft(next);
      onValueChange(parseCSV(next));
      onCompositionEnd?.(event);
    }}
    onBlur={(event) => { setDraft(parseCSV(event.currentTarget.value).join(", ")); onBlur?.(event); }}
  />;
}

/** Preserve editable numeric text instead of reformatting every character. */
export function NumberInput({ value, onValueChange, onBlur, ...props }: InputProps & {
  value: number | undefined;
  onValueChange(value: number | undefined): void;
}) {
  const [draft, setDraft] = useState(() => value === undefined ? "" : String(value));
  useEffect(() => {
    setDraft((current) => (current === "" ? undefined : Number(current)) === value
      ? current : value === undefined ? "" : String(value));
  }, [value]);
  return <input {...props} type="number" value={draft}
    onChange={(event) => {
      const next = event.target.value;
      setDraft(next);
      onValueChange(next === "" ? undefined : Number(next));
    }}
    onBlur={(event) => {
      setDraft(value === undefined ? "" : String(value));
      onBlur?.(event);
    }}
  />;
}
