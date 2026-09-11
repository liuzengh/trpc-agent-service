import { useState } from "react";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { CsvInput, NumberInput } from "./editor-inputs";

afterEach(cleanup);

function CsvHarness({ initial = [], onChange = () => {} }: {
  initial?: string[];
  onChange?(next: string[]): void;
}) {
  const [value, setValue] = useState(initial);
  return <>
    <CsvInput aria-label="CSV values" value={value} onValueChange={(next) => {
      onChange(next);
      setValue(next);
    }} />
    <output data-testid="csv-value">{JSON.stringify(value)}</output>
  </>;
}

function NumberHarness({ initial = 3 }: { initial?: number }) {
  const [value, setValue] = useState<number | undefined>(initial);
  return <>
    <NumberInput aria-label="Temperature" value={value} onValueChange={setValue} step="any" />
    <output data-testid="number-value">{value === undefined ? "undefined" : String(value)}</output>
  </>;
}

describe("CsvInput controlled text buffer", () => {
  it("preserves a typed trailing comma and spaces while publishing each parsed projection", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<CsvHarness onChange={onChange} />);
    const input = screen.getByRole("textbox", { name: "CSV values" });

    await user.type(input, "search,");
    expect(input).toHaveValue("search,");
    expect(onChange).toHaveBeenLastCalledWith(["search"]);
    await user.type(input, "  fetch,");
    expect(input).toHaveValue("search,  fetch,");
    expect(screen.getByTestId("csv-value")).toHaveTextContent('["search","fetch"]');
    expect(onChange).toHaveBeenCalledTimes("search,  fetch,".length);
  });

  it("normalizes whitespace and duplicates on blur and forwards the blur event", async () => {
    const user = userEvent.setup();
    const onBlur = vi.fn();
    const onValueChange = vi.fn();
    render(<CsvInput aria-label="CSV values" value={[]} onValueChange={onValueChange} onBlur={onBlur} />);
    const input = screen.getByRole("textbox", { name: "CSV values" });
    await user.type(input, " search,fetch, search, ");
    expect(input).toHaveValue(" search,fetch, search, ");
    expect(onValueChange).toHaveBeenLastCalledWith(["search", "fetch"]);

    await user.tab();
    expect(input).toHaveValue("search, fetch");
    expect(onBlur).toHaveBeenCalledTimes(1);
  });

  it("keeps an equivalent external array unformatted but refreshes a different external value", async () => {
    const user = userEvent.setup();
    const onValueChange = vi.fn();
    const { rerender } = render(<CsvInput aria-label="CSV values" value={["search"]} onValueChange={onValueChange} />);
    const input = screen.getByRole("textbox", { name: "CSV values" });
    await user.type(input, ", ");
    expect(input).toHaveValue("search, ");

    rerender(<CsvInput aria-label="CSV values" value={["search"]} onValueChange={onValueChange} />);
    expect(input).toHaveValue("search, ");
    rerender(<CsvInput aria-label="CSV values" value={["docs", "notes"]} onValueChange={onValueChange} />);
    expect(input).toHaveValue("docs, notes");
    rerender(<CsvInput aria-label="CSV values" value={[]} onValueChange={onValueChange} />);
    expect(input).toHaveValue("");
  });

  it("preserves the caret while editing in the middle and echoing controlled values", async () => {
    const user = userEvent.setup();
    const { rerender } = render(<CsvHarness initial={["search", "fetch"]} />);
    const input = screen.getByRole("textbox", { name: "CSV values" });
    if (!(input instanceof HTMLInputElement)) throw new Error("CSV input is not an input element");

    await user.click(input);
    await user.keyboard("{Home}{ArrowRight}{ArrowRight}");
    expect(input.selectionStart).toBe(2);
    await user.keyboard("x");
    expect(input).toHaveValue("sexarch, fetch");
    expect(input.selectionStart).toBe(3);
    expect(input.selectionEnd).toBe(3);
    expect(screen.getByTestId("csv-value")).toHaveTextContent('["sexarch","fetch"]');

    rerender(<CsvHarness initial={["search", "fetch"]} />);
    expect(input).toHaveFocus();
    expect(input.selectionStart).toBe(3);
    await user.keyboard("y");
    expect(input).toHaveValue("sexyarch, fetch");
    expect(input.selectionStart).toBe(4);
  });

  it("buffers IME changes and publishes the completed composition once", () => {
    const onValueChange = vi.fn();
    const onCompositionStart = vi.fn();
    const onCompositionEnd = vi.fn();
    const { rerender } = render(<CsvInput aria-label="CSV values" value={["search"]}
      onValueChange={onValueChange} onCompositionStart={onCompositionStart} onCompositionEnd={onCompositionEnd} />);
    const input = screen.getByRole("textbox", { name: "CSV values" });

    fireEvent.compositionStart(input);
    fireEvent.change(input, { target: { value: "search,搜" } });
    expect(input).toHaveValue("search,搜");
    expect(onValueChange).not.toHaveBeenCalled();
    rerender(<CsvInput aria-label="CSV values" value={["search"]}
      onValueChange={onValueChange} onCompositionStart={onCompositionStart} onCompositionEnd={onCompositionEnd} />);
    expect(input).toHaveValue("search,搜");
    fireEvent.change(input, { target: { value: "search,搜索" } });
    expect(onValueChange).not.toHaveBeenCalled();
    fireEvent.compositionEnd(input, { data: "搜索" });

    expect(input).toHaveValue("search,搜索");
    expect(onValueChange).toHaveBeenCalledExactlyOnceWith(["search", "搜索"]);
    expect(onCompositionStart).toHaveBeenCalledTimes(1);
    expect(onCompositionEnd).toHaveBeenCalledTimes(1);
  });
});

describe("NumberInput controlled text buffer", () => {
  it("clears to undefined and retypes 0.2 without collapsing the decimal edit", async () => {
    const user = userEvent.setup();
    render(<NumberHarness />);
    const input = screen.getByRole("spinbutton", { name: "Temperature" });
    expect(input).toHaveValue(3);
    await user.clear(input);
    expect(input).toHaveValue(null);
    expect(screen.getByTestId("number-value")).toHaveTextContent("undefined");
    await user.type(input, "0.2");
    expect(input).toHaveValue(0.2);
    expect(screen.getByTestId("number-value")).toHaveTextContent("0.2");
    await user.tab();
    expect(input).toHaveValue(0.2);
  });

  it("accepts an external numeric reset or undefined without emitting an edit", async () => {
    const user = userEvent.setup();
    const onValueChange = vi.fn();
    const { rerender } = render(<NumberInput aria-label="Temperature" value={0.2} onValueChange={onValueChange} />);
    const input = screen.getByRole("spinbutton", { name: "Temperature" });
    rerender(<NumberInput aria-label="Temperature" value={1.5} onValueChange={onValueChange} />);
    expect(input).toHaveValue(1.5);
    rerender(<NumberInput aria-label="Temperature" value={undefined} onValueChange={onValueChange} />);
    expect(input).toHaveValue(null);
    expect(onValueChange).not.toHaveBeenCalled();
    await user.type(input, "0.2");
    expect(onValueChange).toHaveBeenLastCalledWith(0.2);
    rerender(<NumberInput aria-label="Temperature" value={2} onValueChange={onValueChange} />);
    expect(input).toHaveValue(2);
  });
});
