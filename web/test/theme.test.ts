import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const theme = readFileSync(join(process.cwd(), "app/theme.css"), "utf8");
const colors = Object.fromEntries([...theme.matchAll(/--([\w-]+):\s*(#[\da-f]{6})\s*;/g)].map((match) => [match[1], match[2]]));
function luminance(hex: string) {
  const rgb = [1, 3, 5].map((i) => Number.parseInt(hex.slice(i, i + 2), 16) / 255);
  const linear = rgb.map((c) => c <= .04045 ? c / 12.92 : ((c + .055) / 1.055) ** 2.4);
  return linear[0] * .2126 + linear[1] * .7152 + linear[2] * .0722;
}
function contrast(a: string, b: string) {
  const [dark, light] = [luminance(colors[a]), luminance(colors[b])].sort((x, y) => x - y);
  return (light + .05) / (dark + .05);
}
function sourceFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => entry.isDirectory() ? sourceFiles(join(dir, entry.name)) : /\.(css|tsx)$/.test(entry.name) && !entry.name.includes(".test.") ? [join(dir, entry.name)] : []);
}

describe("Scheme F design tokens", () => {
  it("keeps the selected palette and separates action, category and status roles", () => {
    expect(colors).toMatchObject({ bg: "#f6f3ec", primary: "#245c4f", secondary: "#ad674b", accent: "#537f98" });
    expect(new Set([colors.primary, colors["secondary-ink"], colors["info-ink"], colors.success, colors.warning, colors.danger]).size).toBe(6);
  });
  it.each([
    ["ink", "bg"], ["ink-soft", "surface"], ["muted", "bg"], ["placeholder", "surface"],
    ["on-primary", "primary"], ["primary", "primary-soft"], ["secondary-ink", "secondary-soft"],
    ["info-ink", "info-soft"], ["success", "success-soft"], ["warning", "warning-soft"],
    ["danger", "danger-soft"], ["code-ink", "code-bg"], ["primary-soft", "primary-hover"],
  ])("keeps normal text contrast at least 4.5:1 for %s on %s", (foreground, background) => {
    expect(contrast(foreground, background)).toBeGreaterThanOrEqual(4.5);
  });
  it("defines every CSS role consumed by the console", () => {
    const files = [...sourceFiles(join(process.cwd(), "app")), ...sourceFiles(join(process.cwd(), "components"))];
    const definitions = new Set([...theme.matchAll(/(--[\w-]+):/g)].map((match) => match[1]));
    for (const file of files) {
      const text = readFileSync(file, "utf8");
      for (const match of text.matchAll(/var\((--[\w-]+)/g)) expect(definitions.has(match[1]), `${file}: ${match[1]}`).toBe(true);
    }
  });
  it("loads one shared theme before the global component styles", () => {
    const layout = readFileSync(join(process.cwd(), "app/layout.tsx"), "utf8");
    expect(layout.indexOf('"./theme.css"')).toBeLessThan(layout.indexOf('"./globals.css"'));
  });
});
