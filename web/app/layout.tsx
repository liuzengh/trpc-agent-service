import type { Metadata } from "next";
import type { ReactNode } from "react";

import "./theme.css";
import "./globals.css";

export const metadata: Metadata = {
  title: "Agent tRPC Control",
  description: "多租户 Agent 平台控制台",
};

export default function RootLayout({ children }: Readonly<{ children: ReactNode }>) {
  return <html lang="zh-CN"><body>{children}</body></html>;
}
