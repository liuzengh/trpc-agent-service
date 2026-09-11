import type { ReactNode } from "react";
import { AuthGate } from "../../components/auth-gate";

export default function AdminLayout({ children }: { children: ReactNode }) {
  return <AuthGate requireOperator>{children}</AuthGate>;
}
