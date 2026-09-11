import { cookies } from "next/headers";
import { redirect } from "next/navigation";

export default async function Home() {
  const jar = await cookies();
  // Presence is only a routing hint. /console validates the session with Control.
  const session = jar.get(process.env.CONTROL_SESSION_COOKIE_NAME || "control_session");
  redirect(session?.value ? "/console" : "/login");
}
