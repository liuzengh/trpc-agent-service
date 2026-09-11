/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  reactStrictMode: true,
  async rewrites() {
    const runtimeBase = (process.env.RUNTIME_API_BASE ?? "http://localhost:8080").replace(/\/$/, "");
    return [
      { source: "/runtime-api/:path*", destination: `${runtimeBase}/:path*` },
    ];
  },
};
export default nextConfig;
