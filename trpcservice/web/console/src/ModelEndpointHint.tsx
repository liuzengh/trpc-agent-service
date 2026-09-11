export type ModelEndpointPolicy = "public_https" | "allowlist";

export function ModelEndpointHint({
  policy,
  origins,
}: {
  policy: ModelEndpointPolicy;
  origins: string[];
}) {
  return (
    <div className="muted">
      {policy === "public_https" ? (
        <>
          支持 OpenAI 兼容的公网 HTTPS 地址。内网模型需管理员允许。
          {origins.length > 0 && (
            <div>部署者额外允许：{origins.join("、")}</div>
          )}
        </>
      ) : (
        <>
          此部署启用了严格白名单，仅允许：{origins.join("、") || "暂无地址"}。
          如需其他服务，请联系部署者调整地址策略。
        </>
      )}
    </div>
  );
}
