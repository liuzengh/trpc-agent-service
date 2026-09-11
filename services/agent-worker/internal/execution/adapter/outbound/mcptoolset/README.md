# One selected SDK MCP callable

Open(ctx, Config) returns a Service with Tool() tool.CallableTool and Close().
The configuration is a fixed ServerURL/ToolsetName/ToolName, explicit AuthKind
(none or bearer), BearerToken and positive operation Timeout. The caller owns
Manifest identity, credential resolution, Attempt lifetime and provider aliases.
This package never exports the remote ToolSet or installs a discovered tool list
on an Agent. The selected callable keeps its original SDK remote name.

The root SDK v1.11.2 tool/mcp performs MCP initialization, discovery and calls;
its existing trpc-mcp-go v0.0.10 client performs Streamable HTTP and SSE parsing.
The public Init/filter callback captures the selected SDK callable once. Tools()
is not used: its refresh-on-access and cached fallback semantics are unsuitable
for fixed fail-closed discovery. Missing/duplicate selected names and paginated
lists are rejected. No reconnect or retry options are enabled. Unsolicited GET
SSE is disabled; finite POST SSE responses remain supported and are observed
before being given unchanged to the SDK. No stdio/SSE-URL fallback exists.

The public HTTPReqHandler uses an owned HTTP client: no environment proxy,
normal TLS verification, exact endpoint, no redirects, and fixed Authorization.
Every operation has min(caller deadline, configured timeout). SDK protocol/HTTP
error bodies never reach caller errors or SDK error logs; errors are classified
as authentication, network, protocol, cancellation/deadline. Credentials echoed
in successful response bytes also cause rejection. Close cancels active calls,
closes the SDK session, and closes idle owned connections.

The default SDK NewTool input schema is supported. Root SDK mcp conversion drops
some fields that public tool.Schema can represent, e.g. top-level
additionalProperties:false. The wrapper therefore reads that same discovery
response's input/output schema, validates it using the repository's existing
jsonschema/v6 compiler, and unmarshals directly into public tool.Schema. A JSON
roundtrip equality check prevents silent loss. Only semantically empty
properties/$defs/required are normalized; unsupported constraints such as minimum
are rejected with ErrSchema. Only local refs are permitted: the compiler loader
never fetches a URI. Declaration returns a detached copy. This is an explicit
current SDK expressiveness boundary, not a new Manifest schema contract.

Calls are the actual selected SDK mcpTool.Call. Input validation preserves
additionalProperties and required constraints; malformed/invalid arguments
return ErrArguments before any network call. Valid results retain the actual SDK
result object, including Content, Meta and RetryResultError/IsError. An MCP
business IsError result returns no Go error, allowing model correction; it is not
sticky and causes no adapter retry. A transport/auth/protocol failure returns a
sanitized sentinel. The caller decides fatal Attempt handling for these errors.

service_test.go runs the actual trpc-mcp-go Streamable HTTP server for discover,
call, business failures and cancellation. Discovery mutation fixtures deliberately
inject duplicate/invalid/lossy schemas into that server's response to test reject
paths. Tests cover one discover, only selected tool, none/bearer, parameter schema,
IsError followed by successful correction, auth/network/protocol diagnostics,
cancellation, timeout, closed service, redirects and untrusted TLS.
