# Shared proxy settings for the production edge.
#
# `include`d by every API location so the timeouts, headers and SSE handling are
# defined once: a rollout that fixes buffering for /chat/stream must not have to
# find three copies of the same directive.

proxy_http_version 1.1;
proxy_set_header Host $host;
proxy_set_header X-Real-IP $remote_addr;
proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
proxy_set_header X-Forwarded-Proto $scheme;

# A turn can legitimately take minutes (multi-call model loop, a human approval
# wait), so the read timeout has to exceed the platform's own turn budget —
# otherwise the proxy gives up while the platform is still working and the user
# loses an answer that was about to be produced.
proxy_connect_timeout 5s;
proxy_send_timeout    600s;
proxy_read_timeout    600s;

# Server-Sent-Events must stream through: buffering would hold the reply until
# the response ends, which for a chat stream means the user sees nothing.
proxy_buffering off;
proxy_cache off;
