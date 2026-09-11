package channels

import (
	"encoding/json"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

type MediaReference = runtimecontext.MediaReference

func MediaEnabled(b controlplane.ChannelBinding) bool {
	var cfg struct {
		Enabled bool `json:"attachments_enabled"`
	}
	return b.ChannelType == "telegram" && json.Unmarshal(b.Config, &cfg) == nil && cfg.Enabled
}
