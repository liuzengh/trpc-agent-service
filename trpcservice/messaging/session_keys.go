package messaging

import (
	"github.com/liuzengh/trpc-agent-service/trpcservice/keyspace"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
)

// sessionCoord derives an opaque, collision-resistant coordination identity.
// Length-prefixing keeps concatenated values unambiguous.
func sessionCoord(task message.ExecutionTask) string {
	return keyspace.SessionCoord(task.TenantID, task.ChannelBindingID, task.RunnerUserID, task.SessionID)
}

func userCoord(task message.ExecutionTask) string {
	return keyspace.UserCoord(task.TenantID, task.ChannelBindingID, task.RunnerUserID)
}
