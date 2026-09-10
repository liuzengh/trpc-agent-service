package mysql

import "github.com/XnLemon/trpc-agent-service/trpcservice/channels"

func encodeProtocol(protocol channels.ProtocolConfiguration) ([]byte, error) {
	return encodeJSON(protocol)
}

func decodeProtocol(data []byte, protocol *channels.ProtocolConfiguration) error {
	return decodeJSON(data, protocol)
}
