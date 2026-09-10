package postgres

import appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"

type storedGenerationConfig struct {
	appmodel.GenerationConfig
	Chain *appmodel.ChainConfiguration `json:"chain,omitempty"`
}

func encodeAgentRevisionParts(revision appmodel.Revision) ([]byte, []byte, []byte, error) {
	generation, err := encodeJSON(storedGenerationConfig{
		GenerationConfig: revision.Generation,
		Chain:            revision.Chain.Clone(),
	})
	if err != nil {
		return nil, nil, nil, err
	}
	runtime, err := encodeJSON(revision.Runtime)
	if err != nil {
		return nil, nil, nil, err
	}
	tools, err := encodeJSON(revision.Tools)
	if err != nil {
		return nil, nil, nil, err
	}
	return generation, runtime, tools, nil
}

func decodeAgentRevisionParts(generation, runtime []byte, revision *appmodel.Revision) error {
	var stored storedGenerationConfig
	if err := decodeJSON(generation, &stored); err != nil {
		return err
	}
	revision.Generation = stored.GenerationConfig
	revision.Chain = stored.Chain.Clone()
	return decodeJSON(runtime, &revision.Runtime)
}
