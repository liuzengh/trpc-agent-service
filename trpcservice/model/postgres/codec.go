package postgres

import "github.com/XnLemon/trpc-agent-service/trpcservice/model"

func encodeModelJSON(configuration model.Configuration) ([]byte, []byte, error) {
	options, err := encodeJSON(configuration.Options)
	if err != nil {
		return nil, nil, err
	}
	generation, err := encodeJSON(configuration.Generation)
	if err != nil {
		return nil, nil, err
	}
	return options, generation, nil
}

func decodeModelJSON(options, generation []byte, configuration *model.Configuration) error {
	if err := decodeJSON(options, &configuration.Options); err != nil {
		return err
	}
	return decodeJSON(generation, &configuration.Generation)
}
