package telegram

const TestAPIURL = "http://channel-lab:8080"

// Endpoint selects only deployment-defined destinations; accounts carry no URL.
func Endpoint(profile, defaultURL string) (string, error) {
	switch profile {
	case "", "official":
		if defaultURL == "" {
			return "https://api.telegram.org", nil
		}
		return defaultURL, nil
	case "test":
		return TestAPIURL, nil
	default:
		return "", ErrConfig
	}
}
