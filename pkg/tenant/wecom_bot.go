package tenant

import "fmt"

// IsValidWeComBotBridgeToken validates the independently generated local
// connector token. The provider's Bot Secret never belongs in this field.
func IsValidWeComBotBridgeToken(value string) bool {
	if len(value) < 32 || len(value) > 4096 {
		return false
	}
	for _, ch := range value {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}

// ValidateWeComBotBinding keeps the tenant contract limited to the local
// authenticated bridge. BotID is explicit; provider credentials, endpoints and
// application callback configuration are owned by the connector process.
func ValidateWeComBotBinding(binding ChannelBinding) error {
	if binding.Type != "wecom_bot" || validateLogicalName("smart bot account ID", binding.AccountID, 128) != nil {
		return fmt.Errorf("WeCom smart bot requires an explicit account ID")
	}
	if binding.Secret != "" || binding.SecretRef != "" || binding.EncodingAESKey != "" || binding.EncodingAESKeyRef != "" || binding.AppID != "" || len(binding.Config) != 0 {
		return fmt.Errorf("WeCom smart bot binding does not accept provider credentials or application configuration")
	}
	if binding.TokenRef != "" {
		if binding.Token != "" || SecretRef(binding.TokenRef).Validate() != nil {
			return fmt.Errorf("WeCom smart bot bridge credential source is invalid")
		}
	} else if !IsValidWeComBotBridgeToken(binding.Token) {
		return fmt.Errorf("WeCom smart bot bridge token must contain 32..4096 URL-safe characters")
	}
	return nil
}
