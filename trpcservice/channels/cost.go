package channels

// computeCostMicroCents computes the model call cost in microcents from token
// counts and per‑thousand‑token pricing. Zero pricing returns zero (cost
// tracking disabled). The formula is:
//
//	cost = (promptTokens × promptCost + completionTokens × completionCost) / 1000
//
// where the cost fields are in cents per thousand tokens (microcents after
// division — 1 ¢/1K → 1 μ¢ per token; 1 $ = 100 ¢ = 100_000 μ¢).
func computeCostMicroCents(promptTokens, completionTokens int, promptCost, completCost uint32) int64 {
	if promptCost == 0 && completCost == 0 {
		return 0
	}
	p := int64(promptTokens) * int64(promptCost)
	c := int64(completionTokens) * int64(completCost)
	return (p + c) / 1000
}
