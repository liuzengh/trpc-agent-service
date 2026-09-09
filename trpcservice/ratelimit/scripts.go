package ratelimit

import _ "embed"

// threeDimensionalFixedWindowScript is loaded from a fixed repository resource.
// It checks all dimensions before mutating any of them.
//
//go:embed lua/three_dimensional_fixed_window.lua
var threeDimensionalFixedWindowScript string
