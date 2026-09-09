//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package main

// main is intentionally a thin composition root. Runtime wiring lives in
// admin_bootstrap.go so protocol handlers and policy helpers remain isolated
// from process lifecycle concerns.
func main() {
	runAdmin()
}
