//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package main

// main is the process composition root. Runtime wiring lives in runWorker so
// it can be reviewed and exercised independently from the executable shim.
func main() {
	runWorker()
}
