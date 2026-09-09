//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

// Command releaseverify rejects release inputs that would make the Kubernetes
// rollout non-reproducible or bypass its confidential egress boundary.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/releaseverify"
)

func main() {
	var manifestPaths []string
	var networkPolicyPath, schemaClass, breakingChangeID string
	flag.Func("manifest", "rendered release workload manifest; repeat for every rollout stage", func(value string) error {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("manifest path is empty")
		}
		manifestPaths = append(manifestPaths, value)
		return nil
	})
	flag.StringVar(&networkPolicyPath, "network-policy", "", "reviewed production NetworkPolicy manifest")
	flag.StringVar(&schemaClass, "schema-class", "", "reviewed migration class: bootstrap, compatible, or breaking")
	flag.StringVar(&breakingChangeID, "breaking-change-id", "", "approved change record for a breaking migration")
	flag.Parse()
	if len(manifestPaths) == 0 || strings.TrimSpace(networkPolicyPath) == "" || strings.TrimSpace(schemaClass) == "" {
		log.Fatal("usage: releaseverify --manifest <file> [--manifest <file>...] --network-policy <file> --schema-class <class> [--breaking-change-id <id>]")
	}

	manifests := make([][]byte, 0, len(manifestPaths))
	for _, path := range manifestPaths {
		data, err := os.ReadFile(path)
		if err != nil {
			log.Fatalf("read release manifest: %v", err)
		}
		manifests = append(manifests, data)
	}
	policy, err := os.ReadFile(networkPolicyPath)
	if err != nil {
		log.Fatalf("read production NetworkPolicy: %v", err)
	}
	if err := releaseverify.ValidateRelease(manifests, policy, releaseverify.ReleaseContext{
		SchemaClass:      schemaClass,
		BreakingChangeID: breakingChangeID,
	}); err != nil {
		log.Fatalf("release verification failed: %v", err)
	}
	fmt.Println("release manifest verification passed")
}
