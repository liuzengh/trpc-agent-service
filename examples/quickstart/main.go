package main

import (
	"context"
	"fmt"
	"os"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/policy"
	serviceRuntime "github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func main() {
	path := "configs/demo.yaml"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	file, err := config.LoadFile(path)
	if err != nil {
		fatal(err)
	}
	snapshot, err := file.Snapshot("demo", "assistant")
	if err != nil {
		fatal(err)
	}
	bundle, err := serviceRuntime.NewTestBundle(snapshot)
	if err != nil {
		fatal(err)
	}
	defer bundle.Close()
	engine := &policy.Engine{Identity: policy.AuthenticatedIdentityAuthorizer{}}
	bindingID, externalUserID := "quickstart", "demo-user"
	var allowedUsers []string
	if channels := snapshot.App().Channels; len(channels) > 0 {
		bindingID = channels[0].ID
		allowedUsers = channels[0].AllowedUsers
		if len(allowedUsers) > 0 {
			externalUserID = allowedUsers[0]
		}
	}
	userID := fmt.Sprintf("http/%s/%s", bindingID, externalUserID)
	policyRequest := policy.Request{TenantID: snapshot.TenantID(), AppID: snapshot.AppID(), UserID: userID, ExternalUserID: externalUserID, RequestID: "quickstart-1", AllowedUsers: allowedUsers, Policy: snapshot.App().Tools}
	controls, err := engine.Evaluate(context.Background(), policyRequest)
	if err != nil {
		fatal(err)
	}
	ctx := policy.WithRequest(context.Background(), engine, policyRequest)
	result, err := bundle.Run(ctx, serviceRuntime.RunInput{RequestID: "quickstart-1", UserID: userID, SessionID: fmt.Sprintf("dm/%s/%s", bindingID, externalUserID), Text: "calculate 6*7", ToolFilter: controls.Visibility, ToolExecutionFilter: controls.Execution, ToolPermissionPolicy: controls.Permission})
	if err != nil {
		fatal(err)
	}
	for _, item := range result.Events {
		if item.Response == nil {
			continue
		}
		for _, choice := range item.Choices {
			if choice.Message.Content != "" {
				fmt.Println(choice.Message.Content)
			}
		}
	}
}

func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
