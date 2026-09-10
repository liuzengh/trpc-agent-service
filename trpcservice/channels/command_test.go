package channels_test

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestRoutePlatformCommandRecognizesOnlyExactText(t *testing.T) {
	base := channels.ChannelInput{MessageType: channels.MessageTypeText}
	for _, test := range []struct {
		name string
		text string
		want channels.PlatformCommand
	}{
		{name: "exact", text: "/new", want: channels.PlatformCommandNewSession},
		{name: "surrounding whitespace", text: "  /new\n", want: channels.PlatformCommandNewSession},
		{name: "arguments are not accepted", text: "/new now", want: channels.PlatformCommandNone},
		{name: "not text", text: "/new", want: channels.PlatformCommandNone},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := base
			input.MessageType = channels.MessageTypeText
			if test.name == "not text" {
				input.MessageType = channels.MessageTypeEvent
			}
			input.Text = test.text
			if got := channels.RoutePlatformCommand(input); got != test.want {
				t.Fatalf("command = %q, want %q", got, test.want)
			}
		})
	}
}
