package channel

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSplitUTF8ByBytesBoundsMultibyteText(t *testing.T) {
	content := strings.Repeat("深", 2048) + strings.Repeat("🙂", 1024) + "tail"
	chunks, err := splitUTF8ByBytes(content, 2048)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) < 2 {
		t.Fatalf("split produced %d chunks, want multiple", len(chunks))
	}
	var rebuilt strings.Builder
	for index, chunk := range chunks {
		if !utf8.ValidString(chunk) {
			t.Fatalf("chunk %d is invalid UTF-8", index)
		}
		if len(chunk) > 2048 {
			t.Fatalf("chunk %d has %d bytes, want at most 2048", index, len(chunk))
		}
		rebuilt.WriteString(chunk)
	}
	if rebuilt.String() != content {
		t.Fatal("split chunks did not reconstruct the original text")
	}
}

func TestSplitUTF8ByBytesRejectsMalformedOrImpossibleInput(t *testing.T) {
	if _, err := splitUTF8ByBytes(string([]byte{0xff}), 2048); err == nil {
		t.Fatal("malformed UTF-8 was accepted")
	}
	if _, err := splitUTF8ByBytes("🙂", 3); err == nil {
		t.Fatal("chunk smaller than one rune was accepted")
	}
	if _, err := splitUTF8ByBytes("text", 0); err == nil {
		t.Fatal("zero byte limit was accepted")
	}
}

func TestSplitMessageByRunesKeepsTelegramCharacterLimit(t *testing.T) {
	content := strings.Repeat("🙂", 4097)
	chunks := splitMessageByRunes(content, 4096)
	if len(chunks) != 2 || utf8.RuneCountInString(chunks[0]) != 4096 || chunks[0]+chunks[1] != content {
		t.Fatalf("unexpected rune chunks: count=%d first=%d", len(chunks), utf8.RuneCountInString(chunks[0]))
	}
}
