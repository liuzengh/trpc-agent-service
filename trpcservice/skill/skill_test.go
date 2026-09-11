package skill

import "testing"

func TestBuiltInSkillsRepositoryLoadsGreeting(t *testing.T) {
	repo, err := Repository("skills")
	if err != nil {
		t.Fatal(err)
	}
	summaries := repo.Summaries()
	if len(summaries) == 0 {
		t.Fatal("built-in skills repository is empty")
	}
	found := false
	for _, summary := range summaries {
		if summary.Name == "greeting" {
			found = true
		}
	}
	if !found {
		t.Fatalf("greeting skill missing: %+v", summaries)
	}
	full, err := repo.Get("greeting")
	if err != nil || full == nil || full.Body == "" {
		t.Fatalf("greeting body load failed: skill=%+v err=%v", full, err)
	}
}
