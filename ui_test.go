package ruvo

import (
	"io/fs"
	"strings"
	"testing"
)

func TestUIEmbedsIndex(t *testing.T) {
	b, err := fs.ReadFile(uiFS, "ui/index.html")
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(b), "api/events") {
		t.Fatal("the page does not subscribe to api/events")
	}
}
