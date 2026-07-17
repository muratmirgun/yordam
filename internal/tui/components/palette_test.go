package components_test

import (
	"testing"

	"github.com/muratmirgun/yordam/internal/app"
	"github.com/muratmirgun/yordam/internal/tui/components"
)

func TestPaletteContainsExactSlashCommandTable(t *testing.T) {
	want := []struct {
		name    string
		command app.CommandKind
		screen  string
	}{
		{name: "/new", command: app.CommandNewSession},
		{name: "/sessions", screen: "sessions"},
		{name: "/mode", screen: "mode"},
		{name: "/model", screen: "model"},
		{name: "/reload", command: app.CommandReloadConfig},
		{name: "/compact", command: app.CommandCompact},
		{name: "/help", screen: "help"},
		{name: "/quit", command: app.CommandShutdown},
	}

	items := components.NewPalette().Items()
	if len(items) != len(want) {
		t.Fatalf("items=%v", items)
	}
	for index, expected := range want {
		if got := items[index]; got.Name != expected.name || got.Command != expected.command || got.Screen != expected.screen {
			t.Fatalf("item[%d]=%+v want=%+v", index, got, expected)
		}
	}
}

func TestPaletteFiltersAndSelects(t *testing.T) {
	palette := components.NewPalette()
	palette.SetFilter("sess")
	visible := palette.Visible()
	if len(visible) != 1 || visible[0].Name != "/sessions" {
		t.Fatalf("visible=%v", visible)
	}
	if selected := palette.Select(); selected.Name != "/sessions" {
		t.Fatalf("selected=%+v", selected)
	}

	palette.SetFilter("")
	palette.Move(2)
	if selected := palette.Select(); selected.Name != "/mode" {
		t.Fatalf("moved selected=%+v", selected)
	}
}
