package components_test

import (
	"slices"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/muratmirgun/yordam/internal/tui/components"
)

func TestComposerEnterSubmitsAndCtrlJAddsNewline(t *testing.T) {
	var sent []string
	composer := components.NewComposer(func(value string) {
		sent = append(sent, value)
	})
	composer.SetValue("first")

	composer, _ = composer.Update(key("ctrl+j"))
	composer.Insert("second")
	if got := composer.Value(); got != "first\nsecond" {
		t.Fatalf("value=%q", got)
	}

	composer, _ = composer.Update(key("enter"))
	if !slices.Equal(sent, []string{"first\nsecond"}) || composer.Value() != "" {
		t.Fatalf("sent=%v value=%q", sent, composer.Value())
	}

	composer.SetActiveTurn(true)
	composer.SetValue("blocked")
	composer, _ = composer.Update(key("enter"))
	if len(sent) != 1 {
		t.Fatalf("submitted during active turn: %v", sent)
	}
}

func TestComposerTrimsOnlyOuterBlankLinesAndKeepsEmptyInput(t *testing.T) {
	var sent []string
	composer := components.NewComposer(func(value string) {
		sent = append(sent, value)
	})
	composer.SetValue("\n\n  first\n\nsecond  \n\n")

	composer, _ = composer.Update(key("enter"))
	if !slices.Equal(sent, []string{"  first\n\nsecond  "}) {
		t.Fatalf("sent=%q", sent)
	}

	composer.SetValue(" \n\t\n ")
	composer, _ = composer.Update(key("enter"))
	if len(sent) != 1 || composer.Value() == "" {
		t.Fatalf("blank input submitted or cleared: sent=%q value=%q", sent, composer.Value())
	}
}

func TestComposerStartsAtOneRowAndCapsVisibleHeight(t *testing.T) {
	composer := components.NewComposer(nil)
	if got := composer.Height(); got != 1 {
		t.Fatalf("initial height=%d want=1", got)
	}

	composer.SetWidth(20)
	composer.SetValue("1\n2\n3\n4\n5\n6\n7\n8\n9")
	if got := composer.Height(); got != 8 {
		t.Fatalf("expanded height=%d want=8", got)
	}
}

func TestComposerViewDoesNotEmbedTerminalCursorStyles(t *testing.T) {
	composer := components.NewComposer(nil)
	composer.SetWidth(40)
	view := composer.View()
	if strings.Contains(view, "\x1b[") {
		t.Fatalf("composer view contains ANSI cursor styles: %q", view)
	}
	if strings.HasSuffix(view, " ") {
		t.Fatalf("composer view contains terminal-width padding: %q", view)
	}
}

func key(value string) tea.KeyPressMsg {
	switch value {
	case "enter":
		return tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter})
	case "ctrl+j":
		return tea.KeyPressMsg(tea.Key{Code: 'j', Mod: tea.ModCtrl})
	default:
		return tea.KeyPressMsg(tea.Key{Code: []rune(value)[0], Text: value})
	}
}
