package components_test

import (
	"testing"
	"time"

	"github.com/muratmirgun/yordam/internal/domain"
	"github.com/muratmirgun/yordam/internal/tui/components"
)

func TestSessionPickerFiltersAndSelects(t *testing.T) {
	picker := components.NewSessions([]domain.SessionSummary{
		{ID: "1", Title: "fix auth", UpdatedAt: time.Unix(1, 0)},
		{ID: "2", Title: "cache work", UpdatedAt: time.Unix(2, 0)},
	})
	if got := picker.Visible(); len(got) != 2 || got[0].ID != "2" {
		t.Fatalf("sorted visible=%v", got)
	}

	picker.SetFilter("AUTH")
	if got := picker.Visible(); len(got) != 1 || got[0].ID != "1" {
		t.Fatalf("visible=%v", got)
	}
	if selected := picker.Select(); selected != "1" {
		t.Fatalf("selected=%q", selected)
	}

	picker.SetFilter("2")
	if selected := picker.Select(); selected != "2" {
		t.Fatalf("ID-filtered selected=%q", selected)
	}

	picker.SetFilter("")
	picker.Move(1)
	if selected := picker.Select(); selected != "1" {
		t.Fatalf("moved selected=%q", selected)
	}
}

func TestSessionPickerUpsertsAndResorts(t *testing.T) {
	picker := components.NewSessions([]domain.SessionSummary{{ID: "old", Title: "Old", UpdatedAt: time.Unix(1, 0)}})
	picker.Upsert(domain.SessionSummary{ID: "new", Title: "New", UpdatedAt: time.Unix(3, 0)})
	picker.Upsert(domain.SessionSummary{ID: "old", Title: "Renamed", UpdatedAt: time.Unix(2, 0)})
	visible := picker.Visible()
	if len(visible) != 2 || visible[0].ID != "new" || visible[1].Title != "Renamed" {
		t.Fatalf("visible=%v", visible)
	}
}
