package components

import (
	"sort"
	"strings"

	"github.com/muratmirgun/yordam/internal/domain"
)

type Sessions struct {
	items  []domain.SessionSummary
	filter string
	cursor int
}

func NewSessions(items []domain.SessionSummary) Sessions {
	items = append([]domain.SessionSummary(nil), items...)
	sortSessions(items)
	return Sessions{items: items}
}

func (s *Sessions) Upsert(summary domain.SessionSummary) {
	for index := range s.items {
		if s.items[index].ID == summary.ID {
			s.items[index] = summary
			sortSessions(s.items)
			s.cursor = 0
			return
		}
	}
	s.items = append(s.items, summary)
	sortSessions(s.items)
	s.cursor = 0
}

func sortSessions(items []domain.SessionSummary) {
	sort.SliceStable(items, func(left, right int) bool {
		return items[left].UpdatedAt.After(items[right].UpdatedAt)
	})
}

func (s *Sessions) SetFilter(filter string) {
	s.filter = strings.ToLower(strings.TrimSpace(filter))
	s.cursor = 0
}

func (s Sessions) Filter() string { return s.filter }

func (s Sessions) Visible() []domain.SessionSummary {
	if s.filter == "" {
		return append([]domain.SessionSummary(nil), s.items...)
	}
	visible := make([]domain.SessionSummary, 0, len(s.items))
	for _, item := range s.items {
		if strings.Contains(strings.ToLower(item.Title), s.filter) || strings.Contains(strings.ToLower(item.ID), s.filter) {
			visible = append(visible, item)
		}
	}
	return visible
}

func (s Sessions) Select() string {
	visible := s.Visible()
	if len(visible) == 0 {
		return ""
	}
	index := min(max(s.cursor, 0), len(visible)-1)
	return visible[index].ID
}

func (s *Sessions) Move(delta int) {
	visible := s.Visible()
	if len(visible) == 0 {
		s.cursor = 0
		return
	}
	s.cursor = (s.cursor + delta + len(visible)) % len(visible)
}
