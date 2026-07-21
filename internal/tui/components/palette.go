package components

import (
	"strings"

	"github.com/muratmirgun/yordam/internal/app"
)

type PaletteItem struct {
	Name        string
	Description string
	Command     app.CommandKind
	Screen      string
}

type Palette struct {
	items  []PaletteItem
	filter string
	cursor int
}

func NewPalette() Palette {
	return Palette{items: []PaletteItem{
		{Name: "/new", Description: "Create a new session", Command: app.CommandNewSession},
		{Name: "/sessions", Description: "Open a session", Screen: "sessions"},
		{Name: "/mode", Description: "Choose safe, ask, or auto", Screen: "mode"},
		{Name: "/model", Description: "Choose a configured model", Screen: "model"},
		{Name: "/skills", Description: "Inspect frozen skill catalog", Screen: "skills"},
		{Name: "/reload", Description: "Reload ~/.config/yordam/config.jsonc", Command: app.CommandReloadConfig},
		{Name: "/compact", Description: "Compact older context", Command: app.CommandCompact},
		{Name: "/help", Description: "Show commands and keybindings", Screen: "help"},
		{Name: "/quit", Description: "Exit Yordam", Command: app.CommandShutdown},
	}}
}

func (p Palette) Items() []PaletteItem { return append([]PaletteItem(nil), p.items...) }

func (p *Palette) SetFilter(filter string) {
	p.filter = strings.ToLower(strings.TrimSpace(filter))
	p.cursor = 0
}

func (p Palette) Filter() string { return p.filter }

func (p Palette) Visible() []PaletteItem {
	if p.filter == "" {
		return p.Items()
	}
	visible := make([]PaletteItem, 0, len(p.items))
	for _, item := range p.items {
		if strings.Contains(strings.ToLower(item.Name), p.filter) {
			visible = append(visible, item)
		}
	}
	return visible
}

func (p Palette) Select() PaletteItem {
	visible := p.Visible()
	if len(visible) == 0 {
		return PaletteItem{}
	}
	index := min(max(p.cursor, 0), len(visible)-1)
	return visible[index]
}

func (p *Palette) Move(delta int) {
	visible := p.Visible()
	if len(visible) == 0 {
		p.cursor = 0
		return
	}
	p.cursor = (p.cursor + delta + len(visible)) % len(visible)
}
