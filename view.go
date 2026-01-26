// Copyright 2026 mlrd.tech, Inc.
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

var (
	primaryColor   = lipgloss.Color("39")  // blue
	secondaryColor = lipgloss.Color("252") // light gray
	errorColor     = lipgloss.Color("196") // red
	successColor   = lipgloss.Color("82")  // green
	selectedColor  = lipgloss.Color("12")  // light blue
	filterColor    = lipgloss.Color("5")   // magenta
	cursorColor    = lipgloss.Color("39")  // blue

	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(primaryColor).
			Padding(0, 1)

	statusStyle = lipgloss.NewStyle().
			Foreground(secondaryColor)

	errorStyle = lipgloss.NewStyle().
			Foreground(errorColor)

	inputStyle = lipgloss.NewStyle().
			Foreground(primaryColor)

	tableRowStyle = lipgloss.NewStyle().
			Padding(0, 1)

	selectedRowStyle = lipgloss.NewStyle().
				Padding(0, 1).
				Background(lipgloss.Color("236"))

	cursorStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(cursorColor)

	multiSelectStyle = lipgloss.NewStyle().
				Foreground(selectedColor)

	helpStyle = lipgloss.NewStyle().
			Foreground(secondaryColor).
			Padding(1)

	overlayStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(primaryColor).
			Padding(1)

	modeNormalStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("39")). // blue (like header)
			Padding(0, 1)

	modeCommandStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("82")). // terminal green
				Padding(0, 1)
)

func (m *Model) View() string {
	if m.width == 0 {
		return "Loading..."
	}

	var b strings.Builder

	// Top line: table name and status
	b.WriteString(m.renderHeader())
	b.WriteString("\n")

	// Middle: content based on mode
	// height - 2: one for header, one for bottom status line
	contentHeight := m.height - 2
	switch m.mode {
	case ModeHelp:
		b.WriteString(m.renderHelp(contentHeight))
	case ModeTableSelect:
		b.WriteString(m.renderTableSelect(contentHeight))
	case ModeItemView:
		b.WriteString(m.renderItemView(contentHeight))
	case ModeErrorView:
		b.WriteString(m.renderErrorView(contentHeight))
	case ModeConfirmDelete:
		b.WriteString(m.renderItems(contentHeight))
	case ModeFilter:
		b.WriteString(m.renderItems(contentHeight))
	default:
		b.WriteString(m.renderItems(contentHeight))
	}

	// Bottom line: input or mode indicator
	b.WriteString("\n")
	b.WriteString(m.renderInput())

	return b.String()
}

func (m *Model) renderHeader() string {
	var tableName string
	if len(m.tables) > 0 && m.currentTable < len(m.tables) {
		table := m.tables[m.currentTable]
		tableName = table.Name
		if table.SortKey != "" {
			tableName += fmt.Sprintf(" (PK: %s, SK: %s)", table.PartitionKey, table.SortKey)
		} else {
			tableName += fmt.Sprintf(" (PK: %s)", table.PartitionKey)
		}
	} else {
		tableName = "No table"
	}

	// Add filter indicator if filters are active
	filterIndicator := ""
	if m.isFiltered {
		filterIndicator = lipgloss.NewStyle().
			Bold(true).
			Foreground(filterColor).
			Render(fmt.Sprintf(" FILTERED: %d", len(m.filters)))
	}

	tableStr := headerStyle.Render(tableName) + filterIndicator

	var statusStr string
	if m.err != nil {
		statusStr = errorStyle.Render(m.status)
	} else {
		statusStr = statusStyle.Render(m.status)
	}

	// Calculate spacing
	space := max(m.width-lipgloss.Width(tableStr)-lipgloss.Width(statusStr)-2, 1)

	return tableStr + strings.Repeat(" ", space) + statusStr
}

func (m *Model) renderItems(height int) string {
	displayItems := m.getFilteredItems()
	if len(displayItems) == 0 {
		if m.isFiltered {
			return strings.Repeat("\n", height-2) + statusStyle.Render("  No items match filter")
		}
		return strings.Repeat("\n", height-2) + statusStyle.Render("  No items")
	}

	if len(m.tables) == 0 {
		return strings.Repeat("\n", height-2) + statusStyle.Render("  No table selected")
	}

	table := m.tables[m.currentTable]

	// Calculate column widths
	pkWidth := 20
	skWidth := 20
	jsonWidth := m.width - pkWidth - skWidth - 10
	if table.SortKey == "" {
		skWidth = 0
		jsonWidth = m.width - pkWidth - 6
	}
	jsonWidth = max(20, jsonWidth)

	var lines []string

	// Calculate visible range
	visibleRows := height - 1
	startIdx := 0
	if m.cursor >= visibleRows {
		startIdx = m.cursor - visibleRows + 1
	}
	endIdx := startIdx + visibleRows
	if endIdx > len(displayItems) {
		endIdx = len(displayItems)
	}

	for i := startIdx; i < endIdx; i++ {
		item := displayItems[i]

		pk := truncate(GetKeyValue(item, table.PartitionKey), pkWidth)
		sk := ""
		if table.SortKey != "" {
			sk = truncate(GetKeyValue(item, table.SortKey), skWidth)
		}
		jsonStr := truncate(ItemToJSON(item), jsonWidth)

		// Build row
		var row string
		if table.SortKey != "" {
			row = fmt.Sprintf(" %-*s │ %-*s │ %s", pkWidth, pk, skWidth, sk, jsonStr)
		} else {
			row = fmt.Sprintf(" %-*s │ %s", pkWidth, pk, jsonStr)
		}

		// Apply styling
		if i == m.cursor {
			if m.selected[i] {
				row = multiSelectStyle.Render("▶ ") + selectedRowStyle.Render(row)
			} else {
				row = cursorStyle.Render("▶ ") + selectedRowStyle.Render(row)
			}
		} else if m.selected[i] {
			row = multiSelectStyle.Render("● ") + tableRowStyle.Render(row)
		} else {
			row = "  " + tableRowStyle.Render(row)
		}

		lines = append(lines, row)
	}

	// Pad remaining lines to fill content area
	for len(lines) < visibleRows {
		lines = append(lines, "")
	}

	return strings.Join(lines, "\n")
}

func (m *Model) renderTableSelect(height int) string {
	visibleRows := height - 1
	var lines []string
	lines = append(lines, headerStyle.Render("Select Table:"))
	lines = append(lines, "")

	for i, table := range m.tables {
		prefix := "  "
		if i == m.currentTable {
			prefix = cursorStyle.Render("▶ ")
		}
		line := prefix + table.Name
		if table.SortKey != "" {
			line += statusStyle.Render(fmt.Sprintf(" (PK: %s, SK: %s)", table.PartitionKey, table.SortKey))
		} else {
			line += statusStyle.Render(fmt.Sprintf(" (PK: %s)", table.PartitionKey))
		}
		lines = append(lines, line)
	}

	for len(lines) < visibleRows { // pad
		lines = append(lines, "")
	}

	return strings.Join(lines, "\n")
}

func (m *Model) renderItemView(height int) string {
	visibleRows := height - 1

	if !m.showDataTypes {
		// Normal view - just show values
		content := overlayStyle.Render(m.viewContent)
		contentLines := strings.Split(content, "\n")

		// Start at top
		result := contentLines

		// Pad to fill screen
		for len(result) < visibleRows {
			result = append(result, "")
		}

		// Truncate to fit
		if len(result) > visibleRows {
			result = result[:visibleRows]
		}

		return strings.Join(result, "\n")
	}

	// Split-screen view: values on left, types on right
	item := m.getCurrentItem()
	if item == nil {
		return strings.Repeat("\n", visibleRows-1) + statusStyle.Render("  No item")
	}

	// Get both value and type content
	valueContent := ItemToPrettyJSON(item)
	typeContent := ItemToDataTypes(item)

	// Calculate split width (50/50)
	halfWidth := (m.width - 6) / 2
	if halfWidth < 10 {
		halfWidth = 10
	}

	// Create bordered panels
	leftStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(primaryColor).
		Padding(1).
		Width(halfWidth).
		Height(visibleRows - 2)

	rightStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(successColor).
		Padding(1).
		Width(halfWidth).
		Height(visibleRows - 2)

	leftPanel := leftStyle.Render(valueContent)
	rightPanel := rightStyle.Render(typeContent)

	// Join panels side by side
	return lipgloss.JoinHorizontal(lipgloss.Top, leftPanel, rightPanel)
}

func (m *Model) renderErrorView(height int) string {
	visibleRows := height - 1
	// Wrap text to fit window (leave room for border and padding)
	maxWidth := max(m.width-6, 20)
	wrapped := wrapText(m.viewContent, maxWidth)

	// Add border with error styling
	errorBoxStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(errorColor).
		Foreground(errorColor).
		Padding(1).
		MaxWidth(m.width - 2)

	content := errorBoxStyle.Render(wrapped)
	contentLines := strings.Split(content, "\n")

	// Start at top
	result := contentLines

	// Pad to fill screen
	for len(result) < visibleRows {
		result = append(result, "")
	}

	if len(result) > visibleRows {
		result = result[:visibleRows]
	}

	return strings.Join(result, "\n")
}

func (m *Model) renderHelp(height int) string {
	help := `
Keyboard Shortcuts:
  ↑/k, ↓/j    Move cursor up/down
  gg          Go to first item
  G           Go to last item
  Enter       View item details
  Space       Toggle multi-select
  e           Edit current item in $EDITOR
  dd          Delete selected/current item(s)
  i, a        Insert new item (PutItem)
  f           Filter items (CSV: attr=value, attr2=value2)
  t           Select table
  x           (In item view) Toggle data type display
  ?           Show this help
  Esc         Cancel/close

Commands:
  /scan [index]                    Scan table or index
  /query [index] pk=value          Query by partition key
  /get pk [sk]                     Get single item by primary key
  /put                             Put new item (opens editor)
  /update pk [sk]                  Update item (opens editor)
  /delete pk [sk]                  Delete item
  /rm pk [sk]                      Delete item (alias)
  /?                               Show this help
  /err                             Show last error
  /q, :q, :quit                    Quit

Type Hints:
  When editing items, use <TYPE> suffix to specify DynamoDB types:
  Examples:
    "count<N>": 42              → Number type
    "tags<SS>": ["a", "b"]      → String Set
    "data<L>": [1, "two"]       → List
    "config<M>": {...}          → Map
    "active<BOOL>": true        → Boolean
    "empty<NULL>": null         → Null

  Supported types: S, N, BOOL, NULL, L, M, SS, NS, B, BS
  Type hints are removed from attribute names after conversion.

Press Esc or ? to close
`
	return helpStyle.Render(help)
}

func (m *Model) renderInput() string {
	switch m.mode {
	case ModeConfirmDelete:
		count := len(m.selected)
		if count == 0 {
			count = 1
		}
		return errorStyle.Render(fmt.Sprintf("Delete %d item(s)? (y/n) ", count))

	case ModeTableSelect:
		return statusStyle.Render("Press Enter to select, Esc to cancel")

	case ModeItemView:
		if m.showDataTypes {
			return statusStyle.Render("Press x to hide types, Enter/q/Esc to close")
		}
		return statusStyle.Render("Press x to show types, Enter/q/Esc to close")

	case ModeErrorView:
		return errorStyle.Render("Press Enter, q, or Esc to close")

	case ModeHelp:
		return statusStyle.Render("Press ? or Esc to close")

	case ModeCommand:
		return modeCommandStyle.Render(m.input.View())

	case ModeFilter:
		return lipgloss.NewStyle().
			Bold(true).
			Foreground(filterColor).
			Render("Filter: " + m.filterInput.View())

	default:
		// Normal mode: rows selected with arrows/jk, hotkeys (no input shown)
		return modeNormalStyle.Render("~~ ITEMS ~~")
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

func wrapText(s string, maxWidth int) string {
	if maxWidth <= 0 {
		return s
	}
	var result strings.Builder
	for _, line := range strings.Split(s, "\n") {
		for len(line) > maxWidth {
			// Find last space before maxWidth
			breakAt := maxWidth
			for i := maxWidth - 1; i > 0; i-- {
				if line[i] == ' ' {
					breakAt = i
					break
				}
			}
			result.WriteString(line[:breakAt])
			result.WriteString("\n")
			line = strings.TrimLeft(line[breakAt:], " ")
		}
		result.WriteString(line)
		result.WriteString("\n")
	}
	return strings.TrimSuffix(result.String(), "\n")
}
