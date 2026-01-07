// Copyright 2026 mlrd.tech, Inc.
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

type Mode int

const (
	ModeNormal Mode = iota
	ModeCommand
	ModeTableSelect
	ModeItemView
	ModeConfirmDelete
	ModeHelp
	ModeErrorView
)

type Model struct {
	ddb            *DDB
	tables         []*TableInfo
	currentTable   int
	requestedTable string

	items    []map[string]types.AttributeValue
	cursor   int
	selected map[int]bool

	width  int
	height int

	mode      Mode
	input     textinput.Model
	keyBuffer string

	status string
	err    error

	viewContent     string
	editTmpFile     string
	editOrigContent string
	preserveStatus  bool
	lastError       string
}

// Messages
type tablesLoadedMsg struct {
	tables []*TableInfo
	err    error
}

type itemsLoadedMsg struct {
	items   []map[string]types.AttributeValue
	err     error
	noMatch bool
}

type operationDoneMsg struct {
	status string
	err    error
}

type editorFinishedMsg struct {
	content  string
	original string
	err      error
}

type itemFetchedForEditMsg struct {
	item map[string]types.AttributeValue
	err  error
}

func NewModel(ddb *DDB, requestedTable string) *Model {
	ti := textinput.New()
	ti.Placeholder = "~"
	ti.CharLimit = 256
	ti.Width = 50
	ti.Focus()

	return &Model{
		ddb:            ddb,
		requestedTable: requestedTable,
		selected:       make(map[int]bool),
		input:          ti,
		status:         "Loading tables...",
	}
}

func (m *Model) Init() tea.Cmd {
	return m.loadTables
}

func (m *Model) setError(err error) {
	errStr := err.Error()
	m.lastError = errStr
	m.err = err
	// Truncate for status line, show full error in window
	if len(errStr) > 50 {
		m.status = errStr[:47] + "... (/err)"
		m.viewContent = errStr
		m.mode = ModeErrorView
	} else {
		m.status = errStr
	}
}

func (m *Model) loadTables() tea.Msg {
	ctx := context.Background()

	tableNames, err := m.ddb.ListTables(ctx)
	if err != nil {
		return tablesLoadedMsg{err: err}
	}

	var tables []*TableInfo
	for _, name := range tableNames {
		info, err := m.ddb.DescribeTable(ctx, name)
		if err != nil {
			return tablesLoadedMsg{err: err}
		}
		tables = append(tables, info)
	}

	return tablesLoadedMsg{tables: tables}
}

func (m *Model) loadItems(tableName string, indexName string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		items, err := m.ddb.Scan(ctx, tableName, indexName)
		return itemsLoadedMsg{items: items, err: err}
	}
}

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.input.Width = msg.Width - 4
		return m, nil

	case tablesLoadedMsg:
		if msg.err != nil {
			m.setError(msg.err)
			return m, nil
		}
		m.tables = msg.tables
		if len(m.tables) > 0 {
			m.currentTable = 0
			// Try to find requested table
			if m.requestedTable != "" {
				found := false
				for i, t := range m.tables {
					if t.Name == m.requestedTable {
						m.currentTable = i
						found = true
						break
					}
				}
				if !found {
					m.status = fmt.Sprintf("Table '%s' not found, using %s", m.requestedTable, m.tables[0].Name)
					m.preserveStatus = true
				} else {
					m.status = fmt.Sprintf("Loaded %d tables", len(m.tables))
				}
			} else {
				m.status = fmt.Sprintf("Loaded %d tables", len(m.tables))
			}
			return m, m.loadItems(m.tables[m.currentTable].Name, "")
		}
		m.status = "No tables found"
		return m, nil

	case itemsLoadedMsg:
		if msg.err != nil {
			m.setError(msg.err)
			return m, nil
		}
		m.items = msg.items
		m.cursor = 0
		m.selected = make(map[int]bool)
		if msg.noMatch {
			m.status = "No matching item"
		} else if m.preserveStatus {
			m.preserveStatus = false
		} else {
			m.status = fmt.Sprintf("Loaded %d items", len(m.items))
		}
		return m, nil

	case operationDoneMsg:
		if msg.err != nil {
			m.setError(msg.err)
			return m, nil
		}
		m.status = msg.status
		m.err = nil
		// Reload items after successful operation
		if len(m.tables) > 0 {
			return m, m.loadItems(m.tables[m.currentTable].Name, "")
		}
		return m, nil

	case editorFinishedMsg:
		if msg.err != nil {
			m.setError(msg.err)
			return m, nil
		}
		// Check if content changed
		if msg.content == msg.original {
			m.status = "No changes made"
			return m, nil
		}
		// Parse and save the edited item
		return m, m.saveEditedItem(msg.content)

	case itemFetchedForEditMsg:
		if msg.err != nil {
			m.status = fmt.Sprintf("Error: %v", msg.err)
			return m, nil
		}
		if msg.item == nil {
			m.status = "Item not found"
			return m, nil
		}
		// Store item temporarily and open editor
		m.items = []map[string]types.AttributeValue{msg.item}
		m.cursor = 0
		return m, m.editCurrentItem()

	case tea.KeyMsg:
		return m.handleKeyPress(msg)
	}

	return m, nil
}

func (m *Model) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Handle mode-specific input first
	switch m.mode {
	case ModeCommand:
		return m.handleCommandMode(msg)
	case ModeTableSelect:
		return m.handleTableSelectMode(msg)
	case ModeItemView:
		return m.handleItemViewMode(msg)
	case ModeConfirmDelete:
		return m.handleConfirmDeleteMode(msg)
	case ModeErrorView:
		if msg.Type == tea.KeyEsc || msg.Type == tea.KeyEnter || msg.String() == "q" {
			m.mode = ModeNormal
			m.viewContent = ""
		}
		return m, nil
	case ModeHelp:
		if msg.Type == tea.KeyEsc || msg.String() == "q" || msg.String() == "?" {
			m.mode = ModeNormal
			m.viewContent = ""
		}
		return m, nil
	}

	// Normal mode key handling
	switch msg.String() {
	case "ctrl+c":
		return m, tea.Quit

	case "q":
		// Check if we're in command mode starting with ':'
		if m.keyBuffer == ":" {
			return m, tea.Quit
		}
		return m, nil

	case ":":
		m.mode = ModeCommand
		m.input.SetValue(":")
		m.keyBuffer = ""
		if m.err != nil {
			m.err = nil
			m.status = fmt.Sprintf("%d items", len(m.items))
		}
		return m, nil

	case "/":
		m.mode = ModeCommand
		m.input.SetValue("/")
		m.keyBuffer = ""
		if m.err != nil {
			m.err = nil
			m.status = fmt.Sprintf("%d items", len(m.items))
		}
		return m, nil

	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
		}
		m.keyBuffer = ""
		return m, nil

	case "down", "j":
		if m.cursor < len(m.items)-1 {
			m.cursor++
		}
		m.keyBuffer = ""
		return m, nil

	case "enter":
		// If there's input, execute it as a command
		if m.input.Value() != "" {
			cmd := m.input.Value()
			m.input.SetValue("")
			m.keyBuffer = ""
			return m, m.executeCommand(cmd)
		}
		// Otherwise view the selected item
		if len(m.items) > 0 && m.cursor < len(m.items) {
			m.viewContent = ItemToPrettyJSON(m.items[m.cursor])
			m.mode = ModeItemView
		}
		m.keyBuffer = ""
		return m, nil

	case " ":
		if len(m.items) > 0 {
			if m.selected[m.cursor] {
				delete(m.selected, m.cursor)
			} else {
				m.selected[m.cursor] = true
			}
		}
		m.keyBuffer = ""
		return m, nil

	case "e":
		if len(m.items) > 0 && len(m.selected) <= 1 {
			return m, m.editCurrentItem()
		}
		m.keyBuffer = ""
		return m, nil

	case "d":
		if m.keyBuffer == "d" {
			// dd - delete
			m.mode = ModeConfirmDelete
			m.keyBuffer = ""
			return m, nil
		}
		m.keyBuffer = "d"
		return m, nil

	case "t":
		m.mode = ModeTableSelect
		m.keyBuffer = ""
		return m, nil

	case "i", "a":
		m.keyBuffer = ""
		return m, m.putNewItem()

	case "?":
		m.mode = ModeHelp
		m.keyBuffer = ""
		return m, nil

	case "esc":
		m.keyBuffer = ""
		m.input.SetValue("")
		m.mode = ModeNormal
		return m, nil

	case "g":
		if m.keyBuffer == "g" {
			m.cursor = 0
			m.keyBuffer = ""
		} else {
			m.keyBuffer = "g"
		}
		return m, nil

	case "G":
		m.cursor = max(len(m.items)-1, 0)
		m.keyBuffer = ""
		return m, nil

	default:
		m.keyBuffer = ""
	}

	return m, nil
}

func (m *Model) handleCommandMode(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEsc:
		m.mode = ModeNormal
		m.input.SetValue("")
		return m, nil

	case tea.KeyEnter:
		cmd := m.input.Value()
		m.input.SetValue("")
		m.mode = ModeNormal
		return m, m.executeCommand(cmd)
	}

	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m *Model) handleTableSelectMode(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.mode = ModeNormal
		return m, nil

	case "up", "k":
		if m.currentTable > 0 {
			m.currentTable--
		}
		return m, nil

	case "down", "j":
		if m.currentTable < len(m.tables)-1 {
			m.currentTable++
		}
		return m, nil

	case "enter":
		m.mode = ModeNormal
		if len(m.tables) > 0 {
			return m, m.loadItems(m.tables[m.currentTable].Name, "")
		}
		return m, nil
	}
	return m, nil
}

func (m *Model) handleItemViewMode(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q", "enter":
		m.mode = ModeNormal
		m.viewContent = ""
	case "e":
		m.mode = ModeNormal
		m.viewContent = ""
		return m, m.editCurrentItem()
	}
	return m, nil
}

func (m *Model) handleConfirmDeleteMode(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "y", "Y":
		m.mode = ModeNormal
		return m, m.deleteSelectedItems()

	case "n", "N", "esc":
		m.mode = ModeNormal
		return m, nil
	}
	return m, nil
}

func (m *Model) executeCommand(cmd string) tea.Cmd {
	cmd = strings.TrimSpace(cmd)

	// Handle special commands
	switch cmd {
	case ":q", ":quit", "/q", "\\q":
		return tea.Quit
	case ":?", ":help", "/?", "/help":
		m.mode = ModeHelp
		return nil
	case "/err":
		if m.lastError != "" {
			m.viewContent = m.lastError
			m.mode = ModeErrorView
		} else {
			m.status = "No errors"
		}
		return nil
	case "/mlrd":
		m.status = "https://mlrd.tech/docs ~ https://mlrd.app"
		return nil
	}

	// Parse command
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return nil
	}

	command := strings.ToLower(parts[0])
	args := parts[1:]

	switch command {
	case "/scan":
		indexName := ""
		if len(args) > 0 {
			indexName = args[0]
		}
		if len(m.tables) > 0 {
			return m.loadItems(m.tables[m.currentTable].Name, indexName)
		}

	case "/query":
		if len(args) < 1 {
			m.status = "Usage: /query [indexName] pk=value"
			return nil
		}
		return m.executeQuery(args)

	case "/get":
		if len(args) < 1 {
			m.status = "Usage: /get pk [sk]"
			return nil
		}
		return m.executeGet(args)

	case "/put":
		return m.putNewItem()

	case "/update":
		if len(args) < 1 {
			m.status = "Usage: /update pk [sk]"
			return nil
		}
		return m.executeUpdate(args)

	case "/delete", "/rm":
		if len(args) < 1 {
			// Delete current/selected items
			m.mode = ModeConfirmDelete
			return nil
		}
		return m.executeDelete(args)
	}

	m.setError(fmt.Errorf("unknown command: %s", command))
	return nil
}

func (m *Model) executeQuery(args []string) tea.Cmd {
	if len(m.tables) == 0 {
		m.status = "No table selected"
		return nil
	}

	table := m.tables[m.currentTable]
	indexName := ""
	keyArgs := args

	// Check if first arg is an index name
	if len(args) > 1 && !strings.Contains(args[0], "=") {
		indexName = args[0]
		keyArgs = args[1:]
	}

	if len(keyArgs) == 0 {
		m.status = "Usage: /query [indexName] pk=value"
		return nil
	}

	// Parse the key condition
	pkName, pkValue, err := ParseKeyValue(keyArgs[0])
	if err != nil {
		m.status = fmt.Sprintf("Error: %v", err)
		return nil
	}

	keyCondition := fmt.Sprintf("%s = :pk", pkName)
	exprValues := map[string]types.AttributeValue{
		":pk": pkValue,
	}

	return func() tea.Msg {
		ctx := context.Background()
		items, err := m.ddb.Query(ctx, table.Name, indexName, keyCondition, exprValues)
		return itemsLoadedMsg{items: items, err: err}
	}
}

func (m *Model) executeGet(args []string) tea.Cmd {
	if len(m.tables) == 0 {
		m.status = "No table selected"
		return nil
	}

	table := m.tables[m.currentTable]
	key := make(map[string]types.AttributeValue)

	// First arg is partition key value
	key[table.PartitionKey] = &types.AttributeValueMemberS{Value: args[0]}

	// Second arg (if present) is sort key value
	if len(args) > 1 && table.SortKey != "" {
		key[table.SortKey] = &types.AttributeValueMemberS{Value: args[1]}
	}

	return func() tea.Msg {
		ctx := context.Background()
		item, err := m.ddb.GetItem(ctx, table.Name, key)
		if err != nil {
			return itemsLoadedMsg{err: err}
		}
		if item == nil {
			return itemsLoadedMsg{items: []map[string]types.AttributeValue{}, err: nil, noMatch: true}
		}
		return itemsLoadedMsg{items: []map[string]types.AttributeValue{item}, err: nil}
	}
}

func (m *Model) executeUpdate(args []string) tea.Cmd {
	if len(m.tables) == 0 {
		m.status = "No table selected"
		return nil
	}

	table := m.tables[m.currentTable]
	key := make(map[string]types.AttributeValue)

	// First arg is partition key value
	key[table.PartitionKey] = &types.AttributeValueMemberS{Value: args[0]}

	// Second arg (if present) is sort key value
	if len(args) > 1 && table.SortKey != "" {
		key[table.SortKey] = &types.AttributeValueMemberS{Value: args[1]}
	}

	// Get the item first, then the handler will open editor
	return func() tea.Msg {
		ctx := context.Background()
		item, err := m.ddb.GetItem(ctx, table.Name, key)
		if err != nil {
			return itemFetchedForEditMsg{err: err}
		}
		return itemFetchedForEditMsg{item: item}
	}
}

func (m *Model) executeDelete(args []string) tea.Cmd {
	if len(m.tables) == 0 {
		m.status = "No table selected"
		return nil
	}

	table := m.tables[m.currentTable]
	key := make(map[string]types.AttributeValue)

	// First arg is partition key value
	key[table.PartitionKey] = &types.AttributeValueMemberS{Value: args[0]}

	// Second arg (if present) is sort key value
	if len(args) > 1 && table.SortKey != "" {
		key[table.SortKey] = &types.AttributeValueMemberS{Value: args[1]}
	}

	return func() tea.Msg {
		ctx := context.Background()
		err := m.ddb.DeleteItem(ctx, table.Name, key)
		if err != nil {
			return operationDoneMsg{err: err}
		}
		return operationDoneMsg{status: "Item deleted"}
	}
}

func (m *Model) deleteSelectedItems() tea.Cmd {
	if len(m.tables) == 0 || len(m.items) == 0 {
		return nil
	}

	table := m.tables[m.currentTable]

	// Get items to delete (selected or current)
	toDelete := make([]int, 0)
	if len(m.selected) > 0 {
		for idx := range m.selected {
			toDelete = append(toDelete, idx)
		}
	} else if m.cursor < len(m.items) {
		toDelete = append(toDelete, m.cursor)
	}

	if len(toDelete) == 0 {
		return nil
	}

	return func() tea.Msg {
		ctx := context.Background()
		deleted := 0

		for _, idx := range toDelete {
			if idx >= len(m.items) {
				continue
			}
			item := m.items[idx]

			// Build key from item
			key := make(map[string]types.AttributeValue)
			key[table.PartitionKey] = item[table.PartitionKey]
			if table.SortKey != "" {
				if sk, ok := item[table.SortKey]; ok {
					key[table.SortKey] = sk
				}
			}

			if err := m.ddb.DeleteItem(ctx, table.Name, key); err != nil {
				return operationDoneMsg{err: err}
			}
			deleted++
		}

		return operationDoneMsg{status: fmt.Sprintf("Deleted %d item(s)", deleted)}
	}
}

func (m *Model) putNewItem() tea.Cmd {
	// New item template with just primary key attributes
	var content string
	if len(m.tables) > 0 {
		table := m.tables[m.currentTable]
		if table.SortKey != "" {
			content = fmt.Sprintf("{\n  \"%s\": \"\",\n  \"%s\": \"\"\n}", table.PartitionKey, table.SortKey)
		} else {
			content = fmt.Sprintf("{\n  \"%s\": \"\"\n}", table.PartitionKey)
		}
	} else {
		content = "{}"
	}
	return m.openEditor(content)
}

func (m *Model) editCurrentItem() tea.Cmd {
	if len(m.items) == 0 || m.cursor >= len(m.items) {
		m.status = "No item selected"
		return nil
	}
	content := ItemToPrettyJSON(m.items[m.cursor])
	return m.openEditor(content)
}

func (m *Model) openEditor(content string) tea.Cmd {
	m.editOrigContent = content

	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = "vim"
	}

	// Create temp file
	tmpFile, err := os.CreateTemp("", "dui-*.json")
	if err != nil {
		m.status = fmt.Sprintf("Error creating temp file: %v", err)
		return nil
	}
	m.editTmpFile = tmpFile.Name()

	if _, err := tmpFile.WriteString(content); err != nil {
		os.Remove(tmpFile.Name())
		m.status = fmt.Sprintf("Error writing temp file: %v", err)
		return nil
	}
	tmpFile.Close()

	c := exec.Command(editor, m.editTmpFile)
	origContent := content // capture for closure
	return tea.ExecProcess(c, func(err error) tea.Msg {
		if err != nil {
			os.Remove(m.editTmpFile)
			return editorFinishedMsg{err: err}
		}

		// Read result
		result, err := os.ReadFile(m.editTmpFile)
		os.Remove(m.editTmpFile)
		if err != nil {
			return editorFinishedMsg{err: err}
		}

		return editorFinishedMsg{content: string(result), original: origContent}
	})
}

func (m *Model) saveEditedItem(content string) tea.Cmd {
	if len(m.tables) == 0 {
		return func() tea.Msg {
			return operationDoneMsg{err: fmt.Errorf("no table selected")}
		}
	}

	table := m.tables[m.currentTable]

	return func() tea.Msg {
		item, err := JSONToItem(content)
		if err != nil {
			return operationDoneMsg{err: err}
		}

		ctx := context.Background()
		if err := m.ddb.PutItem(ctx, table.Name, item); err != nil {
			return operationDoneMsg{err: err}
		}

		return operationDoneMsg{status: "Item saved"}
	}
}
