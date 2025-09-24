package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/joho/godotenv"
	"github.com/ogioldat/ttrunksdb/client"
)

var (
	titleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FAFAFA")).
			Background(lipgloss.Color("#7D56F4")).
			Padding(0, 1).
			Bold(true)

	promptStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#04B575")).
			Bold(true)

	successStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#04B575"))

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF5F87"))

	infoStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFD700"))
)

type model struct {
	textInput textinput.Model
	client    *client.DBClient
	output    []string
	err       error
}

func initialModel() model {
	ti := textinput.New()
	ti.Placeholder = "Enter command (read <key>, write <key> <value>, list, help, quit)"
	ti.Focus()
	ti.CharLimit = 156
	ti.Width = 60

	client := client.NewDBClient("localhost:8080")
	if err := client.Connect(); err != nil {
		return model{
			textInput: ti,
			client:    client,
			output:    []string{errorStyle.Render(fmt.Sprintf("Failed to connect to server: %v", err))},
			err:       err,
		}
	}

	return model{
		textInput: ti,
		client:    client,
		output:    []string{successStyle.Render("✓ Connected to OlappieDB server")},
		err:       nil,
	}
}

func (m model) Init() tea.Cmd {
	return textinput.Blink
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyEnter:
			input := strings.TrimSpace(m.textInput.Value())
			if input != "" {
				m = m.processCommand(input)
				m.textInput.SetValue("")
			}
		case tea.KeyCtrlC, tea.KeyEsc:
			m.client.Disconnect()
			return m, tea.Quit
		}

	case tea.WindowSizeMsg:
		m.textInput.Width = msg.Width - 4
	}

	m.textInput, cmd = m.textInput.Update(msg)
	return m, cmd
}

func (m model) processCommand(input string) model {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return m
	}

	command := strings.ToLower(parts[0])

	switch command {
	case "quit", "exit", "q":
		m.client.Disconnect()
		m.output = append(m.output, successStyle.Render("Goodbye! 👋"))
		return m

	case "help", "h":
		helpText := []string{
			"Available commands:",
			"  read <key>           - Read value for a key",
			"  write <key> <value>  - Write value to a key",
			"  list                 - List all key-value pairs",
			"  help                 - Show this help message",
			"  quit                 - Exit the CLI",
		}
		for _, line := range helpText {
			m.output = append(m.output, infoStyle.Render(line))
		}

	case "read", "r":
		if len(parts) != 2 {
			m.output = append(m.output, errorStyle.Render("Usage: read <key>"))
		} else {
			key := parts[1]
			value, err := m.client.Read(key)
			if err != nil {
				m.output = append(m.output, errorStyle.Render(fmt.Sprintf("Error reading '%s': %v", key, err)))
			} else {
				m.output = append(m.output, successStyle.Render(fmt.Sprintf("%s = %s", key, string(value))))
			}
		}

	case "write", "w":
		if len(parts) < 3 {
			m.output = append(m.output, errorStyle.Render("Usage: write <key> <value>"))
		} else {
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			err := m.client.Write(key, []byte(value))
			if err != nil {
				m.output = append(m.output, errorStyle.Render(fmt.Sprintf("Error writing '%s': %v", key, err)))
			} else {
				m.output = append(m.output, successStyle.Render(fmt.Sprintf("✓ Wrote: %s = %s", key, value)))
			}
		}

	case "list", "l":
		data, err := m.client.List()
		if err != nil {
			m.output = append(m.output, errorStyle.Render(fmt.Sprintf("Error listing entries: %v", err)))
		} else {
			if data == "" {
				m.output = append(m.output, infoStyle.Render("No entries found"))
			} else {
				m.output = append(m.output, successStyle.Render("All entries:"))
				entries := strings.Split(data, "\n")
				for _, entry := range entries {
					if entry != "" {
						m.output = append(m.output, fmt.Sprintf("  %s", entry))
					}
				}
			}
		}

	default:
		m.output = append(m.output, errorStyle.Render(fmt.Sprintf("Unknown command: %s. Type 'help' for available commands.", command)))
	}

	// Keep only last 20 output lines
	if len(m.output) > 20 {
		m.output = m.output[len(m.output)-20:]
	}

	return m
}

func (m model) View() string {
	var b strings.Builder

	// Title
	b.WriteString(titleStyle.Render("🗄️  OlappieDB CLI"))
	b.WriteString("\n\n")

	// Output history
	if len(m.output) > 0 {
		for _, line := range m.output {
			b.WriteString(line)
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}

	// Input prompt
	b.WriteString(promptStyle.Render("ttrunksdb> "))
	b.WriteString(m.textInput.View())
	b.WriteString("\n\n")

	// Help text
	b.WriteString(infoStyle.Render("Press Ctrl+C or type 'quit' to exit • Type 'help' for commands"))

	return b.String()
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	p := tea.NewProgram(initialModel(), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
