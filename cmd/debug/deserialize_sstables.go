package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/ogioldat/olappie/core"
)

var (
	titleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FAFAFA")).
			Background(lipgloss.Color("#7D56F4")).
			Padding(0, 1).
			Bold(true)

	successStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#04B575"))

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF5F87"))

	infoStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFD700"))

	progressStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#61AFEF"))
)

type processMsg struct {
	path    string
	success bool
	error   error
}

type scanCompleteMsg struct {
	files []string
	error error
}

type doneMsg struct{}

type model struct {
	sstablesDir string
	files       []string
	processed   []processMsg
	current     int
	done        bool
	totalFiles  int
	scanning    bool
}

func (m model) Init() tea.Cmd {
	return m.scanDirectory()
}

func (m model) scanDirectory() tea.Cmd {
	return func() tea.Msg {
		var sstFiles []string

		err := filepath.Walk(m.sstablesDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if strings.HasSuffix(path, ".bin") {
				sstFiles = append(sstFiles, path)
			}
			return nil
		})

		if err != nil {
			return scanCompleteMsg{error: err}
		}

		return scanCompleteMsg{files: sstFiles}
	}
}

func (m model) processFile(path string) tea.Cmd {
	return func() tea.Msg {
		file, err := os.Open(path)
		if err != nil {
			return processMsg{path: path, success: false, error: err}
		}
		defer file.Close()

		deserializer := &core.BinarySSTableDeserializer{}
		deserialized, err := deserializer.Deserialize(file)
		if err != nil {
			return processMsg{path: path, success: false, error: err}
		}

		textContent := formatDeserialized(deserialized)
		outputPath := strings.TrimSuffix(path, ".bin") + ".txt"

		err = os.WriteFile(outputPath, []byte(textContent), 0644)
		if err != nil {
			return processMsg{path: path, success: false, error: err}
		}

		return processMsg{path: path, success: true}
	}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" || msg.String() == "q" {
			return m, tea.Quit
		}
		if m.done && (msg.String() == "enter" || msg.String() == " ") {
			return m, tea.Quit
		}

	case scanCompleteMsg:
		m.scanning = false
		if msg.error != nil {
			m.processed = append(m.processed, processMsg{error: msg.error})
			m.done = true
			return m, nil
		}

		m.files = msg.files
		m.totalFiles = len(msg.files)

		if len(msg.files) == 0 {
			m.done = true
			return m, nil
		}

		return m, m.processFile(msg.files[0])

	case processMsg:
		m.processed = append(m.processed, msg)
		m.current++

		if m.current < len(m.files) {
			return m, m.processFile(m.files[m.current])
		} else {
			m.done = true
			return m, nil
		}

	case doneMsg:
		m.done = true
		return m, nil
	}

	return m, nil
}

func (m model) View() string {
	var b strings.Builder

	b.WriteString(titleStyle.Render("🔍 SSTable Debug Tool"))
	b.WriteString("\n\n")

	b.WriteString(infoStyle.Render("Working directory: " + m.sstablesDir + "\n\n"))

	if m.scanning {
		b.WriteString(progressStyle.Render("🔍 Scanning directory for .bin files..."))
		b.WriteString("\n\n")
		b.WriteString(infoStyle.Render("Press Ctrl+C or 'q' to quit"))
		return b.String()
	}

	if m.totalFiles == 0 && m.done {
		b.WriteString(infoStyle.Render("No .bin files found in directory"))
		b.WriteString("\n\n")
		b.WriteString(infoStyle.Render("Press any key to exit"))
		return b.String()
	}

	b.WriteString(fmt.Sprintf("Processing directory: %s\n", m.sstablesDir))
	b.WriteString(fmt.Sprintf("Total files: %d\n\n", m.totalFiles))

	progress := float64(len(m.processed)) / float64(m.totalFiles) * 100
	b.WriteString(progressStyle.Render(fmt.Sprintf("Progress: %.1f%% (%d/%d)", progress, len(m.processed), m.totalFiles)))
	b.WriteString("\n\n")

	for _, proc := range m.processed {
		if proc.success {
			outputPath := strings.TrimSuffix(proc.path, ".bin") + ".txt"
			b.WriteString(successStyle.Render(fmt.Sprintf("✓ %s → %s", proc.path, outputPath)))
		} else {
			b.WriteString(errorStyle.Render(fmt.Sprintf("✗ %s: %v", proc.path, proc.error)))
		}
		b.WriteString("\n")
	}

	if !m.done && m.current < len(m.files) {
		b.WriteString("\n")
		b.WriteString(progressStyle.Render(fmt.Sprintf("Processing: %s", m.files[m.current])))
	}

	if m.done {
		b.WriteString("\n")
		b.WriteString(successStyle.Render("🎉 Processing complete!"))
		b.WriteString("\n")
		b.WriteString(infoStyle.Render("Press Enter or Space to exit"))
	}

	b.WriteString("\n\n")
	b.WriteString(infoStyle.Render("Press Ctrl+C or 'q' to quit"))

	return b.String()
}

func main() {
	sstablesDir := "./data/sstables/level_0"

	m := model{
		sstablesDir: sstablesDir,
		files:       []string{},
		processed:   []processMsg{},
		current:     0,
		done:        false,
		totalFiles:  0,
		scanning:    true,
	}

	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func formatDeserialized(d *core.Deserialized) string {
	var sb strings.Builder

	sb.WriteString("=== SSTable Contents ===\n\n")

	sb.WriteString("BLOOM FILTER:\n")
	sb.WriteString(fmt.Sprintf("Size: %d bits\n", len(d.BloomFilter.Bits())))
	sb.WriteString(fmt.Sprintf("Data: %s\n\n", d.BloomFilter.String()))

	sb.WriteString("SPARSE INDEX:\n")
	sb.WriteString(fmt.Sprintf("Data: %s\n\n", d.SparseIndex.String()))

	sb.WriteString("RECORDS:\n")
	sb.WriteString(fmt.Sprintf("Count: %d\n\n", len(d.Records)))

	for i, record := range d.Records {
		sb.WriteString(fmt.Sprintf("Record %d:\n", i+1))
		sb.WriteString(fmt.Sprintf("  Key: %s\n", string(record.Key)))
		sb.WriteString(fmt.Sprintf("  Value: %s\n", string(record.Value)))
		sb.WriteString(fmt.Sprintf("  Timestamp: %d\n", int64(record.Timestamp)))
		sb.WriteString(fmt.Sprintf("  Tombstone: %t\n", bool(record.Tombstone)))
		sb.WriteString("\n")
	}

	return sb.String()
}
