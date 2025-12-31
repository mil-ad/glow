package ui

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/glamour"
	"github.com/charmbracelet/glamour/styles"
	"github.com/charmbracelet/lipgloss"
	"github.com/mil-ad/glow/v2/utils"
	te "github.com/muesli/termenv"
)

// Message types for streaming.
type (
	streamChunkMsg    string
	streamEOFMsg      struct{}
	streamErrorMsg    struct{ err error }
	streamRenderedMsg string
)

// streamingModel is a Bubble Tea model for streaming markdown input.
type streamingModel struct {
	cfg Config

	// Content accumulation
	rawContent *strings.Builder

	// Rendered content
	glamOutput   string
	glamHeight   int
	glamViewport viewport.Model

	// Terminal dimensions
	width  int
	height int

	// Channel for receiving chunks from stdin reader goroutine
	chunkChan <-chan string
	errChan   <-chan error

	// Track state
	eofReached    bool
	renderPending bool
}

// NewStreamingProgram creates a new streaming-mode Bubble Tea program.
func NewStreamingProgram(cfg Config, reader io.Reader) (*tea.Program, func() string) {
	// Set auto style based on terminal background
	if cfg.GlamourStyle == styles.AutoStyle {
		if te.HasDarkBackground() {
			cfg.GlamourStyle = styles.DarkStyle
		} else {
			cfg.GlamourStyle = styles.LightStyle
		}
	}

	// Create channels for stdin reading
	chunkChan := make(chan string, 100)
	errChan := make(chan error, 1)

	// Start stdin reader goroutine
	go readStdin(reader, chunkChan, errChan)

	vp := viewport.New(0, 0)
	vp.GotoBottom()

	m := streamingModel{
		cfg:          cfg,
		rawContent:   &strings.Builder{},
		glamViewport: vp,
		chunkChan:    chunkChan,
		errChan:      errChan,
	}

	// Render to stderr for inline display
	opts := []tea.ProgramOption{
		tea.WithOutput(os.Stderr),
		tea.WithInput(nil), // No keyboard input needed during streaming
	}

	return tea.NewProgram(m, opts...), func() string { return "" }
}

func readStdin(reader io.Reader, chunks chan<- string, errCh chan<- error) {
	defer close(chunks)
	defer close(errCh)

	bufReader := bufio.NewReader(reader)
	for {
		line, err := bufReader.ReadString('\n')
		if len(line) > 0 {
			chunks <- line
		}
		if err != nil {
			if err != io.EOF {
				errCh <- err
			}
			return
		}
	}
}

func (m streamingModel) Init() tea.Cmd {
	return m.readNextChunk()
}

func (m streamingModel) readNextChunk() tea.Cmd {
	return func() tea.Msg {
		for {
			select {
			case chunk, ok := <-m.chunkChan:
				if !ok {
					select {
					case err := <-m.errChan:
						if err != nil {
							return streamErrorMsg{err}
						}
					default:
					}
					return streamEOFMsg{}
				}
				return streamChunkMsg(chunk)
			default:
				select {
				case chunk, ok := <-m.chunkChan:
					if !ok {
						select {
						case err := <-m.errChan:
							if err != nil {
								return streamErrorMsg{err}
							}
						default:
						}
						return streamEOFMsg{}
					}
					return streamChunkMsg(chunk)
				case err := <-m.errChan:
					if err != nil {
						return streamErrorMsg{err}
					}
					continue
				}
			}
		}
	}
}

func (m streamingModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.glamViewport.Width = msg.Width
		m.glamViewport.Height = msg.Height

		if m.rawContent.Len() > 0 {
			cmds = append(cmds, m.renderContent())
		}

	case streamChunkMsg:
		m.rawContent.WriteString(string(msg))
		m.renderPending = true
		cmds = append(cmds, m.renderContent(), m.readNextChunk())

	case streamRenderedMsg:
		m.renderPending = false
		m.glamOutput = string(msg)
		m.glamHeight = strings.Count(m.glamOutput, "\n")

		// Update viewport content
		m.glamViewport.SetContent(m.glamOutput)
		m.glamViewport.GotoBottom()

		if m.eofReached {
			return m, tea.Quit
		}

	case streamEOFMsg:
		m.eofReached = true
		if !m.renderPending {
			return m, tea.Quit
		}

	case streamErrorMsg:
		m.eofReached = true
		return m, tea.Quit
	}

	// Update viewport
	var cmd tea.Cmd
	m.glamViewport, cmd = m.glamViewport.Update(msg)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

func (m streamingModel) renderContent() tea.Cmd {
	return func() tea.Msg {
		content := m.rawContent.String()
		content = string(utils.RemoveFrontmatter([]byte(content)))

		rendered, err := m.glamourRender(content)
		if err != nil {
			return streamErrorMsg{err}
		}
		return streamRenderedMsg(rendered)
	}
}

func (m streamingModel) glamourRender(markdown string) (string, error) {
	if !m.cfg.GlamourEnabled {
		return markdown, nil
	}

	width := m.width
	if width == 0 {
		width = int(m.cfg.GlamourMaxWidth)
		if width == 0 {
			width = 80
		}
	}
	if m.cfg.GlamourMaxWidth > 0 && width > int(m.cfg.GlamourMaxWidth) {
		width = int(m.cfg.GlamourMaxWidth)
	}

	options := []glamour.TermRendererOption{
		utils.GlamourStyle(m.cfg.GlamourStyle, false),
		glamour.WithWordWrap(width),
	}

	if m.cfg.PreserveNewLines {
		options = append(options, glamour.WithPreservedNewLines())
	}

	r, err := glamour.NewTermRenderer(options...)
	if err != nil {
		return "", fmt.Errorf("error creating glamour renderer: %w", err)
	}

	out, err := r.Render(markdown)
	if err != nil {
		return "", fmt.Errorf("error rendering markdown: %w", err)
	}

	// Truncate lines to terminal width
	if m.width > 0 {
		trunc := lipgloss.NewStyle().MaxWidth(m.width).Render
		lines := strings.Split(out, "\n")
		for i, line := range lines {
			lines[i] = trunc(line)
		}
		out = strings.Join(lines, "\n")
	}

	return out, nil
}

func (m streamingModel) viewportNeeded() bool {
	return m.glamHeight > m.height
}

func (m streamingModel) View() string {
	if m.viewportNeeded() {
		return m.glamViewport.View()
	}
	return m.glamOutput
}
