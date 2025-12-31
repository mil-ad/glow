package ui

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/glamour"
	"github.com/charmbracelet/glamour/styles"
	"github.com/mil-ad/glow/v2/utils"
	"github.com/charmbracelet/lipgloss"
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
	cfg      Config
	viewport viewport.Model

	// Content accumulation (using pointer to avoid copy issues with strings.Builder)
	rawContent      *strings.Builder
	renderedContent string

	// Scroll tracking for auto-tail
	wasAtBottom bool

	// Dimensions
	width  int
	height int

	// For final output after quit
	finalContent *string
	contentMutex sync.Mutex

	// Channel for receiving chunks from stdin reader goroutine
	chunkChan <-chan string
	errChan   <-chan error

	// Track if we've finished reading input
	eofReached bool
	// Track if a render is pending (to wait for final render before quitting)
	renderPending bool
}

// NewStreamingProgram creates a new streaming-mode Bubble Tea program.
// Returns the program and a function to retrieve final content after Run().
func NewStreamingProgram(cfg Config, reader io.Reader) (*tea.Program, func() string) {
	var finalContent string

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

	m := streamingModel{
		cfg:          cfg,
		viewport:     viewport.New(0, 0),
		rawContent:   &strings.Builder{},
		wasAtBottom:  true, // Start at bottom
		finalContent: &finalContent,
		chunkChan:    chunkChan,
		errChan:      errChan,
	}

	m.viewport.HighPerformanceRendering = cfg.HighPerformancePager

	opts := []tea.ProgramOption{tea.WithAltScreen()}
	if cfg.EnableMouse {
		opts = append(opts, tea.WithMouseCellMotion())
	}

	return tea.NewProgram(m, opts...), func() string { return finalContent }
}

// readStdin reads from the reader line by line and sends chunks to the channel.
// It also handles content that doesn't end with a newline.
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
		// Prioritize reading chunks over checking for EOF/errors
		// This prevents a race condition where both channels are ready
		// and Go randomly picks errChan first, missing the chunk
		for {
			select {
			case chunk, ok := <-m.chunkChan:
				if !ok {
					// Channel closed, check for errors
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
				// No chunk immediately available, wait on both channels
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
					// Error channel closed without error, but chunks might still be available
					// Loop back to check chunkChan
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
			m.storeFinalContent()
			return m, tea.Quit
		case "ctrl+z":
			return m, tea.Suspend
		case "home", "g":
			m.viewport.GotoTop()
			if m.viewport.HighPerformanceRendering {
				cmds = append(cmds, viewport.Sync(m.viewport))
			}
		case "end", "G":
			m.viewport.GotoBottom()
			if m.viewport.HighPerformanceRendering {
				cmds = append(cmds, viewport.Sync(m.viewport))
			}
		case "d":
			m.viewport.HalfViewDown()
			if m.viewport.HighPerformanceRendering {
				cmds = append(cmds, viewport.Sync(m.viewport))
			}
		case "u":
			m.viewport.HalfViewUp()
			if m.viewport.HighPerformanceRendering {
				cmds = append(cmds, viewport.Sync(m.viewport))
			}
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.viewport.Width = msg.Width
		m.viewport.Height = msg.Height
		// Re-render with new dimensions
		if m.rawContent.Len() > 0 {
			cmds = append(cmds, m.renderContent())
		}

	case streamChunkMsg:
		// Track if we were at bottom before update
		m.wasAtBottom = m.viewport.ScrollPercent() >= 1.0 ||
			m.viewport.TotalLineCount() <= m.viewport.Height

		// Accumulate content
		m.rawContent.WriteString(string(msg))

		// Trigger re-render and continue reading
		m.renderPending = true
		cmds = append(cmds, m.renderContent(), m.readNextChunk())

	case streamRenderedMsg:
		m.renderPending = false
		m.renderedContent = string(msg)
		m.viewport.SetContent(m.renderedContent)

		// Auto-tail: scroll to bottom if user was at bottom
		if m.wasAtBottom {
			m.viewport.GotoBottom()
		}

		if m.viewport.HighPerformanceRendering {
			cmds = append(cmds, viewport.Sync(m.viewport))
		}

		// If EOF was reached while we were rendering, quit now
		if m.eofReached {
			m.storeFinalContent()
			return m, tea.Quit
		}

	case streamEOFMsg:
		m.eofReached = true
		// If no render is pending, quit immediately
		if !m.renderPending {
			m.storeFinalContent()
			return m, tea.Quit
		}
		// Otherwise, wait for the pending render to complete

	case streamErrorMsg:
		m.eofReached = true
		m.storeFinalContent()
		return m, tea.Quit
	}

	// Update viewport for scrolling
	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
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

	// Use viewport width, falling back to GlamourMaxWidth or 80 if viewport not yet sized
	viewportWidth := m.viewport.Width
	if viewportWidth == 0 {
		viewportWidth = int(m.cfg.GlamourMaxWidth)
		if viewportWidth == 0 {
			viewportWidth = 80
		}
	}
	width := max(0, min(int(m.cfg.GlamourMaxWidth), viewportWidth))
	if width == 0 {
		width = viewportWidth
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

	// Add line numbers if configured
	if m.cfg.ShowLineNumbers {
		lines := strings.Split(out, "\n")
		trunc := lipgloss.NewStyle().MaxWidth(m.viewport.Width - lineNumberWidth).Render

		var content strings.Builder
		for i, s := range lines {
			content.WriteString(lineNumberStyle(fmt.Sprintf("%"+fmt.Sprint(lineNumberWidth)+"d", i+1)))
			content.WriteString(trunc(s))
			if i+1 < len(lines) {
				content.WriteRune('\n')
			}
		}
		return content.String(), nil
	}

	return out, nil
}

func (m *streamingModel) storeFinalContent() {
	m.contentMutex.Lock()
	defer m.contentMutex.Unlock()

	// Store the rendered content for output after TUI exits
	*m.finalContent = m.renderedContent
}

func (m streamingModel) View() string {
	return m.viewport.View()
}
