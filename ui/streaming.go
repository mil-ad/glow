package ui

import (
	"bufio"
	"context"
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
	streamChunkMsg string
	streamEOFMsg   struct{}
	streamErrorMsg struct{ err error }
	// mathReadyMsg signals that a background LaTeX render finished and the view
	// should be refreshed to show the now-available image.
	mathReadyMsg struct{}
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

	// Cached glamour renderer, rebuilt only when the wrap width changes.
	renderer      *glamour.TermRenderer
	rendererWidth int

	// math renders LaTeX block math as kitty images when the terminal supports
	// graphics; nil otherwise.
	math *mathRenderer

	// Terminal dimensions
	width  int
	height int

	// Channel for receiving chunks from stdin reader goroutine
	chunkChan <-chan string
	errChan   <-chan error

	// Track state
	eofReached bool
	// flushing is set just before quitting. The inline (non-altscreen) renderer
	// cannot push a frame taller than the terminal into scrollback, so on exit we
	// blank the TUI frame and let the caller reprint the full content instead.
	flushing bool
	// finalOutput receives the full rendered content so the caller can print it
	// to stdout after the program exits (shared pointer, survives model copies).
	finalOutput *string
}

// NewStreamingProgram creates a new streaming-mode Bubble Tea program. It also
// returns an accessor for the final rendered content: the program blanks its
// inline frame on exit, so the caller should print this to stdout afterwards to
// leave the complete render in the terminal's scrollback.
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

	var mr *mathRenderer
	if isKittyTerminal() {
		mr = newMathRenderer(context.Background())
	}

	finalOutput := new(string)
	m := streamingModel{
		cfg:          cfg,
		rawContent:   &strings.Builder{},
		glamViewport: vp,
		math:         mr,
		chunkChan:    chunkChan,
		errChan:      errChan,
		finalOutput:  finalOutput,
	}

	// Render to stderr for inline display
	opts := []tea.ProgramOption{
		tea.WithOutput(os.Stderr),
		tea.WithInput(nil), // No keyboard input needed during streaming
	}

	p := tea.NewProgram(m, opts...)
	// Let background LaTeX renders nudge the program to re-render once their
	// images are ready.
	if mr != nil {
		mr.notify = func() { p.Send(mathReadyMsg{}) }
	}
	return p, func() string { return *finalOutput }
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
		chunk, ok := <-m.chunkChan
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
			m.renderNow()
		}

	case streamChunkMsg:
		m.rawContent.WriteString(string(msg))
		m.renderNow()
		cmds = append(cmds, m.readNextChunk())

	case streamEOFMsg:
		m.eofReached = true
		m.renderNow()
		if !m.mathPending() {
			m.flushing = true
			return m, tea.Quit
		}

	case mathReadyMsg:
		// A formula image became available: re-render to place it, and if we are
		// only still alive to finish rendering math at EOF, quit once done.
		m.renderNow()
		if m.eofReached && !m.mathPending() {
			m.flushing = true
			return m, tea.Quit
		}

	case streamErrorMsg:
		m.eofReached = true
		m.flushing = true
		return m, tea.Quit
	}

	// Update viewport (scrolling etc.)
	var cmd tea.Cmd
	m.glamViewport, cmd = m.glamViewport.Update(msg)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

// mathPending reports whether any LaTeX images are still being fetched.
func (m *streamingModel) mathPending() bool {
	return m.math != nil && m.math.pending()
}

// renderNow renders the accumulated content to the viewport. Markdown rendering
// is synchronous (and cheap with a cached renderer); only LaTeX image fetches
// run in the background, refreshing the view via mathReadyMsg when they land.
func (m *streamingModel) renderNow() {
	content := string(utils.RemoveFrontmatter([]byte(m.rawContent.String())))
	out, err := m.glamourRender(content)
	if err != nil {
		return
	}
	m.glamOutput = out
	m.glamHeight = strings.Count(out, "\n")
	m.glamViewport.SetContent(out)
	m.glamViewport.GotoBottom()
	if m.finalOutput != nil {
		*m.finalOutput = out
	}
}

// wrapWidth is the word-wrap width for the current terminal and config.
func (m *streamingModel) wrapWidth() int {
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
	return width
}

// ensureRenderer builds (or rebuilds) the cached glamour renderer when the wrap
// width changes, avoiding a fresh renderer on every chunk.
func (m *streamingModel) ensureRenderer() error {
	w := m.wrapWidth()
	if m.renderer != nil && m.rendererWidth == w {
		return nil
	}
	options := []glamour.TermRendererOption{
		utils.GlamourStyle(m.cfg.GlamourStyle, false),
		glamour.WithWordWrap(w),
	}
	if m.cfg.PreserveNewLines {
		options = append(options, glamour.WithPreservedNewLines())
	}
	r, err := glamour.NewTermRenderer(options...)
	if err != nil {
		return fmt.Errorf("error creating glamour renderer: %w", err)
	}
	m.renderer = r
	m.rendererWidth = w
	return nil
}

func (m *streamingModel) glamourRender(markdown string) (string, error) {
	if !m.cfg.GlamourEnabled {
		return markdown, nil
	}
	if err := m.ensureRenderer(); err != nil {
		return "", err
	}

	// Swap block math for sentinels before glamour (which has no math support),
	// then substitute kitty placeholder grids back in afterwards.
	renderContent := markdown
	var grids map[int]string
	if m.math != nil {
		processed, formulas := extractBlockMath(markdown)
		renderContent = processed
		grids = m.math.render(formulas)
	}

	out, err := m.renderer.Render(renderContent)
	if err != nil {
		return "", fmt.Errorf("error rendering markdown: %w", err)
	}

	if m.math != nil {
		out = substituteMath(out, grids)
	}

	// Truncate lines to terminal width.
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
	// On exit, blank the TUI frame so the caller can reprint the full content to
	// stdout (the inline renderer can't scroll a too-tall frame into scrollback).
	if m.flushing {
		return ""
	}
	if m.viewportNeeded() {
		return m.glamViewport.View()
	}
	return m.glamOutput
}
