package dump

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

type Goroutine struct {
	ID        int64
	State     string
	Wait      time.Duration
	Frames    []Frame
	CreatedBy Frame
	Creator   int64
}

func ParseFull(r io.Reader) ([]Goroutine, error) {
	sc := newScanner(r)
	p := fullParser{}
	started := false

	for sc.Scan() {
		line := sc.Text()

		if !started {
			if line == "" {
				continue
			}

			if !isHeader(line) {
				return nil, fmt.Errorf("dump: not a debug=2 dump: %s", quote(line))
			}

			started = true
		}

		p.line(line)
	}

	if err := sc.Err(); err != nil {
		return nil, err
	}

	p.finish()

	return p.out, nil
}

type fullParser struct {
	out     []Goroutine
	cur     *Goroutine
	pending string
	created bool
	broken  bool
	origin  bool
}

func (p *fullParser) line(line string) {
	switch {
	case line == "":
		p.finish()
	case isHeader(line):
		p.finish()
		p.header(line)
	case p.cur == nil:
	case p.origin || strings.HasPrefix(line, "[originating from goroutine"):
		p.origin = true
	case strings.HasPrefix(line, "\t"):
		p.fileLine(line)
	case strings.HasPrefix(line, "created by "):
		p.createdBy(line)
	case strings.HasPrefix(line, "..."):
	default:
		if p.pending != "" {
			p.broken = true
		}

		p.pending = funcName(line)
	}
}

func (p *fullParser) header(line string) {
	rest := strings.TrimPrefix(line, "goroutine ")

	id, rest, ok := strings.Cut(rest, " [")
	if !ok {
		return
	}

	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return
	}

	inside, _, ok := strings.Cut(rest, "]")
	if !ok {
		return
	}

	parts := strings.Split(inside, ", ")
	g := Goroutine{ID: n, State: parts[0]}

	for _, part := range parts[1:] {
		if m, ok := strings.CutSuffix(part, " minutes"); ok {
			if v, err := strconv.Atoi(m); err == nil {
				g.Wait = time.Duration(v) * time.Minute
			}
		}
	}

	p.cur = &g
	p.pending = ""
	p.created = false
	p.broken = false
	p.origin = false
}

func (p *fullParser) fileLine(line string) {
	file, n, ok := parseFileLine(line)
	if !ok {
		p.broken = true
		return
	}

	switch {
	case p.created:
		p.cur.CreatedBy.File = file
		p.cur.CreatedBy.Line = n
		p.created = false
	case p.pending != "":
		p.cur.Frames = append(p.cur.Frames, Frame{Func: p.pending, File: file, Line: n})
		p.pending = ""
	}
}

func (p *fullParser) createdBy(line string) {
	rest := strings.TrimPrefix(line, "created by ")
	fn, id, _ := strings.Cut(rest, " in goroutine ")

	p.cur.CreatedBy = Frame{Func: fn}

	if n, err := strconv.ParseInt(strings.TrimSpace(id), 10, 64); err == nil {
		p.cur.Creator = n
	}

	p.created = true
}

func (p *fullParser) finish() {
	if p.cur != nil && !p.broken && p.pending == "" && len(p.cur.Frames) > 0 {
		p.out = append(p.out, *p.cur)
	}

	p.cur = nil
	p.pending = ""
	p.created = false
	p.broken = false
	p.origin = false
}

func isHeader(line string) bool {
	rest, ok := strings.CutPrefix(line, "goroutine ")
	if !ok {
		return false
	}

	id, _, ok := strings.Cut(rest, " [")
	if !ok || id == "" {
		return false
	}

	_, err := strconv.ParseInt(id, 10, 64)

	return err == nil
}

func funcName(line string) string {
	if !strings.HasSuffix(line, ")") {
		return line
	}

	if i := strings.LastIndexByte(line, '('); i > 0 {
		return line[:i]
	}

	return line
}
