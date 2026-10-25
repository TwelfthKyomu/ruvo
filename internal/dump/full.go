package dump

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
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

func ParseFull(data []byte) ([]Goroutine, error) {
	ls := lines{data: data}
	p := fullParser{
		in:     Strings{},
		stacks: map[string][]Frame{},
	}
	started := false

	for {
		line, ok := ls.next()
		if !ok {
			break
		}

		if !started {
			if len(line) == 0 {
				continue
			}

			if _, _, ok := splitHeader(line); !ok {
				return nil, fmt.Errorf("dump: not a debug=2 dump: %s", quote(line))
			}

			started = true
		}

		p.line(line)
	}

	p.finish()

	switch {
	case !started:
		return nil, errors.New("dump: empty debug=2 dump")
	case p.n == 0:
		return nil, errors.New("dump: no goroutines in debug=2 dump")
	}

	out := make([]Goroutine, 0, p.n)

	for _, c := range p.chunks {
		out = append(out, c...)
	}

	return out, nil
}

const chunkSize = 1024

type fullParser struct {
	chunks  [][]Goroutine
	n       int
	in      Strings
	stacks  map[string][]Frame
	key     []byte
	frames  []Frame
	cur     Goroutine
	active  bool
	pending []byte
	waiting bool
	created bool
	broken  bool
	origin  bool
}

func (p *fullParser) line(line []byte) {
	if len(line) == 0 {
		p.finish()
		return
	}

	if id, rest, ok := splitHeader(line); ok {
		p.finish()
		p.header(id, rest)

		return
	}

	switch {
	case !p.active:
	case p.origin || bytes.HasPrefix(line, []byte("[originating from goroutine")):
		p.origin = true
	case line[0] == '\t':
		p.fileLine(line)
	case bytes.HasPrefix(line, []byte("created by ")):
		p.createdBy(line)
	case bytes.HasPrefix(line, []byte("...")):
	default:
		if p.waiting {
			p.broken = true
		}

		p.pending = funcName(line)
		p.waiting = true
	}
}

func splitHeader(line []byte) (id int64, rest []byte, ok bool) {
	rest, ok = bytes.CutPrefix(line, []byte("goroutine "))
	if !ok {
		return 0, nil, false
	}

	i := 0
	for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
		i++
	}

	if i == 0 || i > 18 || i == len(rest) || rest[i] != ' ' {
		return 0, nil, false
	}

	n, _ := atoi(rest[:i])
	rest = rest[i:]

	if !bytes.HasPrefix(rest, []byte(" [")) && !bytes.HasPrefix(rest, []byte(" gp=")) {
		return 0, nil, false
	}

	j := bytes.Index(rest, []byte(" ["))
	if j < 0 {
		return 0, nil, false
	}

	return int64(n), rest[j+2:], true
}

func (p *fullParser) header(id int64, rest []byte) {
	inside, _, ok := bytes.Cut(rest, []byte("]"))
	if !ok {
		return
	}

	state, extra, _ := bytes.Cut(inside, []byte(", "))
	p.cur = Goroutine{ID: id, State: p.in.str(state)}

	for len(extra) > 0 {
		var part []byte

		part, extra, _ = bytes.Cut(extra, []byte(", "))

		if m, ok := bytes.CutSuffix(part, []byte(" minutes")); ok {
			if v, ok := atoi(m); ok {
				p.cur.Wait = time.Duration(v) * time.Minute
			}
		}
	}

	p.active = true
}

func (p *fullParser) fileLine(line []byte) {
	file, n, ok := parseFileLine(line)
	if !ok {
		p.broken = true
		return
	}

	switch {
	case p.created:
		p.cur.CreatedBy.File = p.in.str(file)
		p.cur.CreatedBy.Line = n
		p.created = false
	case p.waiting:
		p.frames = append(p.frames, Frame{Func: p.in.str(p.pending), File: p.in.str(file), Line: n})
		p.waiting = false
	}
}

func (p *fullParser) createdBy(line []byte) {
	rest := line[len("created by "):]
	fn, id, _ := bytes.Cut(rest, []byte(" in goroutine "))

	p.cur.CreatedBy = Frame{Func: p.in.str(fn)}

	if n, ok := atoi(bytes.TrimSpace(id)); ok {
		p.cur.Creator = int64(n)
	}

	p.created = true
}

func (p *fullParser) finish() {
	if p.active && !p.broken && !p.waiting && len(p.frames) > 0 {
		p.cur.Frames = p.intern(p.frames)
		p.add(p.cur)
	}

	p.frames = p.frames[:0]
	p.active = false
	p.waiting = false
	p.created = false
	p.broken = false
	p.origin = false
}

func (p *fullParser) add(g Goroutine) {
	last := len(p.chunks) - 1
	if last < 0 || len(p.chunks[last]) == chunkSize {
		p.chunks = append(p.chunks, make([]Goroutine, 0, chunkSize))
		last++
	}

	p.chunks[last] = append(p.chunks[last], g)
	p.n++
}

func (p *fullParser) intern(frames []Frame) []Frame {
	p.key = p.key[:0]

	for _, f := range frames {
		p.key = append(p.key, f.Func...)
		p.key = append(p.key, 0)
		p.key = append(p.key, f.File...)
		p.key = strconv.AppendInt(append(p.key, 0), int64(f.Line), 10)
		p.key = append(p.key, '\n')
	}

	if s, ok := p.stacks[string(p.key)]; ok {
		return s
	}

	s := append([]Frame(nil), frames...)
	p.stacks[string(p.key)] = s

	return s
}

func funcName(line []byte) []byte {
	if !bytes.HasSuffix(line, []byte(")")) {
		return line
	}

	if i := bytes.LastIndexByte(line, '('); i > 0 {
		return line[:i]
	}

	return line
}
