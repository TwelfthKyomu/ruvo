package dump

import (
	"bytes"
	"errors"
	"fmt"
)

type Record struct {
	Count  int
	Frames []Frame
}

type Profile struct {
	Total   int
	Records []Record
}

func ParseStacks(data []byte) (Profile, error) {
	var in Strings

	return in.ParseStacks(data)
}

func (in *Strings) ParseStacks(data []byte) (Profile, error) {
	ls := lines{data: data}

	first, ok := ls.next()
	if !ok {
		return Profile{}, errors.New("dump: empty profile")
	}

	total, ok := parseTotal(first)
	if !ok {
		return Profile{}, fmt.Errorf("dump: not a debug=1 profile: %s", quote(first))
	}

	if total < 0 {
		return Profile{}, fmt.Errorf("dump: negative goroutine total %d", total)
	}

	p := Profile{Total: total}
	strs := in.ready()
	cur := -1
	sum := 0

	var frames []Frame

	flush := func() {
		if cur >= 0 && len(frames) > 0 {
			p.Records[cur].Frames = append([]Frame(nil), frames...)
		}

		frames = frames[:0]
	}

	for {
		line, ok := ls.next()
		if !ok {
			break
		}

		switch {
		case len(line) == 0:
			flush()
			cur = -1
		case line[0] == '#':
			if cur < 0 || bytes.HasPrefix(line, []byte("# labels:")) {
				continue
			}

			if f, ok := parseStackFrame(line, strs); ok {
				frames = append(frames, f)
			}
		default:
			n, ok := parseCount(line)
			if !ok {
				continue
			}

			if n < 1 {
				return Profile{}, fmt.Errorf("dump: bad record count: %s", quote(line))
			}

			flush()

			sum += n
			p.Records = append(p.Records, Record{Count: n})
			cur = len(p.Records) - 1
		}
	}

	flush()

	if sum != total {
		return Profile{}, fmt.Errorf("dump: incomplete profile: %d of %d goroutines", sum, total)
	}

	return p, nil
}

func parseTotal(line []byte) (int, bool) {
	name, n, ok := bytes.Cut(line, []byte(" profile: total "))
	if !ok || (string(name) != "goroutine" && string(name) != "goroutineleak") {
		return 0, false
	}

	return signedAtoi(bytes.TrimSpace(n))
}

func parseCount(line []byte) (int, bool) {
	n, rest, ok := bytes.Cut(line, []byte(" @"))
	if !ok || (len(rest) > 0 && rest[0] != ' ') {
		return 0, false
	}

	return signedAtoi(n)
}

func parseStackFrame(line []byte, in Strings) (Frame, bool) {
	var fields [4][]byte

	n := 0

	for len(line) > 0 && n < 4 {
		i := bytes.IndexByte(line, '\t')

		var f []byte
		if i < 0 {
			f, line = line, nil
		} else {
			f, line = line[:i], line[i+1:]
		}

		if len(f) > 0 {
			fields[n] = f
			n++
		}
	}

	if n < 4 {
		return Frame{}, false
	}

	fn := fields[2]

	if i := bytes.LastIndex(fn, []byte("+0x")); i >= 0 {
		fn = fn[:i]
	}

	file, ln, ok := parseFileLine(fields[3])
	if !ok {
		return Frame{}, false
	}

	return Frame{Func: in.str(fn), File: in.str(file), Line: ln}, true
}
