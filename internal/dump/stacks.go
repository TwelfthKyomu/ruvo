package dump

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Record struct {
	Count  int
	Frames []Frame
}

type Profile struct {
	Total   int
	Records []Record
}

func ParseStacks(r io.Reader) (Profile, error) {
	sc := newScanner(r)

	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return Profile{}, err
		}

		return Profile{}, errors.New("dump: empty profile")
	}

	total, ok := parseTotal(sc.Text())
	if !ok {
		return Profile{}, fmt.Errorf("dump: not a debug=1 profile: %s", quote(sc.Text()))
	}

	if total < 0 {
		return Profile{}, fmt.Errorf("dump: negative goroutine total %d", total)
	}

	p := Profile{Total: total}
	cur := -1
	sum := 0

	for sc.Scan() {
		line := sc.Text()

		switch {
		case line == "":
			cur = -1
		case strings.HasPrefix(line, "# labels:"):
		case strings.HasPrefix(line, "#"):
			if f, ok := parseStackFrame(line); ok && cur >= 0 {
				p.Records[cur].Frames = append(p.Records[cur].Frames, f)
			}
		default:
			if n, ok := parseCount(line); ok {
				if n < 1 {
					return Profile{}, fmt.Errorf("dump: bad record count: %s", quote(line))
				}

				sum += n
				p.Records = append(p.Records, Record{Count: n})
				cur = len(p.Records) - 1
			}
		}
	}

	if err := sc.Err(); err != nil {
		return Profile{}, err
	}

	if sum != total {
		return Profile{}, fmt.Errorf("dump: incomplete profile: %d of %d goroutines", sum, total)
	}

	return p, nil
}

func parseTotal(line string) (int, bool) {
	name, n, ok := strings.Cut(line, " profile: total ")
	if !ok || (name != "goroutine" && name != "goroutineleak") {
		return 0, false
	}

	total, err := strconv.Atoi(strings.TrimSpace(n))

	return total, err == nil
}

func parseCount(line string) (int, bool) {
	n, rest, ok := strings.Cut(line, " @")
	if !ok || (rest != "" && rest[0] != ' ') {
		return 0, false
	}

	count, err := strconv.Atoi(n)

	return count, err == nil
}

func parseStackFrame(line string) (Frame, bool) {
	var fields []string

	for _, f := range strings.Split(line, "\t") {
		if f != "" {
			fields = append(fields, f)
		}
	}

	if len(fields) < 4 {
		return Frame{}, false
	}

	fn := fields[2]

	if i := strings.LastIndex(fn, "+0x"); i >= 0 {
		fn = fn[:i]
	}

	file, n, ok := parseFileLine(fields[3])
	if !ok {
		return Frame{}, false
	}

	return Frame{Func: fn, File: file, Line: n}, true
}
