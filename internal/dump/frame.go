package dump

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type Frame struct {
	Func string
	File string
	Line int
}

func (f Frame) String() string {
	if f.Func == "" {
		return ""
	}

	return fmt.Sprintf("%s %s:%d", f.Func, base(f.File), f.Line)
}

func base(file string) string {
	if i := strings.LastIndexAny(file, `/\`); i >= 0 {
		return file[i+1:]
	}

	return file
}

func parseFileLine(s []byte) ([]byte, int, bool) {
	s = bytes.TrimSpace(s)

	if i := bytes.Index(s, []byte(" +0x")); i >= 0 {
		s = s[:i]
	}

	i := bytes.LastIndexByte(s, ':')
	if i < 0 {
		return nil, 0, false
	}

	n, ok := atoi(s[i+1:])
	if !ok {
		return nil, 0, false
	}

	return s[:i], n, true
}

func atoi(b []byte) (int, bool) {
	if len(b) == 0 || len(b) > 18 {
		return 0, false
	}

	n := 0

	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}

		n = n*10 + int(c-'0')
	}

	return n, true
}

func signedAtoi(b []byte) (int, bool) {
	if len(b) > 0 && b[0] == '-' {
		n, ok := atoi(b[1:])

		return -n, ok
	}

	if len(b) > 0 && b[0] == '+' {
		b = b[1:]
	}

	return atoi(b)
}

type lines struct {
	data []byte
}

func (l *lines) next() ([]byte, bool) {
	if len(l.data) == 0 {
		return nil, false
	}

	var line []byte

	if i := bytes.IndexByte(l.data, '\n'); i >= 0 {
		line, l.data = l.data[:i], l.data[i+1:]
	} else {
		line, l.data = l.data, nil
	}

	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}

	return line, true
}

const stringsMax = 50_000

type Strings map[string]string

func (in Strings) str(b []byte) string {
	if s, ok := in[string(b)]; ok {
		return s
	}

	s := string(b)
	in[s] = s

	return s
}

func (in *Strings) ready() Strings {
	if *in == nil || len(*in) >= stringsMax {
		*in = Strings{}
	}

	return *in
}

func quote(b []byte) string {
	s := string(b[:min(len(b), 61)])
	if len(b) > 60 {
		s = s[:60] + "…"
	}

	return strconv.Quote(s)
}
