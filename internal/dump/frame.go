package dump

import (
	"bufio"
	"fmt"
	"io"
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

func parseFileLine(s string) (string, int, bool) {
	s = strings.TrimSpace(s)

	if i := strings.Index(s, " +0x"); i >= 0 {
		s = s[:i]
	}

	i := strings.LastIndexByte(s, ':')
	if i < 0 {
		return "", 0, false
	}

	n, err := strconv.Atoi(s[i+1:])
	if err != nil {
		return "", 0, false
	}

	return s[:i], n, true
}

func newScanner(r io.Reader) *bufio.Scanner {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1<<20)

	return sc
}

func quote(s string) string {
	if len(s) > 60 {
		s = s[:60] + "…"
	}

	return strconv.Quote(s)
}
