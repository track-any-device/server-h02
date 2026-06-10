package protocol

import (
	"bytes"
	"fmt"
	"strings"
)

// ParseFrame parses an H02 ASCII frame from a raw string.
// The raw string must include both the leading '*' and the trailing '#'.
// Returns an error if the frame is malformed or the terminator is missing.
func ParseFrame(raw string) (*Frame, error) {
	s := strings.Trim(raw, "\r\n \t")

	if !strings.HasSuffix(s, "#") {
		return nil, fmt.Errorf("h02: missing '#' terminator in %q", truncate(s, 64))
	}
	if !strings.HasPrefix(s, "*") {
		return nil, fmt.Errorf("h02: missing '*' prefix in %q", truncate(s, 64))
	}

	// Strip leading '*' and trailing '#'.
	inner := s[1 : len(s)-1]
	parts := strings.Split(inner, ",")
	if len(parts) < 3 {
		return nil, fmt.Errorf("h02: frame too short (need at least HQ,IMEI,CMD): %q", truncate(s, 64))
	}
	if parts[0] != "HQ" {
		return nil, fmt.Errorf("h02: expected HQ header, got %q", parts[0])
	}
	imei := parts[1]
	if imei == "" {
		return nil, fmt.Errorf("h02: empty IMEI in frame")
	}

	return &Frame{
		IMEI:   imei,
		Cmd:    parts[2],
		Fields: parts[3:],
		Raw:    s,
	}, nil
}

// SplitOnHash is a bufio.SplitFunc that returns tokens ending with '#'.
// Used by the TCP scanner to frame H02 messages from the byte stream.
func SplitOnHash(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '#'); i >= 0 {
		// Return data up to and including '#'.
		return i + 1, data[:i+1], nil
	}
	if atEOF {
		// Stream ended without '#'; discard remaining bytes.
		return len(data), nil, nil
	}
	// Need more data.
	return 0, nil, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
