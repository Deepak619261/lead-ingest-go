// Package csvstream reads a CSV one row at a time: memory is O(row), not O(file).
package csvstream

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
)

// sniffBytes is how much of the file we peek at to guess the delimiter.
const sniffBytes = 64 * 1024

var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// ErrEmptyFile is fatal: without a header nothing can be mapped.
var ErrEmptyFile = errors.New("csvstream: file has no header row")

// Row is one data row (the header is not a Row).
type Row struct {
	Num    int      // 1-based data row number; header is row 0
	Line   int      // physical line in the file where the row starts (for error reports)
	Fields []string // safe to keep: owned by this Row, not reused by the reader
	Err    error    // non-nil => malformed row. The import should record it and move on.
}

// Reader streams rows from a CSV source.
type Reader struct {
	cr     *csv.Reader
	header []string
	comma  rune
	rowNum int
}

// NewReader sniffs the delimiter, strips a UTF-8 BOM, and reads the header.
func NewReader(r io.Reader) (*Reader, error) {
	br := bufio.NewReaderSize(r, sniffBytes)

	// Peek looks without consuming, so the csv reader still sees these bytes.
	head, err := br.Peek(sniffBytes)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, bufio.ErrBufferFull) {
		return nil, fmt.Errorf("csvstream: peek: %w", err)
	}
	if bytes.HasPrefix(head, utf8BOM) {
		_, _ = br.Discard(len(utf8BOM)) // Excel adds a BOM; it would corrupt the first header name
		head = head[len(utf8BOM):]
	}
	comma := sniffDelimiter(head)

	cr := csv.NewReader(br)
	cr.Comma = comma
	cr.ReuseRecord = true  // reuse the []string between Read calls => fewer allocations
	cr.FieldsPerRecord = 0 // 0 = "every record must have as many fields as the first one (the header)"
	cr.TrimLeadingSpace = false

	header, err := cr.Read()
	if errors.Is(err, io.EOF) {
		return nil, ErrEmptyFile
	}
	if err != nil {
		return nil, fmt.Errorf("csvstream: header: %w", err)
	}

	return &Reader{
		cr:     cr,
		header: cloneStrings(header), // must copy: ReuseRecord will overwrite this slice
		comma:  comma,
	}, nil
}

// Header returns the header row exactly as in the file.
func (r *Reader) Header() []string { return r.header }

// Comma returns the detected delimiter.
func (r *Reader) Comma() rune { return r.comma }

// Next returns a row (row.Err set if malformed), io.EOF at the end, or a fatal stream error.
func (r *Reader) Next() (Row, error) {
	rec, err := r.cr.Read()
	if errors.Is(err, io.EOF) {
		return Row{}, io.EOF
	}

	r.rowNum++
	line, _ := r.cr.FieldPos(0)
	row := Row{Num: r.rowNum, Line: line}

	var perr *csv.ParseError
	switch {
	case err == nil:
		row.Fields = cloneStrings(rec)
	case errors.As(err, &perr):
		// Row-level problem: record it and keep reading (csv.Reader can continue after a ParseError).
		row.Line = perr.Line
		row.Fields = cloneStrings(rec) // may be nil for quote errors
		row.Err = fmt.Errorf("malformed row at line %d: %w", perr.Line, perr.Err)
	default:
		return Row{}, fmt.Errorf("csvstream: read: %w", err)
	}
	return row, nil
}

// NormalizeHeader is case-insensitive and ignores spaces ("First Name" == "firstname").
func NormalizeHeader(h string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(h), " ", ""))
}

// sniffDelimiter: more ';' than ',' on the first line means semicolon-separated (European Excel).
func sniffDelimiter(head []byte) rune {
	first := head
	if i := bytes.IndexAny(head, "\r\n"); i >= 0 {
		first = head[:i]
	}
	if bytes.Count(first, []byte{';'}) > bytes.Count(first, []byte{','}) {
		return ';'
	}
	return ','
}

// cloneStrings copies the slice because ReuseRecord overwrites it on the next Read.
func cloneStrings(in []string) []string {
	if in == nil {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}
