package readers

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"time"
)

type AzureFunctionsReader struct {
	*BaseReader

	backend *csv.Reader
	cursor  int
	fields  map[string]int
}

func NewAzureFunctionsReader(rd io.Reader) *AzureFunctionsReader {
	reader := &AzureFunctionsReader{
		BaseReader: NewBaseReader(),
		backend:    csv.NewReader(bufio.NewReader(rd)),
		fields:     make(map[string]int),
	}
	return reader
}

func (reader *AzureFunctionsReader) Read() (*Record, error) {
	if reader.cursor == 0 {
		// Skip first line
		fields, err := reader.backend.Read()
		if err != nil {
			return nil, err
		}

		reader.cursor++
		for i, field := range fields {
			reader.fields[field] = i
		}
	}

	line, err := reader.backend.Read()
	if err != nil {
		return nil, err
	}

	rec, _ := reader.BaseReader.Read()
	reader.cursor++

	rec.Key = reader.readField(line, "AnonBlobETag")
	sz, szErr := strconv.ParseFloat(reader.readField(line, "BlobBytes"), 64)
	if szErr == nil {
		rec.Size = uint64(sz)
	}
	ts, tsErr := strconv.ParseInt(reader.readField(line, "Timestamp"), 10, 64)
	if tsErr == nil {
		rec.Timestamp = ts * int64(time.Millisecond)
	}

	if szErr != nil || tsErr != nil {
		rec.Error = fmt.Errorf("error on parse record, skip line %d: %v(%v, %v)", reader.cursor, line, szErr, tsErr)
	}
	return rec, nil
}

func (reader *AzureFunctionsReader) Report() []string {
	return nil
}

func (reader *AzureFunctionsReader) readField(line []string, field string) string {
	return line[reader.fields[field]]
}
