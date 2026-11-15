package capture

import (
	"bufio"
	"context"
	"os"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/emilyzyy/LLM-eval-harness/gateway/gen/go/capturepb"
)

// JSONLSink appends each record to a file as one line of JSON. It is meant for
// looking at captures on a laptop without running Kafka.
//
// The JSON is the standard protobuf JSON mapping, so the two body fields are
// base64 strings. To read one:
//
//	tail -n 1 captures.jsonl | jq -r .responseBody | base64 -d
type JSONLSink struct {
	file   *os.File
	writer *bufio.Writer
}

// NewJSONLSink opens path for appending, creating it if needed.
func NewJSONLSink(path string) (*JSONLSink, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &JSONLSink{file: file, writer: bufio.NewWriter(file)}, nil
}

func (s *JSONLSink) Write(_ context.Context, rec *capturepb.CaptureRecord) error {
	line, err := protojson.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := s.writer.Write(line); err != nil {
		return err
	}
	if err := s.writer.WriteByte('\n'); err != nil {
		return err
	}
	// Flush after every record so `tail -f` shows it straight away.
	return s.writer.Flush()
}

func (s *JSONLSink) Flush(context.Context) error {
	if err := s.writer.Flush(); err != nil {
		return err
	}
	return s.file.Sync()
}

func (s *JSONLSink) Close() error { return s.file.Close() }
