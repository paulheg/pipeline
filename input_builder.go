package pipeline

import (
	"encoding/gob"
	"encoding/json"
	"io"
)

var _ InputBuilder = &inputBuilder{}
var _ Builder = &inputBuilder{}

type inputBuilder struct {
	inputStrategyWithSize consumeReaderWithSize
	progressBar           ProgressBarRegistrator

	lineParser LineParser[[]byte]

	parser  LineParser[interface{}]
	encoder NewEncoder

	gzipDecompress bool
}

func (i *inputBuilder) build(next Reader) ReaderWithSize {

	if i.encoder != nil && i.parser != nil {
		next = ParseLineToCustomEncoder(i.encoder, next, i.parser)
	}

	if i.lineParser != nil {
		next = ParseLine(next, i.lineParser)
	}

	if i.gzipDecompress {
		next = DecompressGzip(next)
	}

	// configure progress bar
	var runWithSize ReaderWithSize

	if i.progressBar != nil {
		runWithSize = ProgressBar(i.progressBar, next)
	} else {
		runWithSize = IgnoreSize(next)
	}

	return runWithSize
}

// ParseLinesToCustomEncoder implements InputBuilder.
func (i *inputBuilder) ParseLinesToCustomEncoder(encoder NewEncoder, parser LineParser[interface{}]) InputBuilder {
	i.encoder = encoder
	i.parser = parser

	return i
}

// ParseLinesToJson implements InputBuilder.
func (i *inputBuilder) ParseLinesToJson(parser LineParser[interface{}]) InputBuilder {
	i.encoder = func(w io.Writer) Encoder {
		return json.NewEncoder(w)
	}
	i.parser = parser

	return i
}

// ParseLinesToGob implements InputBuilder.
func (i *inputBuilder) ParseLinesToGob(parser LineParser[interface{}]) InputBuilder {
	i.encoder = func(w io.Writer) Encoder {
		return gob.NewEncoder(w)
	}
	i.parser = parser

	return i
}

// FromFile implements PipelineBuilder.
func (i *inputBuilder) FromFile(path string) InputBuilder {
	i.inputStrategyWithSize = func(next ReaderWithSize) error {
		return FromFile(path, next)
	}

	return i
}

// FromReader implements PipelineBuilder.
func (i *inputBuilder) FromReader(r io.Reader, size int64) InputBuilder {
	i.inputStrategyWithSize = func(next ReaderWithSize) error {
		return FromReader(r, size, next)
	}

	return i
}

// FromWeb implements PipelineBuilder.
func (i *inputBuilder) FromWeb(url string) InputBuilder {
	i.inputStrategyWithSize = func(next ReaderWithSize) error {
		return FromWeb(url, next)
	}

	return i
}

// DecompressGzip implements PipelineInput.
func (i *inputBuilder) DecompressGzip(enable bool) InputBuilder {
	i.gzipDecompress = enable
	return i
}

// ParseLines implements PipelineInput.
func (i *inputBuilder) ParseLines(parser LineParser[[]byte]) InputBuilder {
	i.lineParser = parser
	return i
}

// ProgressBar implements PipelineInput.
func (i *inputBuilder) ProgressBar(register ProgressBarRegistrator) InputBuilder {
	i.progressBar = register
	return i
}

// Fanout implements PipelineInput.
func (i *inputBuilder) Fanout() FanoutBuilder {
	f := newFanoutBuilder(&input{
		processing: i.build,
		source:     i.inputStrategyWithSize,
	})

	return f
}

// ReadOnly implements PipelineInput.
func (i *inputBuilder) ReadOnly() ReadonlyBuilder {
	r := newReadonlyBuilder(input{
		processing: i.build,
		source:     i.inputStrategyWithSize,
	})

	return r
}

// ToFile implements PipelineInput.
func (i *inputBuilder) ToFile(path string) OutputConfigurationBuilder {
	out := newOutputBuilder(&input{
		processing: i.build,
		source:     i.inputStrategyWithSize,
	})
	out.ToFile(path)

	return out
}

// ToWriter implements PipelineInput.
func (i *inputBuilder) ToWriter(w io.Writer) OutputConfigurationBuilder {
	out := newOutputBuilder(&input{
		processing: i.build,
		source:     i.inputStrategyWithSize,
	})
	out.ToWriter(w)

	return out
}
