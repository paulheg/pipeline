package pipeline

import (
	"io"
)

func Build() Builder {
	return &inputBuilder{}
}

type Builder interface {
	FromFile(path string) InputBuilder
	FromWeb(url string) InputBuilder
	FromReader(r io.Reader, size int64) InputBuilder
}

type InputBuilder interface {
	// Enable decompression of the input with gzip
	DecompressGzip(enable bool) InputBuilder

	// Add a ProgressBar to the pipeline
	// The io.Writer will get updated
	ProgressBar(register ProgressBarRegistrator) InputBuilder

	OutputBuilder

	// parsing
	ParseLines(parser LineParser[[]byte]) InputBuilder
	ParseLinesToGob(parser LineParser[interface{}]) InputBuilder
	ParseLinesToJson(parser LineParser[interface{}]) InputBuilder
	ParseLinesToCustomEncoder(encoder NewEncoder, parser LineParser[interface{}]) InputBuilder

	Fanout() FanoutBuilder

	// Only read the pipeline
	ReadOnly() ReadonlyBuilder
}

type ReadonlyBuilder interface {
	Build() Pipeline
}

type OutputBuilder interface {
	// Output the pipeline into a file
	ToFile(path string) OutputConfigurationBuilder

	// Output the pipeline into any io.Writer
	ToWriter(w io.Writer) OutputConfigurationBuilder
}

type OutputConfigurationBuilder interface {
	Preamble(preamble string) OutputConfigurationBuilder
	Appendix(appendix string) OutputConfigurationBuilder
	CompressGzip(enable bool) OutputConfigurationBuilder
	AddProcessingStep(p Processor) OutputConfigurationBuilder
	Build() Pipeline
}

type FanoutBuilder interface {
	Register(func(output OutputBuilder) Pipeline) FanoutBuilder
	Build() Pipeline
}

type Pipeline interface {
	Execute() error
}

type readerStep (func(next Reader) Reader)
type consumeReaderWithSize func(next ReaderWithSize) error
type consumeReader func(next Reader) error

type connectorToReader func(next Connector) Reader

type makeReaderWithSize func(next Reader) ReaderWithSize
