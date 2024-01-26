package pipeline

import "io"

var _ OutputBuilder = &outputBuilder{}
var _ OutputConfigurationBuilder = &outputBuilder{}
var _ Pipeline = &outputBuilder{}
var _ ReadonlyBuilder = &outputBuilder{}

func newOutputBuilder(in *input) *outputBuilder {

	return &outputBuilder{
		in:    in,
		steps: make([]Processor, 0),
	}
}

func newOutputBuilderFanout() *outputBuilder {
	return &outputBuilder{
		steps: make([]Processor, 0),
	}
}

type input struct {
	// where the data is read from
	processing makeReaderWithSize
	source     consumeReaderWithSize
}

type outputBuilder struct {
	in *input

	// where the data is written to
	outputStep connectorToReader

	preamble string
	appendix string

	compress bool

	steps []Processor

	// completely configured output
	output Reader

	// pipeline to execute
	pipeline func() error
}

// AddReadonlyProcessor implements ReadonlyBuilder.
func (o *outputBuilder) AddReadonlyProcessor(p Processor) ReadonlyBuilder {
	o.AddProcessingStep(p)

	return o
}

// AddProcessingStep implements OutputConfigurationBuilder.
func (b *outputBuilder) AddProcessingStep(p Processor) OutputConfigurationBuilder {
	b.steps = append(b.steps, p)
	return b
}

// Execute implements Pipeline.
func (o *outputBuilder) Execute() error {
	return o.pipeline()
}

// Build implements ConfigurePipelineOutput.
func (o *outputBuilder) Build() Pipeline {
	// configure output steps
	var out Connector = Copy

	if o.compress {
		out = CompressGzip(out)
	}

	// configure input steps
	var input Reader = o.outputStep(out)

	if len(o.appendix) != 0 {
		input = Appendix(input, o.appendix)
	}

	if len(o.preamble) != 0 {
		input = Preamble(input, o.preamble)
	}

	for i := 0; i < len(o.steps); i++ {
		input = o.steps[i](input)
	}

	o.output = input

	// when the output builder is used to configure fanout
	// we can return
	if o.in == nil {
		return o
	}

	var readerWithSize = o.in.processing(input)

	o.pipeline = func() error {
		return o.in.source(readerWithSize)
	}

	return o
}

// Appendix implements ConfigurePipelineOutput.
func (o *outputBuilder) Appendix(appendix string) OutputConfigurationBuilder {
	o.appendix = appendix
	return o
}

// CompressGzip implements ConfigurePipelineOutput.
func (o *outputBuilder) CompressGzip(enable bool) OutputConfigurationBuilder {
	o.compress = enable
	return o
}

// Preamble implements ConfigurePipelineOutput.
func (o *outputBuilder) Preamble(preamble string) OutputConfigurationBuilder {
	o.preamble = preamble
	return o
}

// ReadOnly implements PipelineInput.
func (o *outputBuilder) ReadOnly() ReadonlyBuilder {

	o.outputStep = func(next Connector) Reader {
		return Readonly(next)
	}

	return o
}

// ToFile implements MakeOutputPipeline.
func (o *outputBuilder) ToFile(path string) OutputConfigurationBuilder {
	o.outputStep = func(next Connector) Reader {
		return ToNewFile(path, next)
	}

	return o
}

// ToWriter implements MakeOutputPipeline.
func (o *outputBuilder) ToWriter(w io.Writer) OutputConfigurationBuilder {
	o.outputStep = func(next Connector) Reader {
		return ToWriter(w, next)
	}

	return o
}
