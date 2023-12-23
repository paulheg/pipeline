package pipeline

var _ FanoutBuilder = &fanoutBuilder{}
var _ Pipeline = &fanoutBuilder{}

func newFanoutBuilder(in *input) *fanoutBuilder {
	return &fanoutBuilder{
		in: in,
	}
}

type fanoutBuilder struct {
	in *input

	reader []Reader

	pipeline func() error
}

// Execute implements Pipeline.
func (f *fanoutBuilder) Execute() error {
	return f.pipeline()
}

// Build implements FanoutPipeline.
func (f *fanoutBuilder) Build() Pipeline {

	var readerWithSize = f.in.processing(MultiProcess(f.reader...))

	f.pipeline = func() error {
		return f.in.source(readerWithSize)
	}

	return f
}

// Register implements FanoutPipeline.
func (f *fanoutBuilder) Register(o func(output OutputBuilder) Pipeline) FanoutBuilder {
	builder := newOutputBuilderFanout()
	o(builder)

	f.reader = append(f.reader, builder.output)

	return f
}
