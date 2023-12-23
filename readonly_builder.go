package pipeline

import "io"

var _ Pipeline = &readonlyBuilder{}
var _ ReadonlyBuilder = &readonlyBuilder{}

func newReadonlyBuilder(in input) *readonlyBuilder {
	return &readonlyBuilder{
		input: in,
	}
}

type readonlyBuilder struct {
	input

	pipeline func() error
}

// Build implements ReadonlyPipeline.
func (p *readonlyBuilder) Build() Pipeline {
	// configure discard steps
	// read the end into nothing
	// data copying can happen in the ParseLine
	var discard Reader = func(r io.Reader) error {
		_, err := io.Copy(io.Discard, r)
		return err
	}

	var a = p.processing(discard)

	p.pipeline = func() error {
		return p.source(a)
	}

	return p
}

// Execute implements Pipeline.
func (r *readonlyBuilder) Execute() error {
	return r.pipeline()
}
