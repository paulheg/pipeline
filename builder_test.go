package pipeline_test

import (
	"io"
	"strings"
	"testing"

	"github.com/paulheg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestGobEncoding(t *testing.T) {

	const input = "Hello,World\nMessage,Text"
	const expected = "Hello.World\nMessage.Text\n"

	reader := strings.NewReader(input)
	var out strings.Builder

	p := pipeline.Build().
		FromReader(reader, reader.Size()).
		ParseLinesToGob(func(line string) (interface{}, error) {
			return strings.Split(line, ","), nil
		}).
		ToWriter(&out).
		AddProcessingStep(pipeline.DecodeGob[[]string](func(s *[]string) []byte {
			return []byte(strings.Join(*s, ".") + "\n")
		})).
		Build()

	err := p.Execute()

	assert.Nil(t, err)
	assert.Equal(t, expected, out.String())
}

func TestJsonEncoding(t *testing.T) {

	const input = "Hello,World\nMessage,Text"
	const expected = "Hello.World\nMessage.Text\n"

	reader := strings.NewReader(input)
	var out strings.Builder

	p := pipeline.Build().
		FromReader(reader, reader.Size()).
		ParseLinesToJson(func(line string) (interface{}, error) {
			return strings.Split(line, ","), nil
		}).
		ToWriter(&out).
		AddProcessingStep(pipeline.DecodeJson[[]string](func(s *[]string) []byte {
			return []byte(strings.Join(*s, ".") + "\n")
		})).
		Build()

	err := p.Execute()

	assert.Nil(t, err)
	assert.Equal(t, expected, out.String())
}

func TestFanout(t *testing.T) {
	const text = "Test"
	const preamble = "lel"
	const appendix = "lul"
	reader := strings.NewReader(text)

	var out1 strings.Builder
	var out2 strings.Builder

	pipe := pipeline.Build().
		FromReader(reader, int64(reader.Len())).
		Fanout().
		Register(func(output pipeline.OutputBuilder) pipeline.Pipeline {
			return output.
				ToWriter(&out1).
				Preamble(preamble).
				Appendix(appendix).
				Build()
		}).
		Register(func(output pipeline.OutputBuilder) pipeline.Pipeline {
			return output.
				ToWriter(&out2).
				Build()
		}).Build()

	err := pipe.Execute()

	assert.Nil(t, err)
	assert.Equal(t, preamble+text+appendix, out1.String())
	assert.Equal(t, text, out2.String())
}

func TestFanoutWithGobAndProcessorSteps(t *testing.T) {

	const lines = 5
	text := strings.Repeat("test,test,test\n", lines)

	reader := strings.NewReader(text)

	var out1 strings.Builder
	var out2 strings.Builder

	pipe := pipeline.Build().
		FromReader(reader, int64(reader.Len())).
		ParseLinesToGob(func(line string) (interface{}, error) {
			return strings.Split(line, ","), nil
		}).
		Fanout().
		Register(func(output pipeline.OutputBuilder) pipeline.Pipeline {
			return output.
				ToWriter(&out1).
				AddProcessingStep(pipeline.DecodeGob[[]string](func(s *[]string) []byte {
					return []byte(strings.Join(*s, ",") + "\n")
				})).Build()
		}).
		Register(func(output pipeline.OutputBuilder) pipeline.Pipeline {
			return output.
				ToWriter(&out2).
				AddProcessingStep(pipeline.DecodeGob[[]string](func(s *[]string) []byte {
					return []byte(strings.Join(*s, ".") + "\n")
				})).Build()
		}).Build()

	err := pipe.Execute()

	assert.Nil(t, err)
	assert.Equal(t, strings.Repeat("test,test,test\n", lines), out1.String())
	assert.Equal(t, strings.Repeat("test.test.test\n", lines), out2.String())
}

func TestFanoutWithGobAndProcessorStepsAndStructs(t *testing.T) {

	const lines = 5
	text := strings.Repeat("Hello,World,This.Is.A.Long.List\n", lines)

	type payload struct {
		Title    string
		Subtitle string
		List     []string
	}

	decode := func(input string) payload {
		values := strings.Split(input, ",")
		return payload{
			Title:    values[0],
			Subtitle: values[1],
			List:     strings.Split(values[2], "."),
		}
	}

	reader := strings.NewReader(text)

	var out1 strings.Builder
	var out2 strings.Builder

	pipe := pipeline.Build().
		FromReader(reader, int64(reader.Len())).
		ParseLinesToGob(func(line string) (interface{}, error) {
			return decode(line), nil
		}).
		Fanout().
		Register(func(output pipeline.OutputBuilder) pipeline.Pipeline {
			return output.
				ToWriter(&out1).
				AddProcessingStep(pipeline.DecodeGob[payload](func(p *payload) []byte {
					assert.NotEmpty(t, p.List)
					return []byte{}
				})).Build()
		}).
		Register(func(output pipeline.OutputBuilder) pipeline.Pipeline {
			return output.
				ToWriter(&out2).
				AddProcessingStep(pipeline.DecodeGob[payload](func(p *payload) []byte {
					assert.NotEmpty(t, p.List)
					return []byte{}
				})).Build()
		}).Build()

	err := pipe.Execute()

	assert.Nil(t, err)
}

func TestReadonly(t *testing.T) {
	const text = "1\n2\n3"
	reader := strings.NewReader(text)

	counter := 0

	pipe := pipeline.Build().
		FromReader(reader, reader.Size()).
		ParseLines(func(line string) ([]byte, error) {
			counter++
			return []byte(line), nil
		}).
		ReadOnly().Build()

	err := pipe.Execute()
	assert.Nil(t, err)
	assert.Equal(t, 3, counter)
}

func TestFanout_WithLineParser(t *testing.T) {
	const text = "Test"
	const preamble = "lel"
	reader := strings.NewReader(text)

	var out1 strings.Builder
	var out2 strings.Builder

	pipe := pipeline.Build().
		FromReader(reader, int64(reader.Len())).
		ParseLines(func(line string) ([]byte, error) {
			return []byte(line + "er"), nil
		}).
		Fanout().
		Register(func(output pipeline.OutputBuilder) pipeline.Pipeline {
			return output.
				ToWriter(&out1).
				Preamble(preamble).
				Build()
		}).
		Register(func(output pipeline.OutputBuilder) pipeline.Pipeline {
			return output.
				ToWriter(&out2).
				Build()
		}).Build()

	err := pipe.Execute()

	assert.Nil(t, err)
	assert.Equal(t, preamble+text+"er", out1.String())
	assert.Equal(t, text+"er", out2.String())
}

func TestPreambleAndAppendix(t *testing.T) {
	testCases := []struct {
		desc     string
		input    string
		preamble string
		appendix string
		expected string
	}{
		{
			desc:     "preamble filled",
			preamble: "hello\ntest\n",
			input:    "world",
			expected: "hello\ntest\nworlda",
		},
		{
			desc:     "appendix filled",
			input:    "world",
			appendix: "hello\n!",
			expected: "worldahello\n!",
		},
		{
			desc:     "appendix and preamble filled",
			preamble: "hello",
			input:    "world",
			appendix: "!",
			expected: "helloworlda!",
		},
		{
			desc:     "empty",
			preamble: "",
			input:    "world",
			appendix: "",
			expected: "worlda",
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			r := strings.NewReader(tC.input)
			var b strings.Builder

			err := pipeline.Build().FromReader(r, r.Size()).
				ParseLines(func(line string) ([]byte, error) {
					return []byte(line + "a"), nil
				}).
				ToWriter(&b).
				Preamble(tC.preamble).
				Appendix(tC.appendix).
				Build().Execute()

			assert.Nil(t, err)
			assert.Equal(t, tC.expected, b.String())
		})
	}
}

func BenchmarkBuilder(b *testing.B) {

	var size int = 100 * 100 * 100
	reader := strings.NewReader(strings.Repeat("a", size))
	var builder strings.Builder

	p := pipeline.Build().FromReader(reader, int64(size)).ToWriter(&builder).Build()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p.Execute()
	}
}

func BenchmarkDefaultIO(b *testing.B) {

	var size int = 100 * 100 * 100
	reader := strings.NewReader(strings.Repeat("a", size))
	var builder strings.Builder

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		io.Copy(&builder, reader)
	}
}

func BenchmarkFunctional(b *testing.B) {

	var size int = 100 * 100 * 100
	reader := strings.NewReader(strings.Repeat("a", size))
	var builder strings.Builder

	for i := 0; i < b.N; i++ {
		pipeline.FromReader(reader, int64(size),
			pipeline.IgnoreSize(
				pipeline.ToWriter(&builder, pipeline.Copy)))
	}
}
