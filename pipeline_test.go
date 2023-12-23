package pipeline_test

import (
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/paulheg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestMultiReader(t *testing.T) {

	r := strings.NewReader("hello,world")

	var w1 strings.Builder
	var w2 strings.Builder

	err := pipeline.FromReader(r, r.Size(), pipeline.IgnoreSize(
		pipeline.MultiProcess(
			pipeline.ParseLine(
				pipeline.ToWriter(&w1, pipeline.Copy),
				func(line string) (string, error) {
					return line + "lel", nil
				}),
			pipeline.ParseLine(
				pipeline.ToWriter(&w2, pipeline.Copy),
				func(line string) (string, error) {
					return line + "lul", nil
				}),
		)))

	assert.Nil(t, err)
	assert.Equal(t, "hello,worldlel", w1.String())
	assert.Equal(t, "hello,worldlul", w2.String())
}

func TestCopyPipeCopy(t *testing.T) {
	const text = "hello,world"
	input := strings.NewReader(text)
	var output strings.Builder

	var wg sync.WaitGroup
	wg.Add(2)

	nextReader, nextWriter := io.Pipe()
	go func() {
		defer wg.Done()
		defer nextWriter.Close()
		io.Copy(nextWriter, input)
	}()

	go func() {
		defer wg.Done()
		io.Copy(&output, nextReader)
	}()

	wg.Wait()
	assert.Equal(t, text, output.String())
}
