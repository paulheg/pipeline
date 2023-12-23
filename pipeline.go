package pipeline

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/klauspost/compress/gzip"
)

type LineParser[T any] func(line string) (T, error)

type ProgressBarRegistrator func(size int64) io.Writer

type Reader func(io.Reader) error
type ReaderWithSize func(reader io.Reader, size int64) error

type Processor func(next Reader) Reader

type Writer func(io.Writer) error

type Connector func(io.Writer, io.Reader) error

func FromFile(path string, next ReaderWithSize) error {
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()

	stats, err := file.Stat()
	if err != nil {
		return err
	}

	return next(file, stats.Size())
}

func FromReader(reader io.Reader, size int64, next ReaderWithSize) error {
	return next(reader, size)
}

func FromWeb(url string, next ReaderWithSize) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return next(resp.Body, resp.ContentLength)
}

func IgnoreSize(next Reader) ReaderWithSize {
	return func(reader io.Reader, size int64) error {
		return next(reader)
	}
}

func ProgressBar(register ProgressBarRegistrator, next Reader) ReaderWithSize {
	return func(r io.Reader, size int64) error {
		reader := io.TeeReader(r, register(size))
		return next(reader)
	}
}

func DecompressGzip(next Reader) Reader {
	return func(r io.Reader) error {
		gz, err := gzip.NewReader(r)
		if err != nil {
			return err
		}
		defer gz.Close()

		return next(gz)
	}
}

func Preamble(next Reader, preamble string) Reader {
	return func(r io.Reader) error {
		pr := strings.NewReader(preamble)
		mr := io.MultiReader(pr, r)

		return next(mr)
	}
}

func Appendix(next Reader, appendix string) Reader {
	return func(r io.Reader) error {
		ar := strings.NewReader(appendix)
		mr := io.MultiReader(r, ar)

		return next(mr)
	}
}

func ParseLineToGob(next Reader, p LineParser[interface{}]) Reader {
	return func(r io.Reader) error {
		scanner := bufio.NewScanner(r)
		reader, writer := io.Pipe()

		enc := gob.NewEncoder(writer)

		go func() {
			defer writer.Close()
			for scanner.Scan() {
				line := scanner.Text()

				parsed, err := p(line)
				if err != nil {
					// write error
				}

				err = enc.Encode(parsed)
				if err != nil {
					log.Printf("error encoding: %v", err)
					writer.CloseWithError(err)
				}
			}
		}()

		return next(reader)
	}
}

func ParseLineToJson(next Reader, p LineParser[interface{}]) Reader {
	return func(r io.Reader) error {
		scanner := bufio.NewScanner(r)
		reader, writer := io.Pipe()

		enc := json.NewEncoder(writer)

		go func() {
			defer writer.Close()
			for scanner.Scan() {
				line := scanner.Text()

				parsed, err := p(line)
				if err != nil {
					// write error
				}

				err = enc.Encode(parsed)
				if err != nil {
					log.Printf("error encoding: %v", err)
					writer.CloseWithError(err)
				}
			}
		}()

		return next(reader)
	}
}

func DecodeGob[I any](consumer func(*I) []byte) Processor {
	return func(next Reader) Reader {
		return func(r io.Reader) error {
			decoder := gob.NewDecoder(r)
			reader, writer := io.Pipe()

			go func() {
				defer writer.Close()
				var err error
				var input I
				for {
					err = decoder.Decode(&input)
					if err == io.EOF {
						break
					} else if err != nil {
						log.Printf("error while decoding: %v", err)
						break
					}
					out := consumer(&input)
					writer.Write(out)
				}
			}()

			return next(reader)
		}
	}
}

func DecodeJson[I any](consumer func(*I) []byte) Processor {
	return func(next Reader) Reader {
		return func(r io.Reader) error {
			decoder := json.NewDecoder(r)
			reader, writer := io.Pipe()

			go func() {
				defer writer.Close()
				var input I
				var err error
				for {
					err = decoder.Decode(&input)
					if err == io.EOF {
						break
					} else if err != nil {
						log.Printf("error while decoding: %v", err)
						break
					}
					out := consumer(&input)
					writer.Write(out)
				}
			}()

			return next(reader)
		}
	}
}

func ParseLine(next Reader, p LineParser[string]) Reader {
	return func(r io.Reader) error {
		scanner := bufio.NewScanner(r)
		reader, writer := io.Pipe()

		go func() {
			defer writer.Close()
			for scanner.Scan() {
				line := scanner.Text()

				parsed, err := p(line)
				if err != nil {
					// write error
				}

				writer.Write([]byte(parsed))
			}
		}()

		return next(reader)
	}
}

func ToWriter(w io.Writer, before Connector) Reader {
	return func(r io.Reader) error {
		return before(w, r)
	}
}

func ToFile(file *os.File, before Connector) Reader {
	return func(r io.Reader) error {
		return before(file, r)
	}
}

func ToNewFile(path string, before Connector) Reader {
	return func(r io.Reader) error {
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		defer file.Close()

		return before(file, r)
	}
}

func Copy(w io.Writer, r io.Reader) error {
	_, err := io.Copy(w, r)
	return err
}

func CompressGzip(next Connector) Connector {
	return func(w io.Writer, r io.Reader) error {
		gzip := gzip.NewWriter(w)
		defer gzip.Close()

		return next(gzip, r)
	}
}

func MultiProcess(next ...Reader) Reader {
	return func(r io.Reader) error {
		var err error

		writers := make([]io.Writer, len(next))
		closers := make([]io.Closer, len(next))
		result := make(chan error, len(next))

		for i := 0; i < len(next); i++ {
			reader, writer := io.Pipe()
			writers[i] = writer
			closers[i] = writer

			go func(read Reader, reader io.Reader, closer io.Closer) {
				// defer io.Copy(io.Discard, reader)
				err := read(reader)
				result <- err
			}(next[i], reader, writer)
		}

		go func() {
			w := io.MultiWriter(writers...)
			_, err = io.Copy(w, r)
			if err != nil {
				log.Printf("multiwriter copy error %v", err)
			}

			for i := 0; i < len(next); i++ {
				err = closers[i].Close()
				if err != nil {
					log.Printf("mw close error %v", err)
				}
			}
		}()

		for i := 0; i < len(next); i++ {
			e := <-result
			if err == nil {
				err = e
			}
		}

		return err
	}
}
