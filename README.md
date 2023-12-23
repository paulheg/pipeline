# Pipeline

Pipeline package for processing data streams with the `io` package.

```go
err := pipeline.Build().
    FromReader(r, r.Size()).
    ParseLines(func(line string) (string, error) {
        return line + "a", nil
    }).
    ToWriter(&b).
    Preamble(tC.preamble).
    Appendix(tC.appendix).
    Build().Execute()
```