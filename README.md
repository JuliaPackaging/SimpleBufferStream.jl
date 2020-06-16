# BufferStream implementation

This is what I wish `Base.BufferStream` was.

# Usage

See the tests for examples, but in a nut shell:

```julia
# Stream an HTTP response into an asynchronous processor
buff = BufferStream()
t_processor = @async process(buff)
HTTP.get(url, response_stream=buff)
wait(t_processor)
```

You can `readavailable()`, `write()`, `close()`, `eof()`, etc...  The implementation is really simple, so I suggest you just look at it.