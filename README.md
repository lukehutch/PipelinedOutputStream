# `PipelinedOutputStream.java`

Allows you to set up a producer/consumer relationship between the current thread (the producer / writer) and a new thread that writes data to an `OutputStream` (the consumer / writer). This is useful if you want to overlap (pipeline) two or more of the following:

* Data production / computation
* Data compression
* Writing data to disk

For example, the following allows you to create one new thread for gzipping data produced by `produceAndWriteBuffer` (with a 64MB buffer inserted between data production and the `GZIPOutputStream`), and another new thread for writing the gzipped data to disk (with a 32MB buffer inserted between the `GZIPOutputStream` and the `FileOutputStream`). This creates a chain of three threads, with producer-consumer relationships between the first and second thread, and between the second and third thread:

```java
try (OutputStream os =
         new PipelinedOutputStream(64 * 1024 * 1024,
             new GZIPOutputStream(
                 new PipelinedOutputStream(32 * 1024 * 1024,
                     new FileOutputStream(outputFilename)))) {
    produceAndWriteData(os);
}
```

N.B. `GZIPOutputStream` is quite slow, so maybe consider using [`LZ4FrameOutputStream`](https://github.com/lz4/lz4-java) instead, if it's an option, otherwise the `GZIPOutputStream` stage of the pipeline can still end up being a major bottleneck.

## License

**The MIT License (MIT)**

Copyright (c) 2019 Luke Hutchison

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without
limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO
EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
OR OTHER DEALINGS IN THE SOFTWARE.

