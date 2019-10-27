# `PipelinedOutputStream.java`

Allows you to set up a producer/consumer relationship between the current thread (the producer / writer) and a new thread that writes data to an `OutputStream` (the consumer / writer). This is useful if you want to overlap (pipeline) two or more of the following:

* Data production
* Data compression
* Writing data to disk

For example, the following allows you to create one new thread for gzipping data produced by `produceAndWriteBuffer` (with a 64MB buffer inserted between data production and the `GZipOutputStream`), and another new thread for writing the gzipped data to disk (with a 32MB buffer inserted between the `GZIPOutputStream` and the `FileOutputStream`). This creates a chain of three threads, with producer-consumer relationships between the first and second thread, and between the second and third thread:

```java
try (OutputStream os =
         new PipelinedOutputStream(64 * 1024 * 1024,
             new GZIPOutputStream(
                 new PipelinedOutputStream(32 * 1024 * 1024,
                     new FileOutputStream(outputFilename)))) {
    produceAndWriteData(os);
}
```
