/*
 * PipelinedOutputStream.java
 *
 * Author: Luke Hutchison
 *
 * Hosted at: https://github.com/lukehutch/PipelinedOutputStream
 *
 * --
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Luke Hutchison
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without
 * limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO
 * EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
 * AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
 * OR OTHER DEALINGS IN THE SOFTWARE.
 */
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

// TODO: Auto-generated Javadoc
/**
 * Buffer an {@link OutputStream} in a separate process, to separate data generation from data compression or
 * writing to disk.
 */
public class PipelinedOutputStream extends OutputStream {
    /** An executor service for the consumer thread. */
    private ExecutorService executor;

    /** The consumer stream. */
    private ConsumerStream consumerStream;

    /** The {@link Future} used to await termination of the consumer thread. */
    private Future<Void> consumerStreamFuture;

    /** The buffered output stream inserted between the producer and consumer. */
    private final BufferedOutputStream bufferedOutputStream;

    /** Tracks the number of bytes written. */
    private final AtomicLong bytesWrittenTracker;

    /** Used to generate unique thread names. */
    private static final AtomicInteger streamIndex = new AtomicInteger();

    /** True when the stream has been closed. */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * The Class BufferChunk.
     */
    private static class BufferChunk {
        /** The buffer. */
        byte[] buffer = new byte[CHUNK_SIZE];

        /** The number of bytes used in the buffer. */
        int len;

        /** The poison pill to use to mark end of stream. */
        static final BufferChunk POISON_PILL = new BufferChunk();

        /**
         * The buffer chunk size to use. Should probably be 8192, since that is the default used by Java buffers.
         */
        private static final int CHUNK_SIZE = 8192;

        /**
         * Write the buffer chunk to an {@link OutputStream}.
         *
         * @param out the out
         * @throws IOException Signals that an I/O exception has occurred.
         */
        void writeTo(OutputStream out) throws IOException {
            out.write(buffer, 0, len);
        }
    }

    /**
     * The {@link OutputStream} for the consumer thread.
     */
    private static class ConsumerStream extends OutputStream implements Callable<Void> {
        /** The output stream to write consumed bytes to. */
        private OutputStream out;

        /** The consumer stream {@link Thread} reference. */
        private AtomicReference<Thread> consumerStreamThread = new AtomicReference<>();

        /** True when the consumer stream has been closed. */
        private final AtomicBoolean consumerStreamClosed = new AtomicBoolean(false);

        /** The buffer chunks that have not yet been consumed. */
        private final ArrayBlockingQueue<BufferChunk> bufferChunks;

        /** Recycler for buffer chunks. */
        private final ArrayDeque<BufferChunk> bufferChunkRecycler;

        /**
         * Instantiate a new consumer stream.
         *
         * @param bufSize      the size of the buffer to insert between producer and consumer.
         * @param outputStream the stream to send consumed output to.
         */
        public ConsumerStream(int bufSize, OutputStream outputStream) {
            if (bufSize <= 0) {
                throw new IllegalArgumentException("bufSize must be greater than 0");
            }
            out = outputStream;
            int numBufferChunks = (bufSize + BufferChunk.CHUNK_SIZE - 1) / BufferChunk.CHUNK_SIZE;
            bufferChunks = new ArrayBlockingQueue<>(numBufferChunks);
            bufferChunkRecycler = new ArrayDeque<>(numBufferChunks);
        }

        /**
         * Executed in consumer thread.
         *
         * @return null
         * @throws Exception if anything goes wrong while writing.
         */
        @Override
        public Void call() throws Exception {
            consumerStreamThread.set(Thread.currentThread());
            try {
                while (true) {
                    try {
                        BufferChunk chunk = bufferChunks.take();
                        if (chunk == BufferChunk.POISON_PILL) {
                            break;
                        }
                        chunk.writeTo(out);
                        bufferChunkRecycler.add(chunk);
                    } catch (Exception e) {
                        throw e;
                    }
                }
            } finally {
                // Close the underlying OutputStream once the buffer is flushed, or if exception is thrown
                consumerStreamClosed.set(true);
                out.close();
            }
            return null;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (len > BufferChunk.CHUNK_SIZE) {
                // If b is too big for buf, break b into chunks of size buf.length
                int remaining = len;
                for (int i = 0; i < len; i += BufferChunk.CHUNK_SIZE) {
                    int bytesToWrite = Math.min(BufferChunk.CHUNK_SIZE, remaining);
                    write(b, off + i, bytesToWrite);
                    remaining -= bytesToWrite;
                }
            } else {
                if (consumerStreamClosed.get()) {
                    throw new IOException("Tried to write to closed stream");
                }
                BufferChunk bufferChunk = bufferChunkRecycler.poll();
                if (bufferChunk == null) {
                    bufferChunk = new BufferChunk();
                }
                for (int o = 0, i = off; o < len;) {
                    bufferChunk.buffer[o++] = b[i++];
                }
                bufferChunk.len = len;
                try {
                    bufferChunks.put(bufferChunk);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        }

        @Override
        public synchronized void write(int b) throws IOException {
            if (consumerStreamClosed.get()) {
                throw new IOException("Tried to write to closed stream");
            }
            BufferChunk bufferChunk = bufferChunkRecycler.poll();
            if (bufferChunk == null) {
                bufferChunk = new BufferChunk();
            }
            bufferChunk.buffer[0] = (byte) b;
            bufferChunk.len = 1;
            try {
                bufferChunks.put(bufferChunk);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            if (!consumerStreamClosed.getAndSet(true)) {
                try {
                    bufferChunks.put(BufferChunk.POISON_PILL);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        }
    }

    /**
     * Instantiate a new pipelined output stream.
     *
     * @param bufSize             the size of the buffer to insert between producer and consumer (mostly to collect
     *                            together single-byte {@link OutputStream#write(int)} operations into chunks).
     * @param outputStream        the output stream to write to
     * @param bytesWrittenTracker an {@link AtomicLong} to use to track the number of bytes written.
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public PipelinedOutputStream(int bufSize, OutputStream outputStream, AtomicLong bytesWrittenTracker)
            throws IOException {
        this.bytesWrittenTracker = bytesWrittenTracker;
        executor = Executors.newFixedThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                final Thread thread = new Thread(r,
                        PipelinedOutputStream.class.getSimpleName() + "-" + streamIndex.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        });
        consumerStream = new ConsumerStream(bufSize, outputStream);
        bufferedOutputStream = new BufferedOutputStream(consumerStream);
        consumerStreamFuture = executor.submit(consumerStream);
    }

    /**
     * Instantiate a new pipelined output stream.
     *
     * @param bufSize      the size of the buffer to insert between producer and consumer (mostly to collect
     *                     together single-byte {@link OutputStream#write(int)} operations into chunks).
     * @param outputStream the output stream to write to
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public PipelinedOutputStream(int bufSize, OutputStream out) throws IOException {
        this(bufSize, out, new AtomicLong());
    }

    /**
     * Write an array of bytes.
     *
     * @param b the array of bytes to write.
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public void write(byte[] b) throws IOException {
        bufferedOutputStream.write(b);
        bytesWrittenTracker.addAndGet(b.length);
    }

    /**
     * Write a range of bytes from an array.
     *
     * @param b   the array of bytes.
     * @param off the start offset within the array.
     * @param len the number of bytes to write from the array.
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        bufferedOutputStream.write(b, off, len);
        bytesWrittenTracker.addAndGet(len);
    }

    /**
     * Write a single byte.
     *
     * @param b the byte.
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public synchronized void write(int b) throws IOException {
        bufferedOutputStream.write(b);
        bytesWrittenTracker.incrementAndGet();
    }

    /**
     * Get the number of bytes written so far.
     *
     * @return the number of bytes written so far.
     */
    public long getBytesWritten() {
        return bytesWrittenTracker.get();
    }

    /**
     * Close the stream.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public void close() throws IOException {
        if (!closed.getAndSet(true)) {
            bufferedOutputStream.close();
            try {
                // Block on consumer thread completion
                consumerStreamFuture.get();
            } catch (InterruptedException e) {
                // Ignore
            } catch (ExecutionException e) {
                throw new IOException(e.getCause());
            }
            if (executor != null) {
                // Try to shut down executor service cleanly
                ExecutorService ex = executor;
                executor = null;
                try {
                    ex.shutdown();
                } catch (final SecurityException e) {
                    // Ignore
                }
                boolean terminated = false;
                try {
                    // Await termination
                    terminated = ex.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (final InterruptedException e) {
                    // Ignore
                }
                if (!terminated) {
                    try {
                        ex.shutdownNow();
                    } catch (final SecurityException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }
}
