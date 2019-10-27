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

/**
 * Buffer an {@link OutputStream} in a separate process, to separate data generation from data compression or
 * writing to disk.
 */
public class PipelinedOutputStream extends OutputStream {
    private ExecutorService executor;
    private BufferThreadCallable bufferThreadCallable;
    private Future<Void> bufferThreadFuture;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private static final AtomicInteger streamIndex = new AtomicInteger();
    private final BufferedOutputStream bufferedOutputStream;
    private final AtomicLong bytesWrittenTracker;

    private static final BufferChunk POISON_PILL = new BufferChunk();

    private static final int BUFFER_CHUNK_SIZE = 8192;

    private static class BufferChunk {
        byte[] buffer = new byte[BUFFER_CHUNK_SIZE];
        int len;

        void writeTo(OutputStream out) throws IOException {
            out.write(buffer, 0, len);
        }
    }

    private static class BufferThreadCallable extends OutputStream implements Callable<Void> {
        private OutputStream out;
        private AtomicReference<Thread> bufferThread = new AtomicReference<>();
        private final AtomicBoolean callableClosed = new AtomicBoolean(false);
        private final ArrayBlockingQueue<BufferChunk> bufferChunks;
        private final ArrayDeque<BufferChunk> bufferChunkRecycler;

        public BufferThreadCallable(int bufSize, OutputStream wrappedStream) {
            if (bufSize <= 0) {
                throw new IllegalArgumentException("bufSize must be greater than 0");
            }
            out = wrappedStream;
            int numBufferChunks = (bufSize + BUFFER_CHUNK_SIZE - 1) / BUFFER_CHUNK_SIZE;
            bufferChunks = new ArrayBlockingQueue<>(numBufferChunks);
            bufferChunkRecycler = new ArrayDeque<>(numBufferChunks);
        }

        /** Executed in reader thread */
        @Override
        public Void call() throws Exception {
            bufferThread.set(Thread.currentThread());
            try {
                while (true) {
                    try {
                        BufferChunk chunk = bufferChunks.take();
                        if (chunk == POISON_PILL) {
                            break;
                        }
                        chunk.writeTo(out);
                    } catch (Exception e) {
                        callableClosed.set(true);
                        throw e;
                    }
                }
            } finally {
                // Close the underlying OutputStream once the buffer is flushed, or if exception is thrown
                out.close();
            }
            return null;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (len > BUFFER_CHUNK_SIZE) {
                // If b is too big for buf, break b into chunks of size buf.length
                int remaining = len;
                for (int i = 0; i < len; i += BUFFER_CHUNK_SIZE) {
                    int bytesToWrite = Math.min(BUFFER_CHUNK_SIZE, remaining);
                    write(b, off + i, bytesToWrite);
                    remaining -= bytesToWrite;
                }
            } else {
                if (callableClosed.get()) {
                    throw new IOException("Already closed");
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
            if (callableClosed.get()) {
                throw new IOException("Already closed");
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
            callableClosed.set(true);
            try {
                bufferChunks.put(POISON_PILL);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
    }

    public PipelinedOutputStream(int bufSize, OutputStream out, AtomicLong bytesWrittenTracker) throws IOException {
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
        bufferThreadCallable = new BufferThreadCallable(bufSize, out);
        // Wrap in BufferedOutputStream so that single-byte writes are collected into 8192-byte chunks
        bufferedOutputStream = new BufferedOutputStream(bufferThreadCallable);
        bufferThreadFuture = executor.submit(bufferThreadCallable);
    }

    public PipelinedOutputStream(int bufSize, OutputStream out) throws IOException {
        this(bufSize, out, new AtomicLong());
    }

    @Override
    public void write(byte[] b) throws IOException {
        bufferedOutputStream.write(b);
        bytesWrittenTracker.addAndGet(b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        bufferedOutputStream.write(b, off, len);
        bytesWrittenTracker.addAndGet(len);
    }

    @Override
    public synchronized void write(int b) throws IOException {
        bufferedOutputStream.write(b);
        bytesWrittenTracker.incrementAndGet();
    }

    public long getBytesWritten() {
        return bytesWrittenTracker.get();
    }

    @Override
    public void close() throws IOException {
        if (!closed.getAndSet(true)) {
            bufferedOutputStream.close();
            try {
                // Block on buffer thread completion
                bufferThreadFuture.get();
            } catch (InterruptedException e) {
                // Ignore
            } catch (ExecutionException e) {
                throw new IOException(e.getCause());
            }
            if (executor != null) {
                // Shut down executor service
                ExecutorService ex = executor;
                executor = null;
                try {
                    // Prevent new tasks being submitted
                    ex.shutdown();
                } catch (final SecurityException e) {
                    // Ignore for now (caught again if shutdownNow() fails)
                }
                boolean terminated = false;
                try {
                    // Await termination
                    terminated = ex.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (final InterruptedException e) {
                    //
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
