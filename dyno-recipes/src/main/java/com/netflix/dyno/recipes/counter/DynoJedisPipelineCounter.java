/*******************************************************************************
 * Copyright 2011 Netflix
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.dyno.recipes.counter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.LoggerFactory;

import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.jedis.DynoJedisPipeline;
import com.netflix.dyno.recipes.util.Tuple;

/**
 * Pipeline implementation of {@link DynoCounter}. This implementation has slightly different semantics than
 * {@link DynoJedisPipeline} in that both {@link #incr()} and {@link #sync()} are asynchronous.
 * <p>
 * Note that this implementation is thread-safe whereas {@link DynoJedisPipeline} is not.
 * </p>
 *
 * @see <a href="http://redis.io/topics/pipelining">Redis Pipelining</a>
 *
 * @author jcacciatore
 */
@ThreadSafe
public class DynoJedisPipelineCounter extends DynoJedisCounter {

    private enum Command {
        INCR,
        SYNC,
        STOP
    }

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DynoJedisPipelineCounter.class);

    private final LinkedBlockingQueue<Command> queue = new LinkedBlockingQueue<Command>();

    private final ExecutorService counterThreadPool = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "DynoJedisPipelineCounter-Poller");
        }
    });

    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private final CountDownLatch latch = new CountDownLatch(1);

    private final Consumer consumer;

    public DynoJedisPipelineCounter(String key, DynoJedisClient client) {
        super(key, client);

        this.consumer = new Consumer(queue, generatedKeys);
    }

    @Override
    public void initialize() {
        if (initialized.compareAndSet(false, true)) {
            super.initialize();
            counterThreadPool.submit(consumer);
        }
    }

    @Override
    public void incr() {
        if (!initialized.get()) {
            throw new IllegalStateException("Counter has not been initialized");
        }

        queue.offer(Command.INCR);
    }

    public void sync() {
        if (!initialized.get()) {
            throw new IllegalStateException("Counter has not been initialized");
        }

        logger.debug("sending SYNC offer");
        queue.offer(Command.SYNC);
    }

    @Override
    public void close() {
        if (!initialized.get()) {
            throw new IllegalStateException("Counter has not been initialized");
        }

        queue.offer(Command.STOP);

        try {
            latch.await(2000L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // ignore
        }

    }

    class Consumer implements Runnable {
        /**
         * Used as a synchronizer between producer threads and this Consumer.
         */
        private final LinkedBlockingQueue<Command> queue;

        /**
         * Keys that were generated by an instance of a {@link DynoCounter}.
         */
        private final List<String> keys;

        /**
         * Used for debugging
         */
        private Long syncCount = 0L;

        /**
         * Used to ensure there are operations to sync in the pipeline. This is not
         * an optimization; the pipeline can block if multiple SYNCs are processed
         */
        private int pipelineOps = 0;

        /**
         * Contains a mapping of sharded-key to pipeline.
         */
        private List<Tuple<String, DynoJedisPipeline>> keysAndPipelines;


        public Consumer(final LinkedBlockingQueue<Command> queue, final List<String> keys) {
            this.queue = queue;
            this.keys = keys;
            keysAndPipelines = new ArrayList<Tuple<String, DynoJedisPipeline>>(keys.size());
            for (String key: keys) {
                keysAndPipelines.add(new Tuple<String, DynoJedisPipeline>(key, client.pipelined()));
            }
        }

        @Override
        public void run() {
            Command cmd = null;
            do {
                try {
                    cmd = queue.take();

                    switch (cmd) {
                        case INCR: {
                            Tuple<String, DynoJedisPipeline> tuple = keysAndPipelines.get(randomIntFrom0toN());
                            tuple._2().incr(tuple._1());
                            pipelineOps++;
                            break;
                        }
                        case SYNC: {
                            syncCount++;
                            logger.debug(Thread.currentThread().getName() + " - SYNC " + syncCount + " received");

                            if (pipelineOps > 0) {
                                for (Tuple<String, DynoJedisPipeline> tuple : keysAndPipelines) {
                                    tuple._2().sync();
                                }

                                keysAndPipelines = new ArrayList<Tuple<String, DynoJedisPipeline>>(keys.size());
                                for (String key : keys) {
                                    keysAndPipelines.add(new Tuple<String, DynoJedisPipeline>(key, client.pipelined()));
                                }
                                pipelineOps = 0;
                            }
                            logger.debug(Thread.currentThread().getName() + " - SYNC " + syncCount + " done");
                            break;
                        }
                        case STOP: {
                            counterThreadPool.shutdownNow();
                            latch.countDown();
                            break;
                        }
                    }

                } catch (InterruptedException e) {
                    // ignore
                }
            } while (cmd != Command.STOP);
        }
    }

}