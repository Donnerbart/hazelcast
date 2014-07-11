package com.hazelcast.client.semaphore;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientSemaphoreThreadedTest {

    private static HazelcastInstance client;

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void concurrent_trySemaphoreTest() {
        concurrent_trySemaphoreTest(false);
    }

    @Test
    public void concurrent_trySemaphoreWithTimeOutTest() {
        concurrent_trySemaphoreTest(true);
    }

    private void concurrent_trySemaphoreTest(boolean tryWithTimeOut) {
        ISemaphore semaphore = client.getSemaphore(randomString());
        semaphore.init(1);

        AtomicInteger upTotal = new AtomicInteger(0);
        AtomicInteger downTotal = new AtomicInteger(0);

        SemaphoreTestThread threads[] = new SemaphoreTestThread[8];
        for (int i = 0; i < threads.length; i++) {
            SemaphoreTestThread thread;
            if (tryWithTimeOut) {
                thread = new TrySemaphoreTimeOutThread(semaphore, upTotal, downTotal);
            } else {
                thread = new TrySemaphoreThread(semaphore, upTotal, downTotal);
            }
            thread.start();
            threads[i] = thread;
        }
        HazelcastTestSupport.assertJoinable(threads);

        for (SemaphoreTestThread t : threads) {
            assertNull("thread " + t + " has error " + t.error, t.error);
        }

        assertEquals("concurrent access to locked code caused wrong total", 0, upTotal.get() + downTotal.get());
    }

    private static class TrySemaphoreThread extends SemaphoreTestThread {
        private TrySemaphoreThread(ISemaphore semaphore, AtomicInteger upTotal, AtomicInteger downTotal) {
            super(semaphore, upTotal, downTotal);
        }

        protected void iterativelyRun() throws Exception {
            if (semaphore.tryAcquire()) {
                work();
                semaphore.release();
            }
        }
    }

    private static class TrySemaphoreTimeOutThread extends SemaphoreTestThread {
        private TrySemaphoreTimeOutThread(ISemaphore semaphore, AtomicInteger upTotal, AtomicInteger downTotal) {
            super(semaphore, upTotal, downTotal);
        }

        protected void iterativelyRun() throws Exception {
            if (semaphore.tryAcquire(1, TimeUnit.MILLISECONDS)) {
                work();
                semaphore.release();
            }
        }
    }

    private static abstract class SemaphoreTestThread extends Thread {
        static private final int MAX_ITERATIONS = 1000 * 10;

        protected final ISemaphore semaphore;

        private final Random random = new Random();
        private final AtomicInteger upTotal;
        private final AtomicInteger downTotal;

        private volatile Throwable error;

        protected SemaphoreTestThread(ISemaphore semaphore, AtomicInteger upTotal, AtomicInteger downTotal) {
            this.semaphore = semaphore;
            this.upTotal = upTotal;
            this.downTotal = downTotal;
        }

        @Override
        public final void run() {
            try {
                for (int i = 0; i < MAX_ITERATIONS; i++) {
                    iterativelyRun();
                }
            } catch (Throwable e) {
                error = e;
            }
        }

        protected abstract void iterativelyRun() throws Exception;

        protected void work() {
            final int delta = random.nextInt(1000);
            upTotal.addAndGet(delta);
            downTotal.addAndGet(-delta);
        }
    }
}
