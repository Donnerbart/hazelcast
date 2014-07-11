package com.hazelcast.client.stress;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.core.Hazelcast.newHazelcastInstance;
import static org.junit.Assert.assertNull;

public abstract class StressTestSupport extends HazelcastTestSupport {

    // TODO Should be system property
    private static final int RUNNING_TIME_SECONDS = 180;
    // TODO Should be system property
    private static final int CLUSTER_SIZE = 6;
    // TODO Should be system property
    private static final int KILL_DELAY_SECONDS = 10;

    private static final AtomicLong ID_GENERATOR = new AtomicLong(1);

    private final List<HazelcastInstance> serverInstances = new CopyOnWriteArrayList<HazelcastInstance>();

    private CountDownLatch startLatch;

    private volatile boolean clusterChangeEnabled = true;
    private volatile boolean stopOnError = true;
    private volatile boolean stopTest = false;

    @Before
    public void setup() {
        startLatch = new CountDownLatch(1);

        for (int k = 0; k < CLUSTER_SIZE; k++) {
            HazelcastInstance server = newHazelcastInstance(createClusterConfig());
            serverInstances.add(server);
        }
    }

    @After
    public void tearDown() {
        for (HazelcastInstance server : serverInstances) {
            try {
                server.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected final boolean startAndWaitForTestCompletion() {
        System.out.println("Cluster change enabled:" + clusterChangeEnabled);
        if (clusterChangeEnabled) {
            KillMemberThread killMemberThread = new KillMemberThread();
            killMemberThread.start();
        }

        System.out.println("==================================================================");
        System.out.println("Test started.");
        System.out.println("==================================================================");

        startLatch.countDown();

        for (int k = 1; k <= RUNNING_TIME_SECONDS; k++) {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            float percent = (k * 100.0f) / RUNNING_TIME_SECONDS;
            System.out.printf("%.1f Running for %s of %s seconds\n", percent, k, RUNNING_TIME_SECONDS);

            if (stopTest) {
                System.err.println("==================================================================");
                System.err.println("Test ended premature!");
                System.err.println("==================================================================");
                return false;
            }
        }

        System.out.println("==================================================================");
        System.out.println("Test completed.");
        System.out.println("==================================================================");

        stopTest();
        return true;
    }

    protected void setClusterChangeEnabled(boolean memberShutdownEnabled) {
        this.clusterChangeEnabled = memberShutdownEnabled;
    }

    @SuppressWarnings("unused")
    protected final void setStopOnError(boolean stopOnError) {
        this.stopOnError = stopOnError;
    }

    protected final boolean isStopped() {
        return stopTest;
    }

    protected final void joinAll(TestThread... threads) {
        for (TestThread t : threads) {
            try {
                t.join(60000);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while joining thread:" + t);
            }

            if (t.isAlive()) {
                System.err.println("Could not join Thread:" + t.getName() + ", it is still alive");
                for (StackTraceElement e : t.getStackTrace()) {
                    System.err.println("\tat " + e);
                }
                throw new RuntimeException("Could not join thread:" + t + ", thread is still alive");
            }
        }

        assertNoErrors(threads);
    }

    private void stopTest() {
        stopTest = true;
    }

    private Config createClusterConfig() {
        return new Config();
    }

    private void assertNoErrors(TestThread... threads) {
        for (TestThread thread : threads) {
            thread.assertNoError();
        }
    }

    protected abstract class TestThread extends Thread {
        protected final Random random = new Random();

        private volatile Throwable error;

        protected TestThread() {
            setName(getClass().getName() + "" + ID_GENERATOR.getAndIncrement());
        }

        @Override
        public final void run() {
            try {
                startLatch.await();
                doRun();
            } catch (Throwable t) {
                if (stopOnError) {
                    stopTest();
                }
                t.printStackTrace();
                this.error = t;
            }
        }

        protected abstract void doRun() throws Exception;

        private void assertNoError() {
            assertNull(getName() + " encountered an error", error);
        }
    }

    private class KillMemberThread extends TestThread {
        @Override
        public void doRun() throws Exception {
            while (!stopTest) {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(KILL_DELAY_SECONDS));
                } catch (InterruptedException ignored) {
                }

                int index = random.nextInt(CLUSTER_SIZE);
                HazelcastInstance instance = serverInstances.remove(index);
                instance.shutdown();

                HazelcastInstance newInstance = newHazelcastInstance(createClusterConfig());
                serverInstances.add(newInstance);
            }
        }
    }
}
