package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
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

    protected static final int CLIENT_INSTANCE_COUNT;
    private static final int SERVER_INSTANCE_COUNT;

    private static final int RUNNING_TIME_SECONDS;
    private static final int CHANGE_CLUSTER_INTERVAL_SECONDS;

    private static final AtomicLong ID_GENERATOR;

    static {
        CLIENT_INSTANCE_COUNT = Integer.parseInt(System.getProperty("client_instances", "5"));
        SERVER_INSTANCE_COUNT = Integer.parseInt(System.getProperty("server_instances", "6"));

        CHANGE_CLUSTER_INTERVAL_SECONDS = Integer.parseInt(System.getProperty("change_cluster_interval", "10"));
        RUNNING_TIME_SECONDS = Integer.parseInt(System.getProperty("running_time", "180"));

        ID_GENERATOR = new AtomicLong(1);

        System.out.println("==================================================================");
        System.out.println("  Configuration:");
        System.out.println("  Client instances: " + CLIENT_INSTANCE_COUNT + " nodes");
        System.out.println("  Server instances: " + SERVER_INSTANCE_COUNT + " nodes");
        System.out.println("  Changing cluster interval: " + CHANGE_CLUSTER_INTERVAL_SECONDS + " seconds");
        System.out.println("  Running time: " + RUNNING_TIME_SECONDS + " seconds");
        System.out.println("==================================================================");
        System.out.println();
    }

    protected HazelcastInstance client;

    private final List<HazelcastInstance> serverInstances = new CopyOnWriteArrayList<HazelcastInstance>();

    private volatile boolean clusterChangeEnabled = true;
    private volatile boolean stopTest = false;

    private CountDownLatch startLatch;

    @Before
    public void setup() {
        startLatch = new CountDownLatch(1);

        for (int i = 0; i < SERVER_INSTANCE_COUNT; i++) {
            HazelcastInstance server = newHazelcastInstance(createServerConfig());
            serverInstances.add(server);
        }

        client = HazelcastClient.newHazelcastClient(createClientConfig());
    }

    @After
    public void tearDown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    protected void setClusterChangeEnabled(boolean memberShutdownEnabled) {
        this.clusterChangeEnabled = memberShutdownEnabled;
    }

    protected final boolean startAndWaitForTestCompletion() {
        if (clusterChangeEnabled) {
            ChangeClusterThread changeClusterThread = new ChangeClusterThread();
            changeClusterThread.start();
        }

        System.out.println();
        System.out.println("==================================================================");
        System.out.println("  Test started " + (clusterChangeEnabled ? "with" : "without") + " changing cluster...");
        System.out.println("==================================================================");
        System.out.println();

        startLatch.countDown();

        for (int i = 1; i <= RUNNING_TIME_SECONDS; i++) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            float percent = (i * 100.0f) / RUNNING_TIME_SECONDS;
            System.out.printf("  %5.1f%% done after running for %3d of %3d seconds\n", percent, i, RUNNING_TIME_SECONDS);

            if (stopTest) {
                System.out.println();
                System.err.println("==================================================================");
                System.err.println("  Stopped!");
                System.err.println("==================================================================");
                return false;
            }
        }

        System.out.println();
        System.out.println("==================================================================");
        System.out.println("  Done!");
        System.out.println("==================================================================");
        System.out.println();

        stopTest();
        return true;
    }

    protected final void joinAll(TestThread... threads) {
        for (TestThread thread : threads) {
            try {
                thread.join(60000);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while joining thread: " + thread.getName());
            }

            if (thread.isAlive()) {
                System.err.println("Could not join thread: " + thread.getName() + ", it's still alive");
                for (StackTraceElement stackTraceElement : thread.getStackTrace()) {
                    System.err.println("\tat " + stackTraceElement);
                }
                throw new RuntimeException("Could not join thread: " + thread.getName() + ", it's still alive");
            }
        }

        assertNoErrors(threads);
    }

    protected final boolean isStopped() {
        return stopTest;
    }

    private void stopTest() {
        stopTest = true;
    }

    private Config createServerConfig() {
        return new Config();
    }

    private ClientConfig createClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        return clientConfig;
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
            setName(getClass().getName() + ID_GENERATOR.getAndIncrement());
        }

        @Override
        public final void run() {
            try {
                startLatch.await();
                doRun();
            } catch (Throwable t) {
                stopTest();

                t.printStackTrace();
                this.error = t;
            }
        }

        protected abstract void doRun() throws Exception;

        private void assertNoError() {
            assertNull(getName() + " encountered an error", error);
        }
    }

    private class ChangeClusterThread extends TestThread {
        @Override
        public void doRun() throws Exception {
            while (!stopTest) {
                try {
                    TimeUnit.SECONDS.sleep(CHANGE_CLUSTER_INTERVAL_SECONDS);
                } catch (InterruptedException ignored) {
                }

                int index = random.nextInt(SERVER_INSTANCE_COUNT);
                serverInstances.remove(index).shutdown();

                HazelcastInstance server = newHazelcastInstance(createServerConfig());
                serverInstances.add(server);
            }
        }
    }
}
