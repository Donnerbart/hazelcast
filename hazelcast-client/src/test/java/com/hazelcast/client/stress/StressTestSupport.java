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

    protected static final boolean CHANGE_CLUSTER_TESTS_ACTIVE;
    private static final int CHANGE_CLUSTER_INTERVAL_SECONDS;

    private static final int THREAD_JOIN_TIMEOUT;
    private static final int RUNNING_TIME_SECONDS;

    private static final AtomicLong ID_GENERATOR;

    static {
        CLIENT_INSTANCE_COUNT = Integer.parseInt(System.getProperty("com.hazelcast.stress.client_instances", "5"));
        SERVER_INSTANCE_COUNT = Integer.parseInt(System.getProperty("com.hazelcast.stress.server_instances", "6"));

        CHANGE_CLUSTER_TESTS_ACTIVE = Boolean.parseBoolean(
                System.getProperty("com.hazelcast.stress.change_cluster.active", "false")
        );
        CHANGE_CLUSTER_INTERVAL_SECONDS = Integer.parseInt(
                System.getProperty("com.hazelcast.stress.change_cluster.interval", "10")
        );

        THREAD_JOIN_TIMEOUT = Integer.parseInt(System.getProperty("com.hazelcast.stress.thread_join_timeout", "60"));
        RUNNING_TIME_SECONDS = Integer.parseInt(System.getProperty("com.hazelcast.stress.running_time", "180"));

        ID_GENERATOR = new AtomicLong(1);

        System.out.println("==================================================================");
        System.out.println("  Configuration:");
        System.out.println("  Client instances: " + CLIENT_INSTANCE_COUNT + " nodes");
        System.out.println("  Server instances: " + SERVER_INSTANCE_COUNT + " nodes");
        System.out.println("  Changing cluster: " + CHANGE_CLUSTER_TESTS_ACTIVE);
        System.out.println("  Changing cluster interval: " + CHANGE_CLUSTER_INTERVAL_SECONDS + " seconds");
        System.out.println("  Thread join timeout: " + THREAD_JOIN_TIMEOUT + " seconds");
        System.out.println("  Running time: " + RUNNING_TIME_SECONDS + " seconds");
        System.out.println("==================================================================");
    }

    protected HazelcastInstance client;

    private final List<HazelcastInstance> serverInstances = new CopyOnWriteArrayList<HazelcastInstance>();

    private volatile boolean running = true;

    private CountDownLatch startLatch;
    private ChangeClusterThread changeClusterThread;

    @Before
    public void setup() {
        System.out.println("==================================================================");
        System.out.println("  Hazelcast initialization...");

        startLatch = new CountDownLatch(1);

        for (int i = 0; i < SERVER_INSTANCE_COUNT; i++) {
            HazelcastInstance server = newHazelcastInstance(createServerConfig());
            serverInstances.add(server);
        }

        client = HazelcastClient.newHazelcastClient(createClientConfig());

        System.out.println("  Done!");
        System.out.println("==================================================================");
    }

    @After
    public void tearDown() {
        System.out.println();
        System.out.println("==================================================================");
        System.out.println("  Hazelcast shutdown...");

        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();

        System.out.println("  Done!");
        System.out.println("==================================================================");
    }

    protected final boolean startAndWaitForTestCompletion(boolean clusterChangeEnabled) {
        if (clusterChangeEnabled) {
            changeClusterThread = new ChangeClusterThread();
            changeClusterThread.start();
        }

        System.out.println();
        System.out.println("==================================================================");
        System.out.println("  Test started with " + (clusterChangeEnabled ? "changing" : "fixed") + " cluster...");
        System.out.println("==================================================================");
        System.out.println();

        startLatch.countDown();

        boolean changeClusterThreadInterrupted = false;
        for (int i = 1; i <= RUNNING_TIME_SECONDS; i++) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            float percent = (i * 100.0f) / RUNNING_TIME_SECONDS;
            System.out.printf("  %5.1f%% done after running for %3d of %3d seconds\n", percent, i, RUNNING_TIME_SECONDS);

            // Shutdown the cluster changing thread so we have enough time for the cluster to stabilize for the error checks
            // TODO This still may create a race condition and should be replaced with some smart cluster/rebalancing listeners
            if (clusterChangeEnabled && !changeClusterThreadInterrupted && percent > 90) {
                System.out.println("  Interrupting cluster change thread...");
                changeClusterThread.interrupt();
                changeClusterThreadInterrupted = true;
            }

            if (!running) {
                System.out.println();
                System.err.println("==================================================================");
                System.err.println("  Test stopped!");
                System.err.println("==================================================================");
                return false;
            }
        }

        System.out.println();
        System.out.println("==================================================================");
        System.out.println("  Test done!");
        System.out.println("==================================================================");

        running = false;
        return true;
    }

    protected final void joinAll(TestThread... threads) {
        System.out.println();
        System.out.println("==================================================================");

        if (changeClusterThread != null) {
            System.out.println("  Joining cluster change thread...");
            joinThread(changeClusterThread);
        }

        System.out.println("  Joining " + threads.length + " worker threads...");
        for (TestThread thread : threads) {
            joinThread(thread);
        }

        System.out.println("  Checking " + threads.length + " worker threads for errors...");
        assertNoErrors(threads);

        System.out.println("  Done!");
        System.out.println("==================================================================");
    }

    private void joinThread(Thread thread) {
        try {
            TimeUnit.SECONDS.timedJoin(thread, THREAD_JOIN_TIMEOUT);
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

    private void assertNoErrors(TestThread... threads) {
        for (TestThread thread : threads) {
            thread.assertNoError();
        }
    }

    private Config createServerConfig() {
        return new Config();
    }

    private ClientConfig createClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        return clientConfig;
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
                while (running && !isInterrupted()) {
                    runAction();
                }
            } catch (Throwable t) {
                running = false;

                t.printStackTrace();
                this.error = t;
            }
        }

        protected abstract void runAction();

        private void assertNoError() {
            assertNull(getName() + " encountered an error", error);
        }
    }

    private class ChangeClusterThread extends TestThread {
        @Override
        public void runAction() {
            try {
                TimeUnit.SECONDS.sleep(CHANGE_CLUSTER_INTERVAL_SECONDS);
            } catch (InterruptedException ignored) {
                interrupt();
            }

            if (!isInterrupted()) {
                int index = random.nextInt(SERVER_INSTANCE_COUNT);
                serverInstances.remove(index).shutdown();

                HazelcastInstance server = newHazelcastInstance(createServerConfig());
                serverInstances.add(server);
            }
        }
    }
}
