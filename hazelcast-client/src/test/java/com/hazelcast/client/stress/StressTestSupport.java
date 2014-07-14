package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.test.HazelcastTestSupport;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertNull;

public abstract class StressTestSupport extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 271;
    private static final AtomicLong ID_GENERATOR = new AtomicLong(1);

    protected static final int CLIENT_INSTANCE_COUNT;
    private static final int SERVER_INSTANCE_COUNT;

    protected static final boolean FIXED_CLUSTER_TESTS_ACTIVE;
    protected static final boolean CHANGE_CLUSTER_TESTS_ACTIVE;
    private static final boolean CHANGE_CLUSTER_REMOVE_BEFORE_ADD;
    private static final int CHANGE_CLUSTER_INTERVAL_SECONDS;

    private static final int THREAD_JOIN_TIMEOUT;
    private static final int RUNNING_TIME_SECONDS;

    static {
        CLIENT_INSTANCE_COUNT = Integer.parseInt(System.getProperty("com.hazelcast.stress.client_instances", "5"));
        SERVER_INSTANCE_COUNT = Integer.parseInt(System.getProperty("com.hazelcast.stress.server_instances", "6"));

        FIXED_CLUSTER_TESTS_ACTIVE = Boolean.parseBoolean(
                System.getProperty("com.hazelcast.stress.fixed_cluster.active", "true")
        );
        CHANGE_CLUSTER_TESTS_ACTIVE = Boolean.parseBoolean(
                System.getProperty("com.hazelcast.stress.change_cluster.active", "false")
        );
        CHANGE_CLUSTER_REMOVE_BEFORE_ADD = Boolean.parseBoolean(
                System.getProperty("com.hazelcast.stress.change_cluster.remove_before_add", "true")
        );
        CHANGE_CLUSTER_INTERVAL_SECONDS = Integer.parseInt(
                System.getProperty("com.hazelcast.stress.change_cluster.interval", "10")
        );

        THREAD_JOIN_TIMEOUT = Integer.parseInt(System.getProperty("com.hazelcast.stress.thread_join_timeout", "60"));
        RUNNING_TIME_SECONDS = Integer.parseInt(System.getProperty("com.hazelcast.stress.running_time", "180"));

        String levelString = System.getProperty("com.hazelcast.stress.log_level", "INFO");
        setLogLevel(levelString);

        System.out.println("==================================================================");
        System.out.println("  Configuration:");
        System.out.println("  Log level: " + levelString);
        System.out.println("  Client instances: " + CLIENT_INSTANCE_COUNT + " nodes");
        System.out.println("  Server instances: " + SERVER_INSTANCE_COUNT + " nodes");
        System.out.println("  Run fixed cluster tests: " + FIXED_CLUSTER_TESTS_ACTIVE);
        System.out.println("  Run changing cluster tests: " + CHANGE_CLUSTER_TESTS_ACTIVE);
        if (CHANGE_CLUSTER_TESTS_ACTIVE) {
            System.out.println("  Changing cluster remove before add: " + CHANGE_CLUSTER_REMOVE_BEFORE_ADD);
            System.out.println("  Changing cluster interval: " + CHANGE_CLUSTER_INTERVAL_SECONDS + " seconds");
        }
        System.out.println("  Thread join timeout: " + THREAD_JOIN_TIMEOUT + " seconds");
        System.out.println("  Running time: " + RUNNING_TIME_SECONDS + " seconds");
        System.out.println("==================================================================");
        System.out.println();
    }

    private static void setLogLevel(String levelString) {
        Level logLevel = Level.toLevel(levelString);

        Logger root = Logger.getRootLogger();
        root.setLevel(logLevel);
    }

    protected HazelcastInstance client;

    private final List<HazelcastInstance> serverInstances = new CopyOnWriteArrayList<HazelcastInstance>();

    private volatile boolean running = true;

    private CountDownLatch startLatch;
    private Semaphore[] clusterMigrationSemaphores = new Semaphore[PARTITION_COUNT];
    private Map<Integer, String> clusterMigrationEvents = new HashMap<Integer, String>(PARTITION_COUNT);

    private AtomicInteger migrationStartedCounter = new AtomicInteger(0);
    private AtomicInteger migrationCompletedCounter = new AtomicInteger(0);
    private AtomicInteger migrationFailedCounter = new AtomicInteger(0);

    private ChangeClusterThread changeClusterThread;

    @Before
    public void setup() {
        System.out.println("==================================================================");
        System.out.println("  Hazelcast initialization...");

        startLatch = new CountDownLatch(1);
        for (int i = 0; i < PARTITION_COUNT; i++) {
            clusterMigrationSemaphores[i] = new Semaphore(1, true);
        }

        for (int i = 0; i < SERVER_INSTANCE_COUNT; i++) {
            HazelcastInstance server = Hazelcast.newHazelcastInstance(createServerConfig());
            server.getPartitionService().addMigrationListener(new StressTestMigrationListener());
            serverInstances.add(server);
        }

        client = HazelcastClient.newHazelcastClient(createClientConfig());
        client.getCluster().addMembershipListener(new StressTestMembershipListener());

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

        for (int i = 1; i <= RUNNING_TIME_SECONDS; i++) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            float percent = (i * 100.0f) / RUNNING_TIME_SECONDS;
            System.out.printf("  %5.1f%% done after running for %3d of %3d seconds", percent, i, RUNNING_TIME_SECONDS);
            if (clusterChangeEnabled) {
                System.out.printf(" (%d running migrations)", getRunningMigrationCount());
            }
            System.out.println();

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
            if (!changeClusterThread.isInterrupted()) {
                System.out.println("  Interrupting cluster change thread...");
                changeClusterThread.interrupt();
            }

            System.out.println("  Joining cluster change thread...");
            joinThread(changeClusterThread);

            System.out.printf("  Waiting to finish %d migration events...\n", getRunningMigrationCount());
            for (Semaphore semaphore : clusterMigrationSemaphores) {
                semaphore.acquireUninterruptibly();
            }

            System.out.printf("  Migrations started: %d, Migrations completed: %d, Migrations failed: %d\n",
                    migrationStartedCounter.get(),
                    migrationCompletedCounter.get(),
                    migrationFailedCounter.get()
            );
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

    private int getRunningMigrationCount() {
        int count = 0;
        for (Semaphore semaphore : clusterMigrationSemaphores) {
            if (semaphore.availablePermits() == 0) {
                count++;
            }
        }
        return count;
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
                firstRunAction();
                while (running && !isInterrupted()) {
                    runAction();
                }
            } catch (InterruptedException ignored) {
                System.out.println("  Thread " + getName() + " got interrupted!");
            } catch (Throwable t) {
                running = false;

                t.printStackTrace();
                this.error = t;
            }
        }

        protected void firstRunAction() {
        }

        protected abstract void runAction();

        private void assertNoError() {
            assertNull(getName() + " encountered an error", error);
        }
    }

    private class ChangeClusterThread extends TestThread {
        @Override
        protected void firstRunAction() {
            sleep();
        }

        @Override
        public void runAction() {
            if (CHANGE_CLUSTER_REMOVE_BEFORE_ADD) {
                removeServer();
                addServer();
            } else {
                addServer();
                removeServer();
            }

            sleep();
        }

        private void removeServer() {
            int index = random.nextInt(SERVER_INSTANCE_COUNT);
            serverInstances.remove(index).shutdown();
        }

        private void addServer() {
            HazelcastInstance server = Hazelcast.newHazelcastInstance(createServerConfig());
            server.getPartitionService().addMigrationListener(new StressTestMigrationListener());
            serverInstances.add(server);
        }

        private void sleep() {
            try {
                TimeUnit.SECONDS.sleep(CHANGE_CLUSTER_INTERVAL_SECONDS);
            } catch (InterruptedException e) {
                interrupt();
            }
        }
    }

    private class StressTestMembershipListener implements MembershipListener {
        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
            System.out.println("  Added " + membershipEvent.getMember());
        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            System.out.println("  Removed " + membershipEvent.getMember());
        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
            System.out.println("  " + memberAttributeEvent.getMember()
                            + " changed " + memberAttributeEvent.getKey() + " to " + memberAttributeEvent.getValue()
            );
        }
    }

    private class StressTestMigrationListener implements MigrationListener {
        @Override
        public void migrationStarted(MigrationEvent migrationEvent) {
            clusterMigrationSemaphores[migrationEvent.getPartitionId()].acquireUninterruptibly();
            migrationStartedCounter.incrementAndGet();
        }

        @Override
        public void migrationCompleted(MigrationEvent migrationEvent) {
            int partitionId = migrationEvent.getPartitionId();
            Semaphore semaphore = clusterMigrationSemaphores[partitionId];
            if (semaphore.availablePermits() == 0) {
                clusterMigrationSemaphores[partitionId].release();
            } else {
                System.err.printf(
                        "[ERROR] Migration completed without started event vs. last event:\n%s\n%s\n",
                        migrationEvent, clusterMigrationEvents.get(partitionId)
                );
            }
            clusterMigrationEvents.put(partitionId, migrationEvent.toString());
            migrationCompletedCounter.incrementAndGet();
        }

        @Override
        public void migrationFailed(MigrationEvent migrationEvent) {
            int partitionId = migrationEvent.getPartitionId();
            Semaphore semaphore = clusterMigrationSemaphores[partitionId];
            if (semaphore.availablePermits() == 0) {
                System.err.println("[ERROR] Migration failed: " + migrationEvent);
                clusterMigrationSemaphores[partitionId].release();
            } else {
                System.err.printf(
                        "[ERROR] Migration failed without started event vs. last event:\n%s\n%s\n",
                        migrationEvent, clusterMigrationEvents.get(partitionId)
                );
            }
            clusterMigrationEvents.put(partitionId, migrationEvent.toString());
            migrationFailedCounter.incrementAndGet();
        }
    }
}
