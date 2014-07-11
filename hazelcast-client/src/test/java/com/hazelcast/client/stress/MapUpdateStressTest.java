package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.fail;

/**
 * This tests verifies that map updates are not lost. So we have a client which is going to do updates on a map
 * and in the end we verify that the actual updates in the map, are the same as the expected updates.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class MapUpdateStressTest extends StressTestSupport {

    private static final int CLIENT_THREAD_COUNT = 5;
    private static final int MAP_SIZE = 100000;

    private HazelcastInstance client;
    private IMap<Integer, Integer> map;
    private StressThread[] stressThreads;

    @Before
    public void setup() {
        super.setup();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setRedoOperation(true);
        client = HazelcastClient.newHazelcastClient(clientConfig);

        map = client.getMap("map");

        stressThreads = new StressThread[CLIENT_THREAD_COUNT];
        for (int i = 0; i < stressThreads.length; i++) {
            stressThreads[i] = new StressThread();
            stressThreads[i].start();
        }
    }

    @After
    public void teardown() {
        super.tearDown();

        if (client != null) {
            client.shutdown();
        }
    }

    @Ignore
    @Test
    public void testChangingCluster() {
        runTest(true);
    }

    @Test
    public void testFixedCluster() {
        runTest(false);
    }

    private void runTest(boolean clusterChangeEnabled) {
        setClusterChangeEnabled(clusterChangeEnabled);

        fillMap();

        startAndWaitForTestCompletion();
        joinAll(stressThreads);

        assertNoUpdateFailures();
    }

    private void assertNoUpdateFailures() {
        int[] increments = new int[MAP_SIZE];
        for (StressThread thread : stressThreads) {
            thread.addIncrements(increments);
        }

        Set<Integer> failedKeys = new HashSet<Integer>();
        for (int i = 0; i < MAP_SIZE; i++) {
            int expectedValue = increments[i];
            int foundValue = map.get(i);
            if (expectedValue != foundValue) {
                failedKeys.add(i);
            }
        }

        if (failedKeys.isEmpty()) {
            return;
        }

        int index = 1;
        for (Integer key : failedKeys) {
            System.err.printf("Failed write: %4d, found: %5d, expected: %5d\n", index, map.get(key), increments[key]);
            index++;
        }

        fail(String.format("There are %d failed writes...", failedKeys.size()));
    }

    private void fillMap() {
        System.out.println("==================================================================");
        System.out.println("Inserting data in map");
        System.out.println("==================================================================");

        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, 0);
            if (i % 10000 == 0) {
                System.out.println("Inserted data: " + i);
            }
        }

        System.out.println("==================================================================");
        System.out.println("Completed with inserting data in map");
        System.out.println("==================================================================");
    }

    private class StressThread extends TestThread {
        private final int[] increments = new int[MAP_SIZE];

        @Override
        public void doRun() throws Exception {
            while (!isStopped()) {
                int key = random.nextInt(MAP_SIZE);
                int increment = random.nextInt(10);
                increments[key] += increment;
                while (true) {
                    int oldValue = map.get(key);
                    if (map.replace(key, oldValue, oldValue + increment)) {
                        break;
                    }
                }
            }
        }

        private void addIncrements(int[] increments) {
            for (int i = 0; i < increments.length; i++) {
                increments[i] += this.increments[i];
            }
        }
    }
}
