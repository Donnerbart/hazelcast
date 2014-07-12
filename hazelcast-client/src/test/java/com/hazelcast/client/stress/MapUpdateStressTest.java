package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.fail;

/**
 * This tests verifies that map updates are not lost. So we have a client which is going to do updates on a map
 * and in the end we verify that the actual updates in the map, are the same as the expected updates.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class MapUpdateStressTest extends StressTestSupport {

    private static final int MAP_SIZE = 100000;

    private IMap<Integer, Integer> map;
    private StressThread[] stressThreads;

    @Before
    public void setup() {
        super.setup();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        map = client.getMap("map");

        stressThreads = new StressThread[CLIENT_INSTANCE_COUNT];
        for (int i = 0; i < CLIENT_INSTANCE_COUNT; i++) {
            stressThreads[i] = new StressThread();
            stressThreads[i].start();
        }
    }

    @Test
    @Ignore
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
        int[] globalIncrements = new int[MAP_SIZE];
        for (StressThread thread : stressThreads) {
            thread.addThreadIncrements(globalIncrements);
        }

        int failCount = 0;
        for (int i = 0; i < MAP_SIZE; i++) {
            int expectedValue = globalIncrements[i];
            int actualValue = map.get(i);
            if (expectedValue != actualValue) {
                System.err.printf("Failed write #%4d: found: %5d, expected: %5d\n", ++failCount, actualValue, expectedValue);
            }
        }

        if (failCount > 0) {
            fail(String.format("There are %d failed writes...", failCount));
        }
    }

    private void fillMap() {
        System.out.println();
        System.out.println("==================================================================");
        System.out.println("  Inserting data in map...");

        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, 0);
            if (i % 10000 == 0) {
                System.out.println("  Inserted data: " + i);
            }
        }

        System.out.println("  Done!");
        System.out.println("==================================================================");
    }

    private class StressThread extends TestThread {
        private final int[] threadIncrements = new int[MAP_SIZE];

        @Override
        public void doRun() throws Exception {
            while (!isStopped()) {
                int key = random.nextInt(MAP_SIZE);
                int increment = random.nextInt(10);
                threadIncrements[key] += increment;

                while (true) {
                    int oldValue = map.get(key);
                    if (map.replace(key, oldValue, oldValue + increment)) {
                        break;
                    }
                }
            }
        }

        private void addThreadIncrements(int[] increments) {
            for (int i = 0; i < increments.length; i++) {
                increments[i] += threadIncrements[i];
            }
        }
    }
}
