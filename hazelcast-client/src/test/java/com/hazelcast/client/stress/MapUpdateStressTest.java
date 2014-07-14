package com.hazelcast.client.stress;

import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Assume;
import org.junit.Before;
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

        map = client.getMap("map");

        stressThreads = new StressThread[CLIENT_INSTANCE_COUNT];
        for (int i = 0; i < CLIENT_INSTANCE_COUNT; i++) {
            stressThreads[i] = new StressThread();
            stressThreads[i].start();
        }
    }

    @Test
    public void testChangingCluster() {
        Assume.assumeTrue(CHANGE_CLUSTER_TESTS_ACTIVE);
        runTest(true);
    }

    @Test
    public void testFixedCluster() {
        Assume.assumeTrue(FIXED_CLUSTER_TESTS_ACTIVE);
        runTest(false);
    }

    private void runTest(boolean clusterChangeEnabled) {
        fillMap();

        startAndWaitForTestCompletion(clusterChangeEnabled);
        joinAll(stressThreads);

        assertNoUpdateFailures();
    }

    private void assertNoUpdateFailures() {
        System.out.println();
        System.out.println("==================================================================");
        System.out.println("  Assert update failures...");

        int[] globalIncrements = new int[MAP_SIZE];
        for (StressThread thread : stressThreads) {
            thread.addThreadIncrements(globalIncrements);
        }

        int failCount = 0;
        for (int i = 0; i < MAP_SIZE; i++) {
            int expectedValue = globalIncrements[i];
            int actualValue = map.get(i);
            if (expectedValue != actualValue) {
                System.err.printf("  Failed write #%4d: found: %5d, expected: %5d\n", ++failCount, actualValue, expectedValue);
            }
        }

        System.out.println("  Done!");
        System.out.println("==================================================================");

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
        }

        System.out.println("  Done!");
        System.out.println("==================================================================");
    }

    private class StressThread extends TestThread {
        private final int[] threadIncrements = new int[MAP_SIZE];

        @Override
        public void runAction() {
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

        private void addThreadIncrements(int[] increments) {
            for (int i = 0; i < increments.length; i++) {
                increments[i] += threadIncrements[i];
            }
        }
    }
}
