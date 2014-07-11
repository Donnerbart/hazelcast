package com.hazelcast.client.stress;

import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * This tests puts a lot of key/values in a map, where the value is the same as the key. With a client these
 * key/values are read and are expected to be consistent, even if member join and leave the cluster all the time.
 * <p/>
 * If there would be a bug in replicating the data, it could pop up here.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class MapStableReadStressTest extends StressTestSupport {

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
    }

    private void fillMap() {
        System.out.println();
        System.out.println("==================================================================");
        System.out.println("  Inserting data in map...");

        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        System.out.println("  Done!");
        System.out.println("==================================================================");
    }

    private class StressThread extends TestThread {
        @Override
        public void runAction() {
            int key = random.nextInt(MAP_SIZE);
            int value = map.get(key);

            assertEquals(String.format("The value for key %d was not consistent", key), key, value);
        }
    }
}
