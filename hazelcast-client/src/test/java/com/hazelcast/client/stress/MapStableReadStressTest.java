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

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setRedoOperation(true);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        map = client.getMap("map");

        stressThreads = new StressThread[CLIENT_INSTANCE_COUNT];
        for (int i = 0; i < CLIENT_INSTANCE_COUNT; i++) {
            stressThreads[i] = new StressThread();
            stressThreads[i].start();
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
    }

    private void fillMap() {
        System.out.println();
        System.out.println("==================================================================");
        System.out.println("  Inserting data in map...");

        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
            if (i % 10000 == 0) {
                System.out.println("  Inserted data: "+i);
            }
        }

        System.out.println("  Done!");
        System.out.println("==================================================================");
    }

    private class StressThread extends TestThread {
        @Override
        public void doRun() throws Exception {
            while (!isStopped()) {
                int key = random.nextInt(MAP_SIZE);
                int value = map.get(key);
                assertEquals("The value for the key was not consistent", key, value);
            }
        }
    }
}
