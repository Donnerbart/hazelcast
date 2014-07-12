package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ProblematicTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.fail;

/**
 * This test fails sporadically. It seems to indicate a problem within the core because there is not much logic
 * in the AtomicWrapper that can fail (just increment).
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class AtomicLongUpdateStressTest extends StressTestSupport {

    private static final int REFERENCE_COUNT = 10000;

    private IAtomicLong[] references;
    private StressThread[] stressThreads;

    @Before
    public void setup() {
        super.setup();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        references = new IAtomicLong[REFERENCE_COUNT];
        for (int i = 0; i < REFERENCE_COUNT; i++) {
            references[i] = client.getAtomicLong("atomicReference:" + i);
        }

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
    @Category(ProblematicTest.class)
    public void testFixedCluster() {
        runTest(false);
    }

    private void runTest(boolean clusterChangeEnabled) {
        setClusterChangeEnabled(clusterChangeEnabled);

        startAndWaitForTestCompletion();
        joinAll(stressThreads);

        assertNoUpdateFailures();
    }

    private void assertNoUpdateFailures() {
        int[] increments = new int[REFERENCE_COUNT];
        for (StressThread thread : stressThreads) {
            thread.addIncrements(increments);
        }

        Set<Integer> failedKeys = new HashSet<Integer>();
        for (int i = 0; i < REFERENCE_COUNT; i++) {
            long expectedValue = increments[i];
            long foundValue = references[i].get();
            if (expectedValue != foundValue) {
                failedKeys.add(i);
            }
        }

        if (failedKeys.isEmpty()) {
            return;
        }

        int index = 1;
        for (Integer key : failedKeys) {
            System.err.printf("Failed write: %4d, found: %5d, expected: %5d\n", index, references[key].get(), increments[key]);
            index++;
        }

        fail(String.format("There are %d failed writes...", failedKeys.size()));
    }

    private class StressThread extends TestThread {
        private final int[] threadIncrements = new int[REFERENCE_COUNT];

        @Override
        public void doRun() throws Exception {
            while (!isStopped()) {
                int index = random.nextInt(REFERENCE_COUNT);
                int increment = random.nextInt(100);
                threadIncrements[index] += increment;

                IAtomicLong reference = references[index];
                reference.addAndGet(increment);
            }
        }

        private void addIncrements(int[] increments) {
            for (int i = 0; i < increments.length; i++) {
                increments[i] += threadIncrements[i];
            }
        }
    }
}
