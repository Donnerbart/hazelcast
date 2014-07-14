package com.hazelcast.client.stress;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ProblematicTest;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.fail;

/**
 * This tests verifies that changes on an AtomicLong are not lost. So we have a client which is going to do updates on
 * an AtomicWrapper. In the end we verify that the actual updates in the cluster are the same as the expected updates.
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

        references = new IAtomicLong[REFERENCE_COUNT];
        for (int i = 0; i < REFERENCE_COUNT; i++) {
            references[i] = client.getAtomicLong("atomicReference" + i);
        }

        stressThreads = new StressThread[CLIENT_INSTANCE_COUNT];
        for (int i = 0; i < CLIENT_INSTANCE_COUNT; i++) {
            stressThreads[i] = new StressThread();
            stressThreads[i].start();
        }
    }

    /**
     * This test fails constantly. It seems to indicate a problem within the core (probably at fail over/migration),
     * because there is not much logic in the AtomicWrapper itself and the test with a fixed cluster works.
     */
    @Test
    @Category(ProblematicTest.class)
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
        startAndWaitForTestCompletion(clusterChangeEnabled);
        joinAll(stressThreads);

        assertNoUpdateFailures();
    }

    private void assertNoUpdateFailures() {
        System.out.println();
        System.out.println("==================================================================");
        System.out.println("  Assert update failures...");

        int[] globalIncrements = new int[REFERENCE_COUNT];
        for (StressThread thread : stressThreads) {
            thread.addThreadIncrements(globalIncrements);
        }

        int failCount = 0;
        for (int i = 0; i < REFERENCE_COUNT; i++) {
            long expectedValue = globalIncrements[i];
            long actualValue = references[i].get();
            if (expectedValue != actualValue) {
                System.err.printf(
                        "  Failed write #%4d: actual: %5d, expected: %5d, partition key: %s\n",
                        ++failCount, actualValue, expectedValue, references[i].getPartitionKey()
                );
            }
        }

        System.out.println("  Done!");
        System.out.println("==================================================================");

        if (failCount > 0) {
            fail(String.format("There are %d failed writes...", failCount));
        }
    }

    private class StressThread extends TestThread {
        private final int[] threadIncrements = new int[REFERENCE_COUNT];

        @Override
        public void runAction() {
            int index = random.nextInt(REFERENCE_COUNT);
            int increment = random.nextInt(100);

            threadIncrements[index] += increment;

            IAtomicLong reference = references[index];
            reference.addAndGet(increment);
        }

        private void addThreadIncrements(int[] increments) {
            for (int i = 0; i < increments.length; i++) {
                increments[i] += threadIncrements[i];
            }
        }
    }
}
