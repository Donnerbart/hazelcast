package com.hazelcast.client.stress;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ProblematicTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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

    @Test
    @Ignore
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
        int[] globalIncrements = new int[REFERENCE_COUNT];
        for (StressThread thread : stressThreads) {
            thread.addThreadIncrements(globalIncrements);
        }

        int failCount = 0;
        for (int i = 0; i < REFERENCE_COUNT; i++) {
            long expectedValue = globalIncrements[i];
            long actualValue = references[i].get();
            if (expectedValue != actualValue) {
                System.err.printf("Failed write #%4d: actual: %5d, expected: %5d\n", ++failCount, actualValue, expectedValue);
            }
        }

        if (failCount > 0) {
            fail(String.format("There are %d failed writes...", failCount));
        }
    }

    private class StressThread extends TestThread {
        private final int[] threadIncrements = new int[REFERENCE_COUNT];

        @Override
        public void doRun() throws Exception {
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
