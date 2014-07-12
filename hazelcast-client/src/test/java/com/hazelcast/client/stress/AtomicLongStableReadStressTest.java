package com.hazelcast.client.stress;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class AtomicLongStableReadStressTest extends StressTestSupport {

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
    public void testChangingCluster() {
        runTest(true);
    }

    @Test
    public void testFixedCluster() {
        runTest(false);
    }

    private void runTest(boolean clusterChangeEnabled) {
        setClusterChangeEnabled(clusterChangeEnabled);

        initializeReferences();

        startAndWaitForTestCompletion();
        joinAll(stressThreads);
    }

    private void initializeReferences() {
        System.out.println();
        System.out.println("==================================================================");
        System.out.println("  Initializing references...");

        for (int i = 0; i < REFERENCE_COUNT; i++) {
            references[i].set(i);
        }

        System.out.println("  Done!");
        System.out.println("==================================================================");
    }

    private class StressThread extends TestThread {
        @Override
        public void runAction() {
            int key = random.nextInt(REFERENCE_COUNT);
            IAtomicLong reference = references[key];

            assertEquals(String.format("The value for atomic reference %s was not consistent", reference), key, reference.get());
        }
    }
}
