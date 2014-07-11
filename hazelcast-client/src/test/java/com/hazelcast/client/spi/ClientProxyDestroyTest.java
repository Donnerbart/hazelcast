package com.hazelcast.client.spi;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientProxyDestroyTest {

    private static HazelcastInstance client;

    private IAtomicLong proxy;

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    public void setup() {
        proxy = client.getAtomicLong(HazelcastTestSupport.randomString());
    }

    @Test
    public void testMultipleDestroyCalls() {
        proxy.destroy();
        proxy.destroy();
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testUsageAfterDestroy() {
        proxy.destroy();

        // Since the object is destroyed, getting the value from the atomic long should fail
        proxy.get();
    }
}
