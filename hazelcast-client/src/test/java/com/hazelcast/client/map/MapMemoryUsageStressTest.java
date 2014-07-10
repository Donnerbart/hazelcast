package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class MapMemoryUsageStressTest extends HazelcastTestSupport {

    private HazelcastInstance client;

    @Before
    public void setup() {
        Hazelcast.newHazelcastInstance();
        ClientConfig config = new ClientConfig();
        config.setGroupConfig(new GroupConfig("dev", "dev-pass"));
        config.getNetworkConfig().addAddress("127.0.0.1");
        client = HazelcastClient.newHazelcastClient(config);
    }

    @After
    public void teardown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testIssue2138() throws Exception {
        AtomicInteger counter = new AtomicInteger(200000);
        AtomicInteger errors = new AtomicInteger();

        Thread[] threads = new Thread[8];
        for (int i = 0; i < threads.length; i++) {
            StressThread stressThread = new StressThread(counter, errors);
            threads[i] = stressThread;
            stressThread.start();
        }

        assertJoinable(TimeUnit.MINUTES.toSeconds(10), threads);
        assertEquals(0, errors.get());
        assertTrue(counter.get() <= 0);
    }

    private class StressThread extends Thread {
        private final AtomicInteger counter;
        private final AtomicInteger errors;

        public StressThread(AtomicInteger counter, AtomicInteger errors) {
            this.counter = counter;
            this.errors = errors;
        }

        public void run() {
            try {
                while (true) {
                    int index = counter.decrementAndGet();
                    if (index <= 0) {
                        return;
                    }

                    IMap<Object, Object> map = client.getMap("juka" + index);
                    map.set("aaaa", "bbbb");
                    map.clear();
                    map.destroy();

                    if (index % 1000 == 0) {
                        System.out.println("At: " + index);
                    }
                }
            } catch (Throwable t) {
                errors.incrementAndGet();
                t.printStackTrace();
            }
        }
    }
}