/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.txn;

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientTxnMultiMapTest extends AbstractTxnTest {

    private static final String DEFAULT_KEY = "key";
    private static final String DEFAULT_VALUE = "value";

    private static final String MULTI_MAP_BACKED_BY_LIST = "backedByList*";

    private String backedByListMapName;

    @BeforeClass
    public static void beforeClass() {
        MultiMapConfig multiMapConfig = config.getMultiMapConfig(MULTI_MAP_BACKED_BY_LIST);
        multiMapConfig.setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);

        AbstractTxnTest.beforeClass();
    }

    @Before
    public void setup() {
        super.setup();

        backedByListMapName = MULTI_MAP_BACKED_BY_LIST + randomString();
    }

    @Test
    public void testRemove() throws Exception {
        MultiMap<String, String> multiMap = client.getMultiMap(randomName);
        multiMap.put(DEFAULT_KEY, DEFAULT_VALUE);

        context.beginTransaction();

        TransactionalMultiMap txMultiMap = context.getMultiMap(randomName);
        txMultiMap.remove(DEFAULT_KEY, DEFAULT_VALUE);

        context.commitTransaction();

        assertEquals(Collections.EMPTY_LIST, client.getMultiMap(randomName).get(DEFAULT_KEY));
    }

    @Test
    public void testRemoveAll() throws Exception {
        MultiMap<String, Integer> multiMap = client.getMultiMap(randomName);
        for (int i = 0; i < 10; i++) {
            multiMap.put(DEFAULT_KEY, i);
        }

        context.beginTransaction();

        TransactionalMultiMap txMultiMap = context.getMultiMap(randomName);
        txMultiMap.remove(DEFAULT_KEY);

        context.commitTransaction();

        assertEquals(Collections.EMPTY_LIST, multiMap.get(DEFAULT_KEY));
    }

    @Test
    public void testConcurrentTxnPut() throws Exception {
        final MultiMap<Integer, String> multiMap = client.getMultiMap(randomName);

        int threads = 10;
        ExecutorService ex = Executors.newFixedThreadPool(threads);

        final CountDownLatch latch = new CountDownLatch(threads);
        final AtomicReference<Throwable> error = new AtomicReference<Throwable>(null);

        for (int i = 0; i < threads; i++) {
            final int key = i;
            ex.execute(new Runnable() {
                public void run() {
                    multiMap.put(key, "value");

                    TransactionContext localContext = client.newTransactionContext();
                    try {
                        localContext.beginTransaction();

                        TransactionalMultiMap<Integer, String> transactionalMap = localContext.getMultiMap(randomName);
                        transactionalMap.put(key, "value");
                        transactionalMap.put(key, "value1");
                        transactionalMap.put(key, "value2");

                        assertEquals(3, transactionalMap.get(key).size());

                        localContext.commitTransaction();

                        assertEquals(3, multiMap.get(key).size());
                    } catch (Exception e) {
                        error.compareAndSet(null, e);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        try {
            latch.await(1, TimeUnit.MINUTES);

            assertNull(error.get());
        } finally {
            ex.shutdownNow();
        }
    }

    @Test
    public void testPutAndRoleBack() throws Exception {
        MultiMap<String, String> multiMap = client.getMultiMap(randomName);

        context.beginTransaction();

        TransactionalMultiMap<String, String> transactionalMap = context.getMultiMap(randomName);
        transactionalMap.put(DEFAULT_KEY, DEFAULT_VALUE);
        transactionalMap.put(DEFAULT_KEY, DEFAULT_VALUE);

        context.rollbackTransaction();

        assertEquals(Collections.EMPTY_LIST, multiMap.get(DEFAULT_KEY));
    }

    @Test
    public void testSize() throws Exception {
        MultiMap<String, String> multiMap = client.getMultiMap(randomName);
        multiMap.put(DEFAULT_KEY, DEFAULT_VALUE);

        context.beginTransaction();

        TransactionalMultiMap<String, String> transactionalMap = context.getMultiMap(randomName);
        transactionalMap.put(DEFAULT_KEY, "newValue");
        transactionalMap.put("newKey", DEFAULT_VALUE);

        assertEquals(3, transactionalMap.size());

        context.commitTransaction();
    }

    @Test
    public void testCount() throws Exception {
        MultiMap<String, String> multiMap = client.getMultiMap(randomName);
        multiMap.put(DEFAULT_KEY, DEFAULT_VALUE);

        context.beginTransaction();

        TransactionalMultiMap<String, String> transactionalMap = context.getMultiMap(randomName);
        transactionalMap.put(DEFAULT_KEY, "newValue");

        assertEquals(2, transactionalMap.valueCount(DEFAULT_KEY));

        context.commitTransaction();
    }

    @Test
    public void testGet_whenBackedWithList() throws Exception {
        MultiMap<String, String> multiMap = server.getMultiMap(backedByListMapName);
        multiMap.put(DEFAULT_KEY, DEFAULT_VALUE);

        context.beginTransaction();

        TransactionalMultiMap<String, String> transactionalMap = context.getMultiMap(backedByListMapName);
        Collection<String> collection = transactionalMap.get(DEFAULT_KEY);

        assertFalse(collection.isEmpty());

        context.commitTransaction();
    }

    @Test
    public void testRemove_whenBackedWithList() throws Exception {
        MultiMap<String, String> multiMap = server.getMultiMap(backedByListMapName);
        multiMap.put(DEFAULT_KEY, DEFAULT_VALUE);

        context.beginTransaction();

        TransactionalMultiMap<String, String> transactionalMap = context.getMultiMap(backedByListMapName);
        Collection<String> collection = transactionalMap.remove(DEFAULT_KEY);

        assertFalse(collection.isEmpty());

        context.commitTransaction();
    }
}
