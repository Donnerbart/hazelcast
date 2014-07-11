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

import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author ali 6/10/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientTxnMapTest extends AbstractTxnTest {

    private static final String DEFAULT_KEY = "key";
    private static final String DEFAULT_VALUE = "value";

    @Test
    public void testUnlockAfterRollback() {
        context.beginTransaction();

        TransactionalMap<String, String> transactionalMap = context.getMap(randomName);
        transactionalMap.put(DEFAULT_KEY, DEFAULT_VALUE);

        context.rollbackTransaction();

        assertFalse(client.getMap(randomName).isLocked(DEFAULT_KEY));
    }

    @Test
    public void testDeadLockFromClientInstance() throws InterruptedException {
        final AtomicBoolean running = new AtomicBoolean(true);

        Thread thread = new Thread() {
            public void run() {
                while (running.get()) {
                    client.getMap(randomName).get(DEFAULT_KEY);
                }
            }
        };
        thread.start();

        CBAuthorisation cb = new CBAuthorisation();
        cb.setAmount(15000);

        try {
            context.beginTransaction();

            TransactionalMap<String, CBAuthorisation> transactionalMap = context.getMap(randomName);
            // Init data
            transactionalMap.put(DEFAULT_KEY, cb);
            // Start test deadlock, 3 set and concurrent, get deadlock

            cb.setAmount(12000);
            transactionalMap.set(DEFAULT_KEY, cb);

            cb.setAmount(10000);
            transactionalMap.set(DEFAULT_KEY, cb);

            cb.setAmount(900);
            transactionalMap.set(DEFAULT_KEY, cb);

            cb.setAmount(800);
            transactionalMap.set(DEFAULT_KEY, cb);

            cb.setAmount(700);
            transactionalMap.set(DEFAULT_KEY, cb);

            context.commitTransaction();

        } catch (TransactionException e) {
            e.printStackTrace();
            fail();
        }
        running.set(false);
        thread.join();
    }

    @Test
    public void testTxnMapPut() throws Exception {
        IMap<String, String> map = client.getMap(randomName);

        context.beginTransaction();

        TransactionalMap<String, String> transactionalMap = context.getMap(randomName);
        transactionalMap.put(DEFAULT_KEY, DEFAULT_VALUE);

        context.commitTransaction();

        assertEquals(DEFAULT_VALUE, map.get(DEFAULT_KEY));
    }

    @Test
    public void testTxnMapPut_BeforeCommit() throws Exception {
        context.beginTransaction();

        TransactionalMap<String, String> transactionalMap = context.getMap(randomName);

        assertNull(transactionalMap.put(DEFAULT_KEY, DEFAULT_VALUE));

        context.commitTransaction();
    }

    @Test
    public void testTxnMapGet_BeforeCommit() throws Exception {
        IMap map = client.getMap(randomName);

        context.beginTransaction();

        TransactionalMap<String, String> transactionalMap = context.getMap(randomName);

        transactionalMap.put(DEFAULT_KEY, DEFAULT_VALUE);

        assertEquals(DEFAULT_VALUE, transactionalMap.get(DEFAULT_KEY));
        assertNull(map.get(DEFAULT_KEY));

        context.commitTransaction();
    }

    @Test
    public void testPutWithTTL() {
        int ttlSeconds = 1;
        IMap map = client.getMap(randomName);

        context.beginTransaction();

        TransactionalMap<String, String> transactionalMap = context.getMap(randomName);

        transactionalMap.put(DEFAULT_KEY, DEFAULT_VALUE, ttlSeconds, TimeUnit.SECONDS);
        Object resultFromClientWhileTxnInProgress = map.get(DEFAULT_KEY);

        context.commitTransaction();

        assertNull(resultFromClientWhileTxnInProgress);
        assertEquals(DEFAULT_VALUE, map.get(DEFAULT_KEY));

        // Wait for ttl to expire
        sleepSeconds(ttlSeconds + 1);

        assertNull(map.get(DEFAULT_KEY));
    }

    @Test
    public void testGetForUpdate() throws TransactionException {
        int initialValue = 111;
        final int value = 888;

        final CountDownLatch getKeyForUpdateLatch = new CountDownLatch(1);
        final CountDownLatch afterTryPutResult = new CountDownLatch(1);

        final IMap<String, Integer> map = client.getMap(randomName);
        map.put(DEFAULT_KEY, initialValue);

        final AtomicBoolean tryPutResult = new AtomicBoolean(true);
        Runnable runnable = new Runnable() {
            public void run() {
                try {
                    getKeyForUpdateLatch.await(30, TimeUnit.SECONDS);

                    boolean result = map.tryPut(DEFAULT_KEY, value, 0, TimeUnit.SECONDS);
                    tryPutResult.set(result);

                    afterTryPutResult.countDown();
                } catch (Exception ignored) {
                }
            }
        };
        new Thread(runnable).start();

        client.executeTransaction(new TransactionalTask<Boolean>() {
            public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                try {
                    TransactionalMap<String, Integer> transactionalMap = context.getMap(randomName);
                    transactionalMap.getForUpdate(DEFAULT_KEY);
                    getKeyForUpdateLatch.countDown();
                    afterTryPutResult.await(30, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                }
                return true;
            }
        });

        assertFalse(tryPutResult.get());
    }

    @Test
    public void testKeySetValues() throws Exception {
        IMap<String, String> map = client.getMap(randomName);
        map.put("key1", "value1");

        context.beginTransaction();
        TransactionalMap<String, String> transactionalMap = context.getMap(randomName);

        assertNull(transactionalMap.put("key2", "value2"));
        assertEquals(2, transactionalMap.size());
        assertEquals(2, transactionalMap.keySet().size());
        assertEquals(2, transactionalMap.values().size());

        context.commitTransaction();

        assertEquals(2, map.size());
        assertEquals(2, map.keySet().size());
        assertEquals(2, map.values().size());
    }

    @Test
    public void testKeySetAndValuesWithPredicates() throws Exception {
        SampleObjects.Employee emp1 = new SampleObjects.Employee("abc-123-xvz", 34, true, 10D);
        SampleObjects.Employee emp2 = new SampleObjects.Employee("abc-123-xvz", 20, true, 10D);

        IMap<SampleObjects.Employee, SampleObjects.Employee> map = client.getMap(randomName);
        map.put(emp1, emp1);

        context.beginTransaction();
        TransactionalMap<SampleObjects.Employee, SampleObjects.Employee> transactionalMap = context.getMap(randomName);

        assertNull(transactionalMap.put(emp2, emp2));
        assertEquals(2, transactionalMap.size());
        assertEquals(2, transactionalMap.keySet().size());
        assertEquals(0, transactionalMap.keySet(new SqlPredicate("age = 10")).size());
        assertEquals(0, transactionalMap.values(new SqlPredicate("age = 10")).size());
        assertEquals(2, transactionalMap.keySet(new SqlPredicate("age >= 10")).size());
        assertEquals(2, transactionalMap.values(new SqlPredicate("age >= 10")).size());

        context.commitTransaction();

        assertEquals(2, map.size());
        assertEquals(2, map.values().size());
    }

    @Test
    public void testPutAndRoleBack() throws Exception {
        IMap<String, String> map = client.getMap(randomName);

        context.beginTransaction();

        TransactionalMap<String, String> transactionalMap = context.getMap(randomName);
        transactionalMap.put(DEFAULT_KEY, DEFAULT_VALUE);

        context.rollbackTransaction();

        assertNull(map.get(DEFAULT_KEY));
    }

    @Test
    public void testTnxMapContainsKey() throws Exception {
        IMap<String, String> map = client.getMap(randomName);
        map.put("key1", "value1");

        context.beginTransaction();

        TransactionalMap<String, String> transactionalMap = context.getMap(randomName);
        transactionalMap.put("key2", "value2");

        assertTrue(transactionalMap.containsKey("key1"));
        assertTrue(transactionalMap.containsKey("key2"));
        assertFalse(transactionalMap.containsKey("key3"));

        context.commitTransaction();
    }

    @Test
    public void testTnxMapIsEmpty() throws Exception {
        context.beginTransaction();

        TransactionalMap transactionalMap = context.getMap(randomName);

        assertTrue(transactionalMap.isEmpty());

        context.commitTransaction();
    }

    @Test
    public void testTnxMapPutIfAbsent() throws Exception {
        String keyValue1 = "keyValue1";
        String keyValue2 = "keyValue2";

        IMap<String, String> map = client.getMap(randomName);
        map.put(keyValue1, keyValue1);

        context.beginTransaction();
        TransactionalMap<String, String> transactionalMap = context.getMap(randomName);

        transactionalMap.putIfAbsent(keyValue1, "NOT_THIS");
        transactionalMap.putIfAbsent(keyValue2, keyValue2);

        context.commitTransaction();

        assertEquals(keyValue1, map.get(keyValue1));
        assertEquals(keyValue2, map.get(keyValue2));
    }

    @Test
    public void testTnxMapReplace() throws Exception {
        String key1 = "key1";
        String key2 = "key2";
        String replaceValue = "replaceValue";

        IMap<String, String> map = client.getMap(randomName);
        map.put(key1, "OLD_VALUE");

        context.beginTransaction();
        TransactionalMap<String, String> transactionalMap = context.getMap(randomName);

        transactionalMap.replace(key1, replaceValue);
        transactionalMap.replace(key2, "NOT_POSSIBLE");

        context.commitTransaction();

        assertEquals(replaceValue, map.get(key1));
        assertNull(map.get(key2));
    }

    @Test
    public void testTnxMapReplaceKeyValue() throws Exception {
        String key1 = "key1";
        String key2 = "key2";
        String oldValue1 = "old1";
        String oldValue2 = "old2";
        String newValue1 = "new1";

        IMap<String, String> map = client.getMap(randomName);
        map.put(key1, oldValue1);
        map.put(key2, oldValue2);

        context.beginTransaction();
        TransactionalMap<String, String> transactionalMap = context.getMap(randomName);

        transactionalMap.replace(key1, oldValue1, newValue1);
        transactionalMap.replace(key2, "NOT_OLD_VALUE", "NEW_VALUE_CANT_BE_THIS");

        context.commitTransaction();

        assertEquals(newValue1, map.get(key1));
        assertEquals(oldValue2, map.get(key2));
    }

    @Test
    public void testTnxMapRemove() throws Exception {
        IMap<String, String> map = client.getMap(randomName);
        map.put(DEFAULT_KEY, DEFAULT_VALUE);

        context.beginTransaction();
        TransactionalMap<String, String> transactionalMap = context.getMap(randomName);

        transactionalMap.remove(DEFAULT_KEY);

        context.commitTransaction();

        assertNull(map.get(DEFAULT_KEY));
    }

    @Test
    public void testTnxMapRemoveKeyValue() throws Exception {
        String key1 = "key1";
        String key2 = "key2";
        String oldValue1 = "old1";
        String oldValue2 = "old2";

        IMap<String, String> map = client.getMap(randomName);
        map.put(key1, oldValue1);
        map.put(key2, oldValue2);

        context.beginTransaction();
        TransactionalMap<String, String> transactionalMap = context.getMap(randomName);

        transactionalMap.remove(key1, oldValue1);
        transactionalMap.remove(key2, "NO_REMOVE_AS_NOT_VALUE");

        context.commitTransaction();

        assertNull(map.get(key1));
        assertEquals(oldValue2, map.get(key2));
    }

    @Test
    public void testTnxMapDelete() throws Exception {
        IMap<String, String> map = client.getMap(randomName);
        map.put(DEFAULT_KEY, DEFAULT_VALUE);

        context.beginTransaction();
        TransactionalMap<String, String> transactionalMap = context.getMap(randomName);

        transactionalMap.delete(DEFAULT_KEY);

        context.commitTransaction();

        assertNull(map.get(DEFAULT_KEY));
    }

    @Test(expected = NullPointerException.class)
    public void testKeySetPredicateNull() throws Exception {
        context.beginTransaction();

        TransactionalMap<Object, Object> transactionalMap = context.getMap(randomName);

        transactionalMap.keySet(null);
    }

    @Test(expected = NullPointerException.class)
    public void testKeyValuesPredicateNull() throws Exception {
        context.beginTransaction();

        TransactionalMap<Object, Object> transactionalMap = context.getMap(randomName);

        transactionalMap.values(null);
    }

    @SuppressWarnings("unused")
    private static class CBAuthorisation implements Serializable {
        private int amount;

        public void setAmount(int amount) {
            this.amount = amount;
        }

        public int getAmount() {
            return amount;
        }
    }
}
