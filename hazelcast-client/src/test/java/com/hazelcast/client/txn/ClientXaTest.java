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

import com.atomikos.icatch.jta.UserTransactionManager;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientXaTest extends HazelcastTestSupport {

    private static final Random random = new Random(System.currentTimeMillis());

    private UserTransactionManager userTransactionManager;

    private HazelcastInstance server;
    private HazelcastInstance client;

    @Before
    public void setup() throws SystemException {
        userTransactionManager = new UserTransactionManager();
        userTransactionManager.setTransactionTimeout(60);

        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @After
    public void teardown() {
        userTransactionManager.close();
        cleanTransactionLogs();

        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testRollbackAfterNodeShutdown() throws Exception {
        userTransactionManager.begin();

        TransactionContext context = client.newTransactionContext();
        XAResource xaResource = context.getXaResource();
        Transaction transaction = userTransactionManager.getTransaction();
        transaction.enlistResource(xaResource);

        boolean error = false;
        try {
            TransactionalMap<String, String> transactionalMap = context.getMap("map");
            transactionalMap.put("key", "value");

            throw new RuntimeException("Exception for rolling back");
        } catch (Exception e) {
            error = true;
        } finally {
            close(error, xaResource);
        }

        assertNull(client.getMap("map").get("key"));
    }

    @Test
    public void testRecovery() throws Exception {
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

        TransactionContext context1 = client.newTransactionContext(TransactionOptions.getDefault().setDurability(2));
        XAResource xaResource = context1.getXaResource();

        TestXid testXid = new TestXid();
        xaResource.start(testXid, 0);

        TransactionalMap<String, String> transactionalMap = context1.getMap("map");
        transactionalMap.put("key", "value");
        xaResource.prepare(testXid);
        client.shutdown();

        assertNull(server.getMap("map").get("key"));

        client = HazelcastClient.newHazelcastClient();
        TransactionContext context2 = client.newTransactionContext();
        xaResource = context2.getXaResource();

        Xid[] recover = xaResource.recover(0);
        for (Xid xid : recover) {
            xaResource.commit(xid, false);
        }

        assertEquals("value", server.getMap("map").get("key"));

        try {
            // For setting ThreadLocal of unfinished transaction
            context1.rollbackTransaction();
        } catch (Throwable ignored) {
        }
    }

    @Test
    public void testIsSame() throws Exception {
        HazelcastInstance server2 = Hazelcast.newHazelcastInstance();

        XAResource resource1 = server.newTransactionContext().getXaResource();
        XAResource resource2 = server2.newTransactionContext().getXaResource();

        XAResource clientResource = client.newTransactionContext().getXaResource();

        assertTrue(clientResource.isSameRM(resource1));
        assertTrue(clientResource.isSameRM(resource2));
    }

    @Test
    public void testTimeoutSetting() throws Exception {
        XAResource resource = server.newTransactionContext().getXaResource();

        int timeout = 100;
        boolean result = resource.setTransactionTimeout(timeout);

        assertTrue(result);
        assertEquals(timeout, resource.getTransactionTimeout());

        TestXid testXid = new TestXid();
        resource.start(testXid, 0);

        assertFalse(resource.setTransactionTimeout(120));
        assertEquals(timeout, resource.getTransactionTimeout());

        resource.commit(testXid, true);
    }

    @Test
    public void testParallel() throws Exception {
        int size = 20;
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        final CountDownLatch latch = new CountDownLatch(size);
        for (int i = 0; i < size; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    try {
                        doTransaction(client);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        assertOpenEventually(latch, 20);

        final IMap<Integer, String> map = client.getMap("map");
        for (int i = 0; i < 10; i++) {
            assertFalse(map.isLocked(i));
        }
    }

    @Test
    public void testSequential() throws Exception {
        doTransaction(client);
        doTransaction(client);
        doTransaction(client);
    }

    private void doTransaction(HazelcastInstance instance) throws Exception {
        userTransactionManager.begin();

        TransactionContext context = instance.newTransactionContext();
        XAResource xaResource = context.getXaResource();
        Transaction transaction = userTransactionManager.getTransaction();
        transaction.enlistResource(xaResource);

        boolean error = false;
        try {
            TransactionalMap<Integer, String> transactionalMap = context.getMap("map");
            transactionalMap.put(random.nextInt(10), "value");
        } catch (Exception e) {
            e.printStackTrace();
            error = true;
        } finally {
            close(error, xaResource);
        }
    }

    private void cleanTransactionLogs() {
        try {
            File currentDir = new File(".");

            File[] tmLogs = currentDir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.endsWith(".epoch") || name.startsWith("tmlog");
                }
            });

            for (File tmLog : tmLogs) {
                if (!tmLog.delete()) {
                    throw new RuntimeException("Could not delete log " + tmLog.getName());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close(boolean error, XAResource... xaResource) throws Exception {
        int flag = error ? XAResource.TMFAIL : XAResource.TMSUCCESS;
        Transaction transaction = userTransactionManager.getTransaction();

        for (XAResource resource : xaResource) {
            transaction.delistResource(resource, flag);
        }

        if (error) {
            userTransactionManager.rollback();
        } else {
            userTransactionManager.commit();
        }
    }

    private static class TestXid implements Xid {

        public int getFormatId() {
            return 42;
        }

        @Override
        public byte[] getGlobalTransactionId() {
            return "GlobalTransactionId".getBytes();
        }

        @Override
        public byte[] getBranchQualifier() {
            return "BranchQualifier".getBytes();
        }
    }
}
