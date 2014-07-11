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

import com.hazelcast.core.IQueue;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientTxnQueueTest extends AbstractTxnTest {

    private static final String DEFAULT_ITEM = "offered";

    @Test
    public void testTransactionalOfferPoll() {
        context.beginTransaction();

        TransactionalQueue<String> transactionalQueue = context.getQueue(randomName);
        transactionalQueue.offer(DEFAULT_ITEM);

        assertEquals(DEFAULT_ITEM, transactionalQueue.poll());

        context.commitTransaction();
    }

    @Test
    public void testQueueSizeAfterTxnOfferPoll() {
        IQueue queue = client.getQueue(randomName);

        context.beginTransaction();

        TransactionalQueue<String> transactionalQueue = context.getQueue(randomName);
        transactionalQueue.offer(DEFAULT_ITEM);
        transactionalQueue.poll();

        context.commitTransaction();

        assertEquals(0, queue.size());
    }

    @Test
    public void testTransactionalOfferTake() throws InterruptedException {
        context.beginTransaction();

        TransactionalQueue<String> transactionalQueue = context.getQueue(randomName);

        assertTrue(transactionalQueue.offer(DEFAULT_ITEM));
        assertEquals(1, transactionalQueue.size());
        assertEquals(DEFAULT_ITEM, transactionalQueue.take());

        context.commitTransaction();
    }

    @Test
    public void testTransactionalQueueGetsOfferedItems_whenBlockedOnPoll() throws InterruptedException{
        final IQueue<String> queue = client.getQueue(randomName);
        final CountDownLatch justBeforeBlocked = new CountDownLatch(1);

        new Thread() {
            public void run() {
                try {
                    justBeforeBlocked.await();
                    sleepSeconds(1);
                    queue.offer(DEFAULT_ITEM);
                } catch (InterruptedException e) {
                    fail("failed" + e);
                }
            }
        }.start();

        context.beginTransaction();

        TransactionalQueue<String> transactionalQueue = context.getQueue(randomName);
        justBeforeBlocked.countDown();
        String result = transactionalQueue.poll(5, TimeUnit.SECONDS);

        assertEquals("TransactionalQueue while blocked in pol should get item offered from client queue", DEFAULT_ITEM, result);

        context.commitTransaction();
    }

    @Test
    public void testTransactionalPeek() {
        context.beginTransaction();

        TransactionalQueue<String> transactionalQueue = context.getQueue(randomName);
        transactionalQueue.offer(DEFAULT_ITEM);

        assertEquals(DEFAULT_ITEM, transactionalQueue.peek());
        assertEquals(DEFAULT_ITEM, transactionalQueue.peek());

        context.commitTransaction();
    }

    @Test
    public void testTransactionalOfferRoleBack() {
        IQueue queue = client.getQueue(randomName);

        context.beginTransaction();

        TransactionalQueue<String> transactionalQueue = context.getQueue(randomName);
        transactionalQueue.offer(DEFAULT_ITEM);

        context.rollbackTransaction();

        assertEquals(0, queue.size());
    }

    @Test
    public void testTransactionalQueueSize() {
        IQueue<String> queue = client.getQueue(randomName);
        queue.offer(DEFAULT_ITEM);

        context.beginTransaction();

        TransactionalQueue<String> transactionalQueue = context.getQueue(randomName);
        transactionalQueue.offer(DEFAULT_ITEM);

        assertEquals(2, transactionalQueue.size());

        context.rollbackTransaction();
    }

    @Test
    public void testTransactionalOfferAndPollWithTimeout() throws InterruptedException {
        context.beginTransaction();

        TransactionalQueue<String> transactionalQueue = context.getQueue(randomName);

        assertTrue(transactionalQueue.offer(DEFAULT_ITEM));
        assertEquals(1, transactionalQueue.size());
        assertEquals(DEFAULT_ITEM, transactionalQueue.poll(5, TimeUnit.SECONDS));

        context.commitTransaction();
    }
}