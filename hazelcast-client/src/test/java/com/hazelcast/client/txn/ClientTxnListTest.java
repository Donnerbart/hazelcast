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

import com.hazelcast.core.IList;
import com.hazelcast.core.TransactionalList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author ali 6/11/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientTxnListTest extends AbstractTxnTest {

    @Test
    public void testAddRemove() throws Exception {
        IList<String> list = client.getList(randomName);
        list.add("item1");

        context.beginTransaction();

        TransactionalList<String> transactionalList = context.getList(randomName);
        assertTrue(transactionalList.add("item2"));
        assertEquals(2, transactionalList.size());
        assertEquals(1, list.size());
        assertFalse(transactionalList.remove("item3"));
        assertTrue(transactionalList.remove("item1"));

        context.commitTransaction();

        assertEquals(1, list.size());
    }

    @Test
    public void testAddAndRollback() throws Exception {
        IList<String> list = client.getList(randomName);
        list.add("item1");

        context.beginTransaction();

        TransactionalList<String> transactionalList = context.getList(randomName);
        transactionalList.add("item2");

        context.rollbackTransaction();

        assertEquals(1, list.size());
    }
}