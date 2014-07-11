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

import com.hazelcast.core.ISet;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientTxnSetTest extends AbstractTxnTest {

    private static final String DEFAULT_ELEMENT = "element";

    @Test
    public void testAdd_withinTxn() throws Exception {
        context.beginTransaction();

        TransactionalSet<Object> transactionalSet = context.getSet(randomName);

        assertTrue(transactionalSet.add(DEFAULT_ELEMENT));
        assertEquals(1, transactionalSet.size());

        context.commitTransaction();
    }

    @Test
    public void testSetSizeAfterAdd_withinTxn() throws Exception {
        ISet<String> set = client.getSet(randomName);

        context.beginTransaction();

        TransactionalSet<String> transactionalSet = context.getSet(randomName);
        transactionalSet.add(DEFAULT_ELEMENT);

        context.commitTransaction();

        assertEquals(1, set.size());
    }

    @Test
    public void testRemove_withinTxn() throws Exception {
        ISet<String> set = client.getSet(randomName);
        set.add(DEFAULT_ELEMENT);

        context.beginTransaction();

        TransactionalSet<String> transactionalSet = context.getSet(randomName);
        assertTrue(transactionalSet.remove(DEFAULT_ELEMENT));
        assertFalse(transactionalSet.remove("NOT_THERE"));

        context.commitTransaction();
    }

    @Test
    public void testSetSizeAfterRemove_withinTxn() throws Exception {
        ISet<String> set = client.getSet(randomName);
        set.add(DEFAULT_ELEMENT);

        context.beginTransaction();

        TransactionalSet<String> transactionalSet = context.getSet(randomName);
        transactionalSet.remove(DEFAULT_ELEMENT);

        context.commitTransaction();

        assertEquals(0, set.size());
    }

    @Test
    public void testAddDuplicateElement_withinTxn() throws Exception {
        context.beginTransaction();

        TransactionalSet<String> transactionalSet = context.getSet(randomName);

        assertTrue(transactionalSet.add(DEFAULT_ELEMENT));
        assertFalse(transactionalSet.add(DEFAULT_ELEMENT));

        context.commitTransaction();

        assertEquals(1, client.getSet(randomName).size());
    }

    @Test
    public void testAddExistingElement_withinTxn() throws Exception {
        ISet<String> set = client.getSet(randomName);
        set.add(DEFAULT_ELEMENT);

        context.beginTransaction();

        TransactionalSet<String> transactionalSet = context.getSet(randomName);
        assertFalse(transactionalSet.add(DEFAULT_ELEMENT));

        context.commitTransaction();

        assertEquals(1, set.size());
    }

    @Test
    public void testSetSizeAfterAddingDuplicateElement_withinTxn() throws Exception {
        ISet<String> set = client.getSet(randomName);
        set.add(DEFAULT_ELEMENT);

        context.beginTransaction();

        TransactionalSet<String> transactionalSet = context.getSet(randomName);
        transactionalSet.add(DEFAULT_ELEMENT);

        context.commitTransaction();

        assertEquals(1, set.size());
    }

    @Test
    public void testAddRollBack() throws Exception {
        ISet<String> set = client.getSet(randomName);
        set.add("item1");

        context.beginTransaction();

        TransactionalSet<String> transactionalSet = context.getSet(randomName);
        transactionalSet.add("item2");

        context.rollbackTransaction();

        assertEquals(1, set.size());
    }
}
