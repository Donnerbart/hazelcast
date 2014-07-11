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

package com.hazelcast.client.replicatedmap;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.test.WatchedOperationExecutor;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

public class ClientReplicatedMapClearTest extends AbstractClientReplicatedMapTest<String, String> {

    @Test
    public void testClearObjectDelay0() throws Exception {
        testClear(InMemoryFormat.OBJECT, 0);
    }

    @Test
    public void testClearObjectDelayDefault() throws Exception {
        testClear(InMemoryFormat.OBJECT, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
    }

    @Test
    public void testClearBinaryDelay0() throws Exception {
        testClear(InMemoryFormat.BINARY, 0);
    }

    @Test
    public void testClearBinaryDelayDefault() throws Exception {
        testClear(InMemoryFormat.BINARY, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
    }

    private void testClear(InMemoryFormat inMemoryFormat, long replicationDelay) throws Exception {
        setup(buildConfig(inMemoryFormat, replicationDelay));

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < OPERATIONS; i++) {
                    serverMap.put("foo-" + i, "bar");
                }
            }
        }, 60, EntryEventType.ADDED, OPERATIONS, 0.75, serverMap, clientMap);

        for (Map.Entry<String, String> entry : clientMap.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }

        for (Map.Entry<String, String> entry : serverMap.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }

        // TODO Should clear be a synchronous operation? What happens on lost clear message?
        final AtomicBoolean happened = new AtomicBoolean(false);
        for (int i = 0; i < 10; i++) {
            serverMap.clear();
            Thread.sleep(1000);
            try {
                assertEquals(0, serverMap.size());
                assertEquals(0, clientMap.size());
                happened.set(true);
            } catch (AssertionError ignored) {
                // Ignore and retry
            }
            if (happened.get()) {
                break;
            }
        }
    }
}
