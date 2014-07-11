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
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;

public class ClientReplicatedMapRemoveTest extends AbstractClientReplicatedMapTest<String, String> {

    @Test
    public void testRemoveObjectDelay0() throws TimeoutException {
        testRemove(InMemoryFormat.OBJECT, 0);
    }

    @Test
    public void testRemoveObjectDelayDefault() throws TimeoutException {
        testRemove(InMemoryFormat.OBJECT, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
    }

    @Test
    public void testRemoveBinaryDelay0() throws TimeoutException {
        testRemove(InMemoryFormat.BINARY, 0);
    }

    @Test
    public void testRemoveBinaryDelayDefault() throws TimeoutException {
        testRemove(InMemoryFormat.BINARY, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
    }

    private void testRemove(InMemoryFormat inMemoryFormat, long replicationDelay) throws TimeoutException {
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
        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < OPERATIONS; i++) {
                    clientMap.remove("foo-" + i);
                }
            }
        }, 60, EntryEventType.REMOVED, OPERATIONS, 0.75, serverMap, clientMap);

        int clientMapUpdated = 0;
        for (int i = 0; i < OPERATIONS; i++) {
            Object value = clientMap.get("foo-" + i);
            if (value == null) {
                clientMapUpdated++;
            }
        }
        int serverMapUpdated = 0;
        for (int i = 0; i < OPERATIONS; i++) {
            Object value = serverMap.get("foo-" + i);
            if (value == null) {
                serverMapUpdated++;
            }
        }

        assertMatchSuccessfulOperationQuota(0.75, OPERATIONS, serverMapUpdated, clientMapUpdated);
    }
}
