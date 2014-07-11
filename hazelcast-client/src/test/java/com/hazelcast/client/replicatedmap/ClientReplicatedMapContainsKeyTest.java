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

import java.util.concurrent.TimeoutException;

public class ClientReplicatedMapContainsKeyTest extends AbstractClientReplicatedMapTest<String, String> {

    @Test
    public void testContainsKeyObjectDelay0() throws TimeoutException {
        testContainsKey(InMemoryFormat.OBJECT, 0);
    }

    @Test
    public void testContainsKeyObjectDelayDefault() throws TimeoutException {
        testContainsKey(InMemoryFormat.OBJECT, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
    }

    @Test
    public void testContainsKeyBinaryDelay0() throws TimeoutException {
        testContainsKey(InMemoryFormat.BINARY, 0);
    }

    @Test
    public void testContainsKeyBinaryDelayDefault() throws TimeoutException {
        testContainsKey(InMemoryFormat.BINARY, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
    }

    private void testContainsKey(InMemoryFormat inMemoryFormat, long replicationDelay) throws TimeoutException {
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

        int clientMapContains = 0;
        for (int i = 0; i < OPERATIONS; i++) {
            if (clientMap.containsKey("foo-" + i)) {
                clientMapContains++;
            }
        }
        int serverMapContains = 0;
        for (int i = 0; i < OPERATIONS; i++) {
            if (serverMap.containsKey("foo-" + i)) {
                serverMapContains++;
            }
        }

        assertMatchSuccessfulOperationQuota(0.75, OPERATIONS, serverMapContains, clientMapContains);
    }
}
