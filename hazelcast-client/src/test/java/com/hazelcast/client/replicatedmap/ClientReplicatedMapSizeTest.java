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
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.test.WatchedOperationExecutor;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.concurrent.TimeoutException;

public class ClientReplicatedMapSizeTest extends AbstractClientReplicatedMapTest<Integer, Integer> {

    @Test
    public void testSizeObjectDelay0() throws TimeoutException {
        testSize(InMemoryFormat.OBJECT, 0);
    }

    @Test
    public void testSizeObjectDelayDefault() throws TimeoutException {
        testSize(InMemoryFormat.OBJECT, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
    }

    @Test
    public void testSizeBinaryDelay0() throws TimeoutException {
        testSize(InMemoryFormat.BINARY, 0);
    }

    @Test
    public void testSizeBinaryDelayDefault() throws TimeoutException {
        testSize(InMemoryFormat.BINARY, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
    }

    private void testSize(InMemoryFormat inMemoryFormat, long replicationDelay) throws TimeoutException {
        setup(buildConfig(inMemoryFormat, replicationDelay));

        final AbstractMap.SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap<Integer, Integer> map = (i < half) ? serverMap : clientMap;
                    final AbstractMap.SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, EntryEventType.ADDED, 100, 0.75, serverMap, clientMap);

        assertMatchSuccessfulOperationQuota(0.75, serverMap.size(), clientMap.size());
    }
}
