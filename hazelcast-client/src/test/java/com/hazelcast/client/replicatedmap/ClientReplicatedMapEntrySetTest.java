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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class ClientReplicatedMapEntrySetTest extends AbstractClientReplicatedMapTest<Integer, Integer> {

    @Test
    public void testEntrySetObjectDelay0() throws TimeoutException {
        testEntrySet(InMemoryFormat.OBJECT, 0);
    }

    @Test
    public void testEntrySetObjectDelayDefault() throws TimeoutException {
        testEntrySet(InMemoryFormat.OBJECT, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
    }

    @Test
    public void testEntrySetBinaryDelay0() throws TimeoutException {
        testEntrySet(InMemoryFormat.BINARY, 0);
    }

    @Test
    public void testEntrySetBinaryDelayDefault() throws TimeoutException {
        testEntrySet(InMemoryFormat.BINARY, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
    }

    private void testEntrySet(InMemoryFormat inMemoryFormat, long replicationDelay) throws TimeoutException {
        setup(buildConfig(inMemoryFormat, replicationDelay));

        final AbstractMap.SimpleEntry<Integer, Integer>[] testValues = buildTestValues();
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap<Integer, Integer> map = i < half ? serverMap : clientMap;
                    final AbstractMap.SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, EntryEventType.ADDED, 100, 0.75, serverMap, clientMap);

        List<Map.Entry<Integer, Integer>> entrySet1 = new ArrayList<Map.Entry<Integer, Integer>>(serverMap.entrySet());
        List<Map.Entry<Integer, Integer>> entrySet2 = new ArrayList<Map.Entry<Integer, Integer>>(clientMap.entrySet());

        int clientMapContains = 0;
        for (Map.Entry<Integer, Integer> entry : entrySet2) {
            Integer value = findValue(entry.getKey(), testValues);
            if (value.equals(entry.getValue())) {
                clientMapContains++;
            }
        }

        int serverMapContains = 0;
        for (Map.Entry<Integer, Integer> entry : entrySet1) {
            Integer value = findValue(entry.getKey(), testValues);
            if (value.equals(entry.getValue())) {
                serverMapContains++;
            }
        }

        assertMatchSuccessfulOperationQuota(0.75, testValues.length, serverMapContains, clientMapContains);
    }

    private Integer findValue(int key, AbstractMap.SimpleEntry<Integer, Integer>[] values) {
        for (AbstractMap.SimpleEntry<Integer, Integer> value : values) {
            if (value.getKey().equals(key)) {
                return value.getValue();
            }
        }
        return null;
    }
}
