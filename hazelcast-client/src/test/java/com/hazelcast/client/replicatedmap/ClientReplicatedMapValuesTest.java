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
import java.util.concurrent.TimeoutException;

public class ClientReplicatedMapValuesTest extends AbstractClientReplicatedMapTest<Integer, Integer> {

    @Test
    public void testValuesObjectDelay0() throws TimeoutException {
        testValues(InMemoryFormat.OBJECT, 0);
    }

    @Test
    public void testValuesObjectDelayDefault() throws TimeoutException {
        testValues(InMemoryFormat.OBJECT, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
    }

    @Test
    public void testValuesBinaryDelay0() throws TimeoutException {
        testValues(InMemoryFormat.BINARY, 0);
    }

    @Test
    public void testValuesBinaryDefault() throws TimeoutException {
        testValues(InMemoryFormat.BINARY, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
    }

    private void testValues(InMemoryFormat inMemoryFormat, long replicationDelay) throws TimeoutException {
        setup(buildConfig(inMemoryFormat, replicationDelay));

        final AbstractMap.SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        final List<Integer> valuesTestValues = new ArrayList<Integer>(testValues.length);
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap<Integer, Integer> map = i < half ? serverMap : clientMap;
                    final AbstractMap.SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                    valuesTestValues.add(entry.getValue());
                }
            }
        }, 2, EntryEventType.ADDED, 100, 0.75, serverMap, clientMap);

        List<Integer> values1 = new ArrayList<Integer>(serverMap.values());
        List<Integer> values2 = new ArrayList<Integer>(clientMap.values());

        int serverMapContains = 0;
        int clientMapContains = 0;
        for (Integer value : valuesTestValues) {
            if (values2.contains(value)) {
                clientMapContains++;
            }
            if (values1.contains(value)) {
                serverMapContains++;
            }
        }

        assertMatchSuccessfulOperationQuota(0.75, testValues.length, serverMapContains, clientMapContains);
    }
}
