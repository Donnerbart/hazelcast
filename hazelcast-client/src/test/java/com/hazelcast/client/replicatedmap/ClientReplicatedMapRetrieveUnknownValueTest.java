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
import org.junit.Test;

import static org.junit.Assert.assertNull;

public class ClientReplicatedMapRetrieveUnknownValueTest extends AbstractClientReplicatedMapTest<String, String> {

    @Test
    public void testRetrieveUnknownValueObjectDelay0() {
        testRetrieveUnknownValue(InMemoryFormat.OBJECT, 0);
    }

    @Test
    public void testRetrieveUnknownValueObjectDelayDefault() {
        testRetrieveUnknownValue(InMemoryFormat.OBJECT, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
    }

    @Test
    public void testRetrieveUnknownValueBinaryDelay0() {
        testRetrieveUnknownValue(InMemoryFormat.BINARY, 0);
    }

    @Test
    public void testRetrieveUnknownValueBinaryDelayDefault() {
        testRetrieveUnknownValue(InMemoryFormat.BINARY, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS);
    }

    private void testRetrieveUnknownValue(InMemoryFormat inMemoryFormat, long replicationDelay) {
        setup(buildConfig(inMemoryFormat, replicationDelay));

        String value = clientMap.get("foo");
        assertNull(value);
    }
}
