/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.client;

import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.util.IterationType;

import java.io.IOException;

public class MapEntrySetRequest extends AbstractMapQueryRequest {

    public MapEntrySetRequest() {
    }

    public MapEntrySetRequest(String name) {
        super(name, IterationType.ENTRY);
    }

    @Override
    public int getClassId() {
        return MapPortableHook.ENTRY_SET;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{TruePredicate.INSTANCE};
    }

    @Override
    protected Predicate getPredicate() {
        return TruePredicate.INSTANCE;
    }

    @Override
    protected void writePortableInner(PortableWriter writer) throws IOException {
    }

    @Override
    protected void readPortableInner(PortableReader reader) throws IOException {
    }
}
