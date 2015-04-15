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

import com.hazelcast.client.impl.client.AllPartitionsClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapValueCollection;
import com.hazelcast.map.impl.operation.MapValuesOperationFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;
import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MapValuesRequest extends AllPartitionsClientRequest implements Portable, SecureRequest, RetryableRequest {

    private String name;

    public MapValuesRequest() {
    }

    public MapValuesRequest(String name) {
        this.name = name;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new MapValuesOperationFactory(name);
    }

    @Override
    protected Object reduce(Map<Integer, Object> results) {
        long started = System.nanoTime();
        List<Data> values = new ArrayList<Data>();
        MapService mapService = getService();
        for (Object result : results.values()) {
            values.addAll(((MapValueCollection) mapService.getMapServiceContext().toObject(result)).getValues());
        }
        MapValueCollection finalResult = new MapValueCollection(values);
        System.out.println("      MapValuesRequest.reduce() took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) + " ms");
        return finalResult;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapPortableHook.VALUES;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "values";
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }
}
