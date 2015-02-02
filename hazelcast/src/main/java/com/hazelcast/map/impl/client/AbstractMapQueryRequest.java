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

import com.hazelcast.client.impl.client.InvocationClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.map.impl.MapKeySet;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapValueCollection;
import com.hazelcast.map.impl.QueryResult;
import com.hazelcast.map.impl.operation.QueryOperation;
import com.hazelcast.map.impl.operation.QueryPartitionOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryResultEntry;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.QueryResultSet;

import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

abstract class AbstractMapQueryRequest extends InvocationClientRequest implements Portable, SecureRequest, RetryableRequest {

    private IterationType iterationType;
    private String name;

    public AbstractMapQueryRequest() {
    }

    public AbstractMapQueryRequest(String name, IterationType iterationType) {
        this.name = name;
        this.iterationType = iterationType;
    }

    @Override
    protected final void invoke() {
        boolean isUserQuery = (getClassId() == MapPortableHook.QUERY);
        QueryResultSet result = null;
        Set dataSet = null;
        if (isUserQuery) {
            result = new QueryResultSet(null, iterationType, true);
        } else {
            dataSet = new HashSet();
        }

        try {
            Predicate predicate = getPredicate();

            Collection<MemberImpl> members = getClientEngine().getClusterService().getMemberList();
            List<Future> futures = new ArrayList<Future>();
            createInvocations(members, futures, predicate);

            int partitionCount = getClientEngine().getPartitionService().getPartitionCount();
            Set<Integer> finishedPartitions = new HashSet<Integer>(partitionCount);
            collectResults(isUserQuery, result, dataSet, futures, finishedPartitions);

            if (hasMissingPartitions(finishedPartitions, partitionCount)) {
                List<Integer> missingList = findMissingPartitions(finishedPartitions, partitionCount);
                List<Future> missingFutures = new ArrayList<Future>(missingList.size());
                createInvocationsForMissingPartitions(missingList, missingFutures, predicate);
                collectResultsFromMissingPartitions(isUserQuery, result, dataSet, missingFutures);
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }

        getEndpoint().sendResponse(getFinalResult(isUserQuery, result, dataSet), getCallId());
    }

    private void createInvocations(Collection<MemberImpl> members, List<Future> futures, Predicate predicate) {
        for (MemberImpl member : members) {
            futures.add(createInvocationBuilder(SERVICE_NAME, new QueryOperation(name, predicate), member.getAddress()).invoke());
        }
    }

    @SuppressWarnings("unchecked")
    private void collectResults(boolean isUserQuery, QueryResultSet result, Set dataSet, List<Future> futures,
                                Set<Integer> finishedPartitions) throws InterruptedException, ExecutionException {
        for (Future<QueryResult> future : futures) {
            QueryResult queryResult =  future.get();
            if (queryResult == null) {
                continue;
            }

            Collection<Integer> partitionIds = queryResult.getPartitionIds();
            if (partitionIds == null) {
                continue;
            }

            finishedPartitions.addAll(partitionIds);
            if (isUserQuery) {
                result.addAll(queryResult.getResult());
            } else {
                addQueryResultEntryToDataHashSet(dataSet, queryResult.getResult());
            }
        }
    }

    private boolean hasMissingPartitions(Set<Integer> finishedPartitions, int partitionCount) {
        return (finishedPartitions.size() != partitionCount);
    }

    private List<Integer> findMissingPartitions(Set<Integer> finishedPartitions, int partitionCount) {
        List<Integer> missingList = new ArrayList<Integer>();
        for (int i = 0; i < partitionCount; i++) {
            if (!finishedPartitions.contains(i)) {
                missingList.add(i);
            }
        }
        return missingList;
    }

    private void createInvocationsForMissingPartitions(List<Integer> missingPartitionsList, List<Future> futures,
                                                       Predicate predicate) {
        for (Integer partitionId : missingPartitionsList) {
            QueryPartitionOperation queryPartitionOperation = new QueryPartitionOperation(name, predicate);
            queryPartitionOperation.setPartitionId(partitionId);
            try {
                Future future = createInvocationBuilder(SERVICE_NAME, queryPartitionOperation, partitionId).invoke();
                futures.add(future);
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void collectResultsFromMissingPartitions(boolean isUserQuery, QueryResultSet result, Set dataSet,
                                                     List<Future> futures) throws InterruptedException, ExecutionException {
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            if (isUserQuery) {
                result.addAll(queryResult.getResult());
            } else {
                addQueryResultEntryToDataHashSet(dataSet, queryResult.getResult());
            }
        }
    }

    private void addQueryResultEntryToDataHashSet(Set<Object> dataSet, Collection<QueryResultEntry> entries) {
        for (QueryResultEntry entry : entries) {
            if (iterationType == IterationType.KEY) {
                dataSet.add(entry.getKeyData());
            } else if (iterationType == IterationType.VALUE) {
                dataSet.add(entry.getValueData());
            } else if (iterationType == IterationType.ENTRY) {
                dataSet.add(new SimpleEntry(entry.getKeyData(), entry.getValueData()));
            } else {
                throw new IllegalArgumentException("IterationType[" + iterationType + "] is unknown!!!");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Object getFinalResult(boolean isUserQuery, QueryResultSet result, Set dataSet) {
        if (isUserQuery) {
            return result;
        }
        if (iterationType == IterationType.VALUE) {
            return new MapValueCollection(dataSet);
        }
        if (iterationType == IterationType.KEY) {
            return new MapKeySet(dataSet);
        }
        if (iterationType == IterationType.ENTRY) {
            return new MapEntrySet(dataSet);
        }
        return null;
    }

    @Override
    public final String getMethodName() {
        if (iterationType == IterationType.KEY) {
            return "keySet";
        }
        if (iterationType == IterationType.VALUE) {
            return "values";
        }
        if (iterationType == IterationType.ENTRY) {
            return "entrySet";
        }
        throw new IllegalArgumentException("IterationType " + iterationType + " is unknown!");
    }

    @Override
    public final int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    @Override
    public final String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public final Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_READ);
    }

    @Override
    public final String getDistributedObjectName() {
        return name;
    }

    @Override
    public final void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("t", iterationType.toString());
        writePortableInner(writer);
    }

    @Override
    public final void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        iterationType = IterationType.valueOf(reader.readUTF("t"));
        readPortableInner(reader);
    }

    protected abstract Predicate getPredicate();

    protected abstract void writePortableInner(PortableWriter writer) throws IOException;

    protected abstract void readPortableInner(PortableReader reader) throws IOException;

    private static final class SimpleEntry implements Map.Entry<Data, Data> {
        private final Data key;
        private final Data value;

        private SimpleEntry(Data key, Data value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Data getKey() {
            return key;
        }

        @Override
        public Data getValue() {
            return value;
        }

        @Override
        public Data setValue(Data value) {
            throw new UnsupportedOperationException();
        }
    }
}
