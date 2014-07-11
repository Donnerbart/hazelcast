package com.hazelcast.client.replicatedmap;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;

import java.util.AbstractMap;
import java.util.Random;

import static org.junit.Assert.fail;

abstract class AbstractClientReplicatedMapTest<K, V> extends HazelcastTestSupport {

    static final int OPERATIONS = 100;

    HazelcastInstance server;
    HazelcastInstance client;

    ReplicatedMap<K, V> serverMap;
    ReplicatedMap<K, V> clientMap;

    void setup(Config config) {
        server = Hazelcast.newHazelcastInstance(config);
        client = HazelcastClient.newHazelcastClient();

        serverMap = server.getReplicatedMap("default");
        clientMap = client.getReplicatedMap("default");
    }

    @After
    public void teardown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    Config buildConfig(InMemoryFormat inMemoryFormat, long replicationDelay) {
        Config config = new Config();
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig("default");
        replicatedMapConfig.setReplicationDelayMillis(replicationDelay);
        replicatedMapConfig.setInMemoryFormat(inMemoryFormat);
        return config;
    }

    void assertMatchSuccessfulOperationQuota(double quota, int completeOps, int... values) {
        float[] quotas = new float[values.length];
        Object[] args = new Object[values.length + 1];
        args[0] = quota;

        for (int i = 0; i < values.length; i++) {
            quotas[i] = (float) values[i] / completeOps;
            args[i + 1] = quotas[i];
        }

        boolean success = true;
        for (int i = 0; i < values.length; i++) {
            if (quotas[i] < quota) {
                success = false;
                break;
            }
        }

        if (!success) {
            StringBuilder sb = new StringBuilder("Quote (%s) for updates not reached,");
            for (int i = 0; i < values.length; i++) {
                sb.append(" map").append(i + 1).append(": %s,");
            }
            sb.deleteCharAt(sb.length() - 1);
            fail(String.format(sb.toString(), args));
        }
    }

    @SuppressWarnings("unchecked")
    AbstractMap.SimpleEntry<Integer, Integer>[] buildTestValues() {
        Random random = new Random();
        AbstractMap.SimpleEntry<Integer, Integer>[] testValues = new AbstractMap.SimpleEntry[100];
        for (int i = 0; i < testValues.length; i++) {
            testValues[i] = new AbstractMap.SimpleEntry<Integer, Integer>(random.nextInt(), random.nextInt());
        }
        return testValues;
    }
}
