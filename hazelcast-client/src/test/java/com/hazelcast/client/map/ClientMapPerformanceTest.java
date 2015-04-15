package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.TruePredicate;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class ClientMapPerformanceTest {

    private static final int[] MAP_SIZES = {100, 500, 1000, 5000, 10000, 20000, 30000, 40000, 50000, 75000, 100000};

    private IMap<Integer, Integer> serverMap;
    private IMap<Integer, Integer> clientMap;

    private int mapSize;

    @Test
    public void testMapPerformance() {
        internalSetUpClient(271, 1, mapSize, Integer.MAX_VALUE);

        System.out.println("########################################################");
        System.out.println("IMap<Integer, Integer>");
        for (int mapSize : MAP_SIZES) {
            System.out.println("########################################################");
            this.mapSize = mapSize;

            fillToUpperLimit(serverMap);
            System.out.println("IMap test with mapSize " + serverMap.size());

            System.out.println("--------------------------------------------------------");
            internalRunWithoutException(serverMap);
            System.out.println("--------------------------------------------------------");
            internalRunWithoutException(clientMap);
        }
        System.out.println("########################################################");

        shutdown(serverMap);
    }

    private void internalSetUpClient(int partitionCount, int clusterSize, int mapSize, int preCheckTrigger) {
        Config config = createConfig();
        serverMap = getMapWithNodeCount(clusterSize, config);

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        clientMap = client.getMap(serverMap.getName());
    }

    private Config createConfig() {
        //Config config = new Config();
        //config.setProperty(GroupProperties.PROP_PARTITION_COUNT, String.valueOf(partitionCount));
        //return config;
        return new Config();
    }

    private <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount, Config config) {
        String mapName = UUID.randomUUID().toString();

        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(mapName);
        mapConfig.setAsyncBackupCount(0);
        mapConfig.setBackupCount(0);
        config.addMapConfig(mapConfig);

        while (nodeCount > 1) {
            Hazelcast.newHazelcastInstance(config);
            nodeCount--;
        }

        HazelcastInstance node = Hazelcast.newHazelcastInstance(config);
        return node.getMap(mapName);
    }

    private void fillToUpperLimit(IMap<Integer, Integer> fillMap) {
        for (int index = fillMap.size(); index < mapSize; index++) {
            fillMap.put(index, index);
        }
        assertEquals("Expected map size of server map to match mapSize", mapSize, fillMap.size());
    }

    private void internalRunWithoutException(IMap<Integer, Integer> queryMap) {
        long started = System.nanoTime();
        assertEquals("IMap.values()", mapSize, queryMap.values().size());
        System.out.println("IMap.values() took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) + " ms");

        started = System.nanoTime();
        assertEquals("IMap.values(predicate)", mapSize, queryMap.values(TruePredicate.INSTANCE).size());
        System.out.println("IMap.values(predicate) took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) + " ms");

        started = System.nanoTime();
        assertEquals("IMap.keySet()", mapSize, queryMap.keySet().size());
        System.out.println("IMap.keySet() took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) + " ms");

        started = System.nanoTime();
        assertEquals("IMap.keySet(predicate)", mapSize, queryMap.keySet(TruePredicate.INSTANCE).size());
        System.out.println("IMap.keySet(predicate) took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) + " ms");

        started = System.nanoTime();
        assertEquals("IMap.entrySet()", mapSize, queryMap.entrySet().size());
        System.out.println("IMap.entrySet() took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) + " ms");

        started = System.nanoTime();
        assertEquals("IMap.entrySet(predicate)", mapSize, queryMap.entrySet(TruePredicate.INSTANCE).size());
        System.out.println("IMap.entrySet(predicate) took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) + " ms");
    }

    private void shutdown(IMap map) {
        long started = System.nanoTime();
        map.destroy();
        System.out.println("IMap.destroy() took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
                + " ms with " + mapSize + " elements");

        Hazelcast.shutdownAll();
    }
}
