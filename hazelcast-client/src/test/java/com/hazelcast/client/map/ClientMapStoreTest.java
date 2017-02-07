package com.hazelcast.client.map;

import com.hazelcast.client.map.helpers.AMapStore;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.spi.properties.GroupProperty.MAP_WRITE_BEHIND_QUEUE_CAPACITY;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
@Ignore(value = "https://github.com/hazelcast/hazelcast/issues/7011")
public class ClientMapStoreTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "clientMapStoreLoad";

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private Config nodeConfig;

    @Before
    public void setup() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setImplementation(new SimpleMapStore())
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);

        MapConfig mapConfig = new MapConfig()
                .setName(MAP_NAME)
                .setMapStoreConfig(mapStoreConfig);

        nodeConfig = new Config()
                .addMapConfig(mapConfig);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testOneClient_KickOffMapStoreLoad() {
        hazelcastFactory.newHazelcastInstance(nodeConfig);

        ClientThread client = new ClientThread();
        client.start();

        assertJoinable(client);
        assertSizeEventually(SimpleMapStore.MAX_KEYS, client.map);
    }

    @Test
    public void testTwoClient_KickOffMapStoreLoad() {
        hazelcastFactory.newHazelcastInstance(nodeConfig);
        ClientThread[] clientThreads = new ClientThread[2];
        for (int i = 0; i < clientThreads.length; i++) {
            ClientThread client = new ClientThread();
            client.start();
            clientThreads[i] = client;
        }

        assertJoinable(clientThreads);

        for (ClientThread c : clientThreads) {
            assertSizeEventually(SimpleMapStore.MAX_KEYS, c.map);
        }
    }

    @Test
    public void testOneClientKickOffMapStoreLoad_ThenNodeJoins() {
        hazelcastFactory.newHazelcastInstance(nodeConfig);

        ClientThread client = new ClientThread();
        client.start();

        hazelcastFactory.newHazelcastInstance(nodeConfig);

        assertJoinable(client);

        assertSizeEventually(SimpleMapStore.MAX_KEYS, client.map);
    }

    /**
     * https://github.com/hazelcast/hazelcast/issues/2112
     */
    @Test
    public void testSubsequentProxyCreation_shouldNotThrowMapIsNotReadyException() {
        hazelcastFactory.newHazelcastInstance(nodeConfig);
        HazelcastInstance client1 = hazelcastFactory.newHazelcastClient();
        IMap<String, String> map = client1.getMap(MAP_NAME);
        assertSizeEventually(SimpleMapStore.MAX_KEYS, map);

        hazelcastFactory.newHazelcastInstance(nodeConfig);
        HazelcastInstance client2 = hazelcastFactory.newHazelcastClient();
        map = client2.getMap(MAP_NAME);
        assertSizeEventually(SimpleMapStore.MAX_KEYS, map);
    }

    @Test
    public void mapSize_After_MapStore_OperationQueue_OverFlow() throws Exception {
        int maxCapacity = 1000;

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setImplementation(new MapStoreBackup())
                .setWriteDelaySeconds(Integer.MAX_VALUE)
                .setWriteCoalescing(false);

        MapConfig mapConfig = new MapConfig()
                .setName(MAP_NAME)
                .setMapStoreConfig(mapStoreConfig);

        Config config = new Config()
                .setProperty(MAP_WRITE_BEHIND_QUEUE_CAPACITY.getName(), valueOf(maxCapacity))
                .addMapConfig(mapConfig);

        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        IMap<Integer, Integer> map = client.getMap(MAP_NAME);

        int overflow = 100;
        List<Future> futures = new ArrayList<Future>(maxCapacity + overflow);
        for (int i = 0; i < maxCapacity + overflow; i++) {
            Future future = map.putAsync(i, i);
            futures.add(future);
        }

        int success = 0;
        for (Future future : futures) {
            try {
                future.get();
                success++;
            } catch (ExecutionException e) {
                assertInstanceOf(ReachedMaxSizeException.class, e.getCause());
            }
        }

        assertEquals(success, maxCapacity);
        assertEquals(map.size(), maxCapacity);
    }

    @Test(expected = ReachedMaxSizeException.class)
    public void mapStore_OperationQueue_AtMaxCapacity() {
        int maxCapacity = 1000;

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setImplementation(new MapStoreBackup())
                .setWriteDelaySeconds(60)
                .setWriteCoalescing(false);

        MapConfig mapConfig = new MapConfig()
                .setName(MAP_NAME)
                .setMapStoreConfig(mapStoreConfig);

        Config config = new Config()
                .setProperty(MAP_WRITE_BEHIND_QUEUE_CAPACITY.getName(), valueOf(maxCapacity))
                .addMapConfig(mapConfig);

        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        IMap<Integer, Integer> map = client.getMap(MAP_NAME);
        for (int i = 0; i < maxCapacity; i++) {
            map.put(i, i);
        }
        assertEquals(maxCapacity, map.size());

        map.put(maxCapacity, maxCapacity);
    }

    @Test
    public void destroyMap_configuredWithMapStore() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setImplementation(new MapStoreBackup())
                .setWriteDelaySeconds(4);

        MapConfig mapConfig = new MapConfig()
                .setName(MAP_NAME)
                .setMapStoreConfig(mapStoreConfig);

        Config config = new Config()
                .addMapConfig(mapConfig);

        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        IMap<Integer, Integer> map = client.getMap(MAP_NAME);
        for (int i = 0; i < 1; i++) {
            map.putAsync(i, i);
        }

        map.destroy();
    }

    /**
     * https://github.com/hazelcast/hazelcast/issues/3023
     */
    @Test
    public void testCorrectMapStoreConfigurationIsUsed_whenWildcardsAreUsedInXML() throws Exception {
        String mapNameWithStore = "MapStore*";
        String mapNameWithStoreAndSize = "MapStoreMaxSize*";

        String xml = "<hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config\n" +
                "                             http://www.hazelcast.com/schema/config/hazelcast-config-3.8.xsd\"\n" +
                "                             xmlns=\"http://www.hazelcast.com/schema/config\"\n" +
                "                             xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n" +
                "\n" +
                "    <map name=\"" + mapNameWithStore + "\">\n" +
                "        <map-store enabled=\"true\">\n" +
                "            <class-name>com.will.cause.problem.if.used</class-name>\n" +
                "            <write-delay-seconds>5</write-delay-seconds>\n" +
                "        </map-store>\n" +
                "    </map>\n" +
                "\n" +
                "    <map name=\"" + mapNameWithStoreAndSize + "\">\n" +
                "        <in-memory-format>BINARY</in-memory-format>\n" +
                "        <backup-count>1</backup-count>\n" +
                "        <async-backup-count>0</async-backup-count>\n" +
                "        <max-idle-seconds>0</max-idle-seconds>\n" +
                "        <eviction-policy>LRU</eviction-policy>\n" +
                "        <max-size policy=\"PER_NODE\">10</max-size>\n" +
                "        <eviction-percentage>50</eviction-percentage>\n" +
                "\n" +
                "        <merge-policy>com.hazelcast.map.merge.PassThroughMergePolicy</merge-policy>\n" +
                "\n" +
                "        <map-store enabled=\"true\">\n" +
                "            <class-name>com.hazelcast.client.map.helpers.AMapStore</class-name>\n" +
                "            <write-delay-seconds>5</write-delay-seconds>\n" +
                "        </map-store>\n" +
                "    </map>\n" +
                "\n" +
                "</hazelcast>";

        Config config = buildConfig(xml);
        HazelcastInstance hz = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        IMap<Integer, Integer> map = client.getMap(mapNameWithStoreAndSize + "1");
        map.put(1, 1);

        MapStoreConfig mapStoreConfig = hz.getConfig()
                .getMapConfig(mapNameWithStoreAndSize + "1")
                .getMapStoreConfig();
        final AMapStore store = (AMapStore) (mapStoreConfig.getImplementation());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, store.store.get(1));
            }
        });
    }

    private static Config buildConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }

    private class ClientThread extends Thread {

        IMap<String, String> map;

        @Override
        public void run() {
            HazelcastInstance client = hazelcastFactory.newHazelcastClient();
            map = client.getMap(MAP_NAME);
            map.size();
        }
    }

    private static class MapStoreBackup implements MapStore<Object, Object> {

        public final Map<Object, Object> store = new ConcurrentHashMap<Object, Object>();

        @Override
        public void store(Object key, Object value) {
            store.put(key, value);
        }

        @Override
        public void storeAll(Map<Object, Object> map) {
            for (Map.Entry<Object, Object> kvp : map.entrySet()) {
                store.put(kvp.getKey(), kvp.getValue());
            }
        }

        @Override
        public void delete(Object key) {
            store.remove(key);
        }

        @Override
        public void deleteAll(Collection<Object> keys) {
            for (Object key : keys) {
                store.remove(key);
            }
        }

        @Override
        public Object load(Object key) {
            return store.get(key);
        }

        @Override
        public Map<Object, Object> loadAll(Collection<Object> keys) {
            Map<Object, Object> result = new HashMap<Object, Object>();
            for (Object key : keys) {
                Object value = store.get(key);
                if (value != null) {
                    result.put(key, value);
                }
            }
            return result;
        }

        @Override
        public Set<Object> loadAllKeys() {
            return store.keySet();
        }
    }

    private static class SimpleMapStore implements MapStore<String, String>, MapLoader<String, String> {

        private static final int MAX_KEYS = 30;
        private static final int DELAY_MILLIS_PER_KEY = 500;

        @Override
        public String load(String key) {
            sleepMillis(DELAY_MILLIS_PER_KEY);
            return key + "value";
        }

        @Override
        public Map<String, String> loadAll(Collection<String> keys) {
            Map<String, String> map = new HashMap<String, String>();
            for (String key : keys) {
                map.put(key, load(key));
            }
            return map;
        }

        @Override
        public Set<String> loadAllKeys() {
            Set<String> keys = new HashSet<String>();
            for (int i = 0; i < MAX_KEYS; i++) {
                keys.add("key" + i);
            }
            return keys;
        }

        @Override
        public void delete(String key) {
            sleepMillis(DELAY_MILLIS_PER_KEY);
        }

        @Override
        public void deleteAll(Collection<String> keys) {
            for (String key : keys) {
                delete(key);
            }
        }

        @Override
        public void store(String key, String value) {
            sleepMillis(DELAY_MILLIS_PER_KEY);
        }

        @Override
        public void storeAll(Map<String, String> entries) {
            for (Map.Entry<String, String> e : entries.entrySet()) {
                store(e.getKey(), e.getValue());
            }
        }
    }
}
