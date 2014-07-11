package com.hazelcast.client.txn;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.transaction.TransactionContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class AbstractTxnTest extends HazelcastTestSupport {

    protected static final Config config = new Config();

    protected static HazelcastInstance server;
    protected static HazelcastInstance client;

    protected String randomName;

    protected TransactionContext context;

    @BeforeClass
    public static void beforeClass() {
        server = Hazelcast.newHazelcastInstance(config);
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    public void setup() {
        randomName = randomString();

        context = client.newTransactionContext();
    }
}
