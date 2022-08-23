package org.shallow;

import org.junit.Test;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.NodeRecord;
import org.shallow.pool.DefaultFixedChannelPoolFactory;

import java.util.List;
import java.util.Set;

public class ClusterTest {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterTest.class);

    @Test
    public void testQuery() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9100"));
        Client client = new Client("Client", clientConfig);
        client.start();

        Set<NodeRecord> nodeRecords = client.getMetadataManager().queryNodeRecord(DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool().acquireWithRandomly());

        logger.info("result:{}", nodeRecords);
    }
}
