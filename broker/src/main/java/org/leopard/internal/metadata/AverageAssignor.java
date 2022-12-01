package org.leopard.internal.metadata;

import java.util.Set;
import org.leopard.common.metadata.Partition;
import org.leopard.internal.ResourceContext;
import org.leopard.internal.config.ServerConfig;

public class AverageAssignor extends LeaderAssignorAdapter {

    public AverageAssignor(ServerConfig config, ResourceContext context) {
        super(config, context);
    }

    @Override
    protected Set<Partition> assign(String topic, int partitionLimit, int replicateLimit) throws Exception {

        return null;
    }
}
