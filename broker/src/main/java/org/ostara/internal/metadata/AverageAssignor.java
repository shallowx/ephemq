package org.ostara.internal.metadata;

import java.util.Set;
import org.ostara.common.metadata.Partition;
import org.ostara.internal.ResourceContext;
import org.ostara.internal.config.ServerConfig;

public class AverageAssignor extends LeaderAssignorAdapter {

    public AverageAssignor(ServerConfig config, ResourceContext context) {
        super(config, context);
    }

    @Override
    protected Set<Partition> assign(String topic, int partitionLimit, int replicateLimit) throws Exception {

        return null;
    }
}
