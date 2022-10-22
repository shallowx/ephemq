package org.shallow.client.pool;

import io.netty.util.concurrent.Future;
import org.shallow.client.internal.ClientChannel;

public interface ShallowChannelHealthChecker {

    ShallowChannelHealthChecker ACTIVE = f -> f != null && (f.isSuccess() && f.getNow().isActive());

    boolean isHealthy(Future<ClientChannel> future);
}
