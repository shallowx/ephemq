package org.shallow.pool;

import io.netty.util.concurrent.Future;
import org.shallow.invoke.ClientChannel;

public interface ShallowChannelHealthChecker {

    ShallowChannelHealthChecker ACTIVE = f -> f != null && (!f.isDone() || (f.isSuccess() && f.getNow().isActive()));

    boolean isHealthy(Future<ClientChannel> future);
}
