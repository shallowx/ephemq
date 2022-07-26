package org.shallow.cluster;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.shallow.internal.AbstractMetadataProvider;
import org.shallow.internal.MappedFileConstants;
import org.shallow.internal.MetadataAPI;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.util.List;

import static org.shallow.JsonUtil.object2Json;
import static org.shallow.internal.MappedFileConstants.CLUSTERS;

public class ClusterMetadataProvider extends AbstractMetadataProvider<ClusterMetadata>{
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterMetadataProvider.class);

    private final EventExecutor write2FileExecutor;
    private final MetadataAPI api;

    public ClusterMetadataProvider(MetadataAPI api, EventExecutor executor, long expired) {
        super(api, expired);
        this.api = api;
        this.write2FileExecutor = executor;
    }

    @Override
    public ClusterMetadata acquire(String key) {
        return get(key);
    }

    @Override
    public Future<Boolean> append(ClusterMetadata clusterMetadata) {
        Promise<Boolean> promise = newPromise();
        try {
            final String path = api.assemblesPath(CLUSTERS);
            if (write2FileExecutor.inEventLoop()) {
                doAppend(path , clusterMetadata, promise);
            } else {
                write2FileExecutor.execute(() -> doAppend(path, clusterMetadata, promise));
            }
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
        return promise;
    }

    @Override
    public Future<Boolean> delete(ClusterMetadata clusterMetadata) {
        Promise<Boolean> promise = newPromise();
        try {
            final String path = api.assemblesPath(CLUSTERS);
            if (write2FileExecutor.inEventLoop()) {
                doDelete(path, clusterMetadata, promise);
            } else {
                write2FileExecutor.execute(() -> doDelete(path, clusterMetadata, promise));
            }
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
        return promise;
    }

    private void doAppend(String path, ClusterMetadata clusterMetadata, Promise<Boolean> promise) {
        put(clusterMetadata.id(), clusterMetadata);

        List<ClusterMetadata> clusters = getWhole();
        doModify(path, clusters, promise, MappedFileConstants.Type.APPEND);
    }

    private void doDelete(String path, ClusterMetadata clusterMetadata, Promise<Boolean> promise) {
        invalidate(clusterMetadata.id());

        List<ClusterMetadata> clusters = getWhole();
        clusters.removeIf(f -> f.id().equals(clusterMetadata.id()));

        doModify(path, clusters,  promise, MappedFileConstants.Type.DELETE);
    }

    @Override
    protected ClusterMetadata load(String key) {
        return null;
    }

    @Override
    protected String switchMetadata(List<ClusterMetadata> clusters) {
        return object2Json(clusters);
    }
}
