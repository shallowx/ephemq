package org.shallow.metadata.sraft;

import com.google.protobuf.MessageLite;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.internal.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.pool.ShallowChannelPool;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.shallow.processor.ProcessCommand.Server.POST_COMMIT;
import static org.shallow.processor.ProcessCommand.Server.PREPARE_COMMIT;
import static org.shallow.util.NetworkUtil.newImmediatePromise;

public abstract class AbstractSRaftLog<T> implements SRaftLog<T> {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(AbstractSRaftLog.class);

    protected final Set<SocketAddress> quorumVoterAddresses;
    protected final ShallowChannelPool pool;
    protected final BrokerConfig config;
    protected final SRaftProcessController controller;

    public AbstractSRaftLog(Set<SocketAddress> quorumVoterAddresses, ShallowChannelPool pool, BrokerConfig config, SRaftProcessController controller) {
        this.quorumVoterAddresses = quorumVoterAddresses;
        this.pool = pool;
        this.config = config;
        this.controller = controller;
    }

    @Override
    public void prepareCommit(T t, CommitType type, Promise<MessageLite> promise) {
        CommitRecord<T> commitRecord = this.doPrepareCommit(t, type);

        if (controller.isQuorumLeader()) {
            doPostCommit(commitRecord.getRecord(), commitRecord.getType());
            promise.trySuccess(commitRecord.getResponse());
            return;
        }

        Promise<Void> prepareCommitPromise = newImmediatePromise();
        prepareCommitPromise.addListener((GenericFutureListener<Future<Void>>) f -> {
            if (f.isSuccess()) {
                promise.trySuccess(commitRecord.getResponse());
            }
        });

        if (null == quorumVoterAddresses || quorumVoterAddresses.isEmpty()) {
            throw new IllegalArgumentException("The quorum voters<shallow.controller.quorum.voters> value cannot be empty");
        }
        notifyPrepareCommit(commitRecord, type, prepareCommitPromise);
    }

    private void notifyPrepareCommit(CommitRecord<T> commitRecord, CommitType type, Promise<Void> promise) {
        int half = (int)StrictMath.floor((quorumVoterAddresses.size() >>> 1) + 1);

        AtomicInteger votes = new AtomicInteger(1);
        Promise<MessageLite> preCommitPromise = newImmediatePromise();
        preCommitPromise.addListener((GenericFutureListener<Future<MessageLite>>) f -> {
            if (f.isSuccess()) {
                if (votes.incrementAndGet() >= half) {
                    postCommit(commitRecord.getRecord(), type);
                    notifyPostCommit(commitRecord, type, promise);
                }
            }
        });

        for (SocketAddress address : quorumVoterAddresses) {
            try {
                ClientChannel clientChannel = pool.acquireHealthyOrNew(address);
                clientChannel.invoker().invoke(PREPARE_COMMIT, config.getInvokeTimeMs(), preCommitPromise, commitRecord.getRequest(), commitRecord.getResponse().getClass());
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error(e.getMessage(), e);
                }
                // try again later
            }
        }
    }

    @Override
    public void postCommit(T t, CommitType type) {
        this.doPostCommit(t, type);
    }

    private void notifyPostCommit(CommitRecord<T> commitRecord, CommitType type, Promise<Void> promise) {
        AtomicInteger votes = new AtomicInteger(0);

        Promise<MessageLite> postCommitPromise = newImmediatePromise();
        postCommitPromise.addListener((GenericFutureListener<Future<MessageLite>>) f -> {
            if (f.isSuccess()) {
                promise.trySuccess(null);
            }
        });

        for (SocketAddress address : quorumVoterAddresses) {
            try {
                ClientChannel clientChannel = pool.acquireHealthyOrNew(address);
                clientChannel.invoker().invoke(POST_COMMIT, config.getInvokeTimeMs(), postCommitPromise, commitRecord.getRequest(), commitRecord.getResponse().getClass());
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error(e.getMessage(), e);
                }
                // try again later
            }
        }
    }

    protected abstract CommitRecord<T> doPrepareCommit(T t, CommitType type);

    protected abstract void doPostCommit(T t, CommitType type);
}
