package org.shallow.metadata.sraft;

import io.netty.util.concurrent.*;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.pool.DefaultFixedChannelPoolFactory;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.proto.elector.VoteRequest;
import org.shallow.proto.elector.VoteResponse;

import javax.annotation.concurrent.ThreadSafe;
import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static org.shallow.processor.ProcessCommand.Server.QUORUM_VOTE;
import static org.shallow.util.NetworkUtil.newEventExecutorGroup;
import static org.shallow.util.NetworkUtil.newImmediatePromise;

@ThreadSafe
public class SRaftQuorumVoter {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SRaftQuorumVoter.class);

    private static final AtomicIntegerFieldUpdater<SRaftQuorumVoter> updater = AtomicIntegerFieldUpdater.newUpdater(SRaftQuorumVoter.class, "term");

    private final BrokerConfig config;
    private volatile ProcessRoles role;
    private final SRaftProcessController controller;
    private final EventExecutor quorumVoteExecutor;
    private ShallowChannelPool pool;
    private final EventExecutor respondVoteVoteExecutor;
    private final EventExecutor quorumVoteTimeoutExecutor;
    private volatile int term = 0;

    public SRaftQuorumVoter(BrokerConfig config, SRaftProcessController controller) {
        this.config = config;
        this.role = ProcessRoles.Follower;

        this.controller = controller;


        final EventExecutorGroup group = newEventExecutorGroup(3, "sraft-vote");
        this.quorumVoteExecutor = group.next();
        this.respondVoteVoteExecutor = group.next();
        this.quorumVoteTimeoutExecutor = group.next();
    }

    public void start() throws Exception {
        this.pool = DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool();
    }

    public void quorumVote(Promise<Boolean> promise) {
        Runnable timeoutTask = () -> {
            int theTerm = term;
            if (role == ProcessRoles.Candidate) {
                role = ProcessRoles.Follower;
                updater.compareAndSet(this, theTerm, --term);
            }
        };
        quorumVoteTimeoutExecutor.schedule(timeoutTask, config.getControllerQuorumVoteTimeoutMs(), TimeUnit.MILLISECONDS);

        if (quorumVoteExecutor.inEventLoop()) {
            doQuorumVote(promise);
        } else {
            quorumVoteExecutor.execute(() -> doQuorumVote(promise));
        }
    }

    private void doQuorumVote(Promise<Boolean> promise) {
        int theTerm = term;
        if (role == ProcessRoles.Follower) {
            this.role = ProcessRoles.Candidate;
            updater.compareAndSet(this, theTerm, ++term);
        }

        Set<SocketAddress> addresses = controller.toSocketAddress(true);
        int half = (int)StrictMath.floor((addresses.size() >>> 1) + 1);

        AtomicInteger votes = new AtomicInteger(1);
        Promise<VoteResponse> sendRequestPromise = newImmediatePromise();
        sendRequestPromise.addListener(f -> {
            if (f.isSuccess()) {
                final VoteResponse response = (VoteResponse) f.get();
                if (response.getAck() && votes.incrementAndGet() >= half) {
                    if (role == ProcessRoles.Candidate) {
                        role = ProcessRoles.LEADER;
                        promise.trySuccess(true);
                    }
                }
            }
        });

        doSendVoteRequest(sendRequestPromise, addresses);
    }

    private void doSendVoteRequest(Promise<VoteResponse> promise, Set<SocketAddress> addresses) {
        final VoteRequest request = VoteRequest.newBuilder()
                .setTerm(term)
                .build();

        for (SocketAddress address : addresses) {
            try {
                final ClientChannel clientChannel = acquire(address);
                clientChannel.invoker().invoke(QUORUM_VOTE, config.getInvokeTimeMs(), promise, request, VoteResponse.class);
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Failed to send vote request with address<{}>, try again later", address);
                }
            }
        }
    }

    public ClientChannel acquire(SocketAddress address) {
        return pool.acquireHealthyOrNew(address);
    }

    public void respondVote(Promise<VoteResponse> respondVotePromise) {
        if (respondVoteVoteExecutor.inEventLoop()) {
            doRespondVote(respondVotePromise);
        } else {
            respondVoteVoteExecutor.execute(() -> doRespondVote(respondVotePromise));
        }
    }

    private void doRespondVote(Promise<VoteResponse> respondVotePromise) {
        int theTerm = term;
        if (role == ProcessRoles.Candidate) {
            updater.compareAndSet(this, theTerm, --term);
        }
        role = ProcessRoles.Follower;
        VoteResponse response = VoteResponse.newBuilder().setAck(true).build();
        respondVotePromise.trySuccess(response);
    }

    public ProcessRoles getSRaftRole() {
        return role;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }
}
