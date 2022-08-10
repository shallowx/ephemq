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

import java.net.SocketAddress;
import java.util.Set;

import static org.shallow.processor.ProcessCommand.Server.QUORUM_VOTE;
import static org.shallow.util.NetworkUtil.newEventExecutorGroup;
import static org.shallow.util.NetworkUtil.newImmediatePromise;

public class SRaftQuorumVoter {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SRaftQuorumVoter.class);

    private final BrokerConfig config;
    private volatile ProcessRoles role;
    private final SRaftProcessController controller;
    private final EventExecutor quorumVoteExecutor;
    private final ShallowChannelPool pool;
    private final EventExecutor respondVoteVoteExecutor;
    private volatile int term = 0;

    public SRaftQuorumVoter(BrokerConfig config, SRaftProcessController controller) {
        this.config = config;
        this.role = ProcessRoles.Follower;

        this.controller = controller;

        this.pool = DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool();
        final EventExecutorGroup group = newEventExecutorGroup(2, "sraft-vote");
        this.quorumVoteExecutor = group.next();
        this.respondVoteVoteExecutor = group.next();
    }

    public void quorumVote(Promise<Boolean> promise) {
        if (quorumVoteExecutor.inEventLoop()) {
            doQuorumVote(promise);
        } else {
            quorumVoteExecutor.execute(() -> doQuorumVote(promise));
        }
    }

    private void doQuorumVote(Promise<Boolean> promise) {
        this.role = ProcessRoles.Candidate;

        ++this.term;

        final Set<SocketAddress> addresses = controller.toSocketAddress();
        final int half = (int)StrictMath.floor((addresses.size() >>> 1) + 1);

        final Promise<VoteResponse> sendRequestPromise = newImmediatePromise();
        promise.addListener(f -> {
            if (f.isSuccess()) {
                final VoteResponse response = (VoteResponse) f.get();
                int votes = 0;
                if (response.getAck() && ++votes >= half) {
                    role = ProcessRoles.LEADER;
                    promise.trySuccess(true);
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
        role = ProcessRoles.Follower;
        final VoteResponse response = VoteResponse.newBuilder().setAck(true).build();
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
