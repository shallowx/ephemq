package org.shallow.nraft;

import com.google.protobuf.MessageLite;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.shallow.internal.MetadataConfig;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.pool.DefaultChannelPoolFactory;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.proto.elector.VoteRequest;
import org.shallow.proto.elector.VoteResponse;
import org.shallow.util.NetworkUtil;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.shallow.processor.ProcessCommand.NameServer.VOTE;

public class RaftVoteListener implements VoteListener{

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(RaftVoteListener.class);

    private byte suffrage = 1;
    private final MetadataConfig config;
    private final ShallowChannelPool pool;
    private final EventExecutor electEventExecutor;
    private int votes;
    private LeaderElector elector;

    public RaftVoteListener(MetadataConfig metadataConfig, EventExecutor executor, LeaderElector elector) {
        this.config = metadataConfig;
        this.pool = DefaultChannelPoolFactory.INSTANCE.acquireChannelPool();
        this.electEventExecutor = executor;
        this.elector = elector;
    }

    @Override
    public void vote(Promise<Boolean> promise) {
        if (electEventExecutor.inEventLoop()) {
            doVote(promise);
        } else {
            electEventExecutor.execute(() -> doVote(promise));
        }
    }

    private void doVote(Promise<Boolean> promise) {
        final Set<SocketAddress> socketAddresses = toSocketAddress();
        if (socketAddresses.isEmpty()) {
            promise.trySuccess(true);
            return;
        }

        if (suffrage == 1) {
            --suffrage;
            ++votes;
        }

        final int half = (int) StrictMath.floor(((socketAddresses.size() - 1) >>> 1) + 1);
        final VoteRequest request = VoteRequest
                .newBuilder()
                .setTerm(elector.getTerm())
                .build();

        final Promise<VoteResponse> votePromise = electEventExecutor.newPromise();
        votePromise.addListener((GenericFutureListener<Future<VoteResponse>>) f -> {
            if (f.isSuccess()) {
                final VoteResponse response = f.get();
                if (response.getAck()) {
                    ++votes;
                    if (votes == half) {
                        promise.trySuccess(true);
                    }
                }
            }
        });

        for (SocketAddress address : socketAddresses) {
            doInvoke(address, votePromise, request);
        }
    }

    private void doInvoke(SocketAddress address, Promise<VoteResponse> promise, VoteRequest request) {
        try {
            final ClientChannel channel = acquireChannel(address);
            channel.invoker().invoke(VOTE, 1000, promise, request, VoteResponse.class);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("[doInvoke] - invoke leader vote with address<{}> failed, will try again later {} ms", address.toString(), 1000);
            }
            electEventExecutor.schedule(() -> doInvoke(address, promise, request), 1000, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void answer(int term, Promise<? super MessageLite> promise) {
        if (electEventExecutor.inEventLoop()) {
            doAnswer(term, promise);
        } else {
            electEventExecutor.execute(() -> doAnswer(term, promise));
        }
    }

    private void doAnswer(int term, Promise<? super MessageLite> promise) {
        final VoteResponse response = VoteResponse
                .newBuilder()
                .setAck(suffrage == 1)
                .build();

        promise.trySuccess(response);

        if(suffrage == 1) {
            --suffrage;
        }
    }

    private Set<SocketAddress> toSocketAddress() {
        return Arrays.stream(config.getClusterUrl().split(","))
                .filter(f -> !f.equals(config.getExposedHost() + ":" + config.getExposedPort()))
                .map(NetworkUtil::switchSocketAddress)
                .collect(Collectors.toSet());
    }

    private ClientChannel acquireChannel(SocketAddress address) {
        return pool.acquireHealthyOrNew(address);
    }

    @Override
    public void revert() {
        suffrage = 1;
        votes = 0;
    }
}
