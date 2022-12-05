package org.ostara.cli

import org.ostara.client.internal.Client
import org.ostara.client.internal.ClientChannel
import org.ostara.remote.util.NetworkUtils

object ClientChannelFactory {

    fun buildClientChannel(client: Client, addr: String?): ClientChannel? {
        val chanelPool = client.chanelPool

        if (addr.isNullOrBlank()) {
            return chanelPool.acquireWithRandomly()
        }

        return chanelPool.acquireHealthyOrNew(NetworkUtils.switchSocketAddress(addr))
    }
}