package org.ostara.cli

import org.ostara.client.internal.Client
import org.ostara.client.internal.ClientConfig

object ClientFactory {

    fun buildClient(host: String): Client {
        val clientConfig = ClientConfig()
        clientConfig.bootstrapSocketAddress = listOf(host)

        val client = Client("create-client", clientConfig)
        client.start()

        return client
    }
}