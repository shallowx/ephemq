package org.ostara.cli.command

import io.netty.util.concurrent.Promise
import org.ostara.cli.Alias
import org.ostara.cli.ClientFactory
import org.ostara.client.internal.Client
import org.ostara.remote.proto.server.DelTopicResponse
import picocli.CommandLine


@CommandLine.Command(
    name = "deleteTopic",
    sortOptions = false,
    version = ["1.0.0"],
    mixinStandardHelpOptions = true,
    description = ["delete topic"]
)
class DeleteTopicCommand : Runnable, Alias {

    @CommandLine.Option(
        names = ["-b", "--host"],
        description = ["Socket address, e.g. 127.0.0.1:9127"],
        required = true
    )
    var host: String = "127.0.0.1:9127"

    @CommandLine.Option(
        names = ["-t", "--topic"],
        description = ["Topic name"],
        required = true
    )
    var topic: String = ""

    override fun run() {
        if (host.isBlank()) {
            throw IllegalArgumentException("host cannot be blank")
        }

        if (topic.isBlank()) {
            throw IllegalArgumentException("topic cannot be blank")
        }

        val response = delete(host, topic)
        println(if (response?.ack == 1) "delete topic successfully, topic=$topic" else "failed to delete topic, topic=$topic, host=$host")

    }
}

fun delete(host: String, topic: String): DelTopicResponse? {
    var client: Client? = null
    try {
        client = ClientFactory.buildOstaraClient(host)
        val metadataWriter = client.metadataSupport
        val promise: Promise<DelTopicResponse> = metadataWriter.delTopic(topic)

        val response: DelTopicResponse = promise.get()
        return response
    } catch (t: Throwable) {
        println("failed to delete topic, topic=$topic host=$host error=${t.localizedMessage}")
    }
    client?.shutdownGracefully()

    return null
}