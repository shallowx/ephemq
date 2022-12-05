package org.ostara.cli.command

import io.netty.util.concurrent.Promise
import org.ostara.cli.Alias
import org.ostara.cli.ClientFactory
import org.ostara.client.internal.Client
import org.ostara.remote.proto.server.CreateTopicResponse
import picocli.CommandLine

@CommandLine.Command(
    name = "createTopic",
    sortOptions = false,
    version = ["1.0.0"],
    mixinStandardHelpOptions = true,
    description = ["create topic"]
)
class CreateTopicCommand : Runnable, Alias {

    @CommandLine.Option(
        names = ["-b", "--host"],
        description = ["Socket address, e.g. 127.0.0.1:9127"],
        required = true
    )
    var host: String = "127.0.0.1:9127"

    @CommandLine.Option(names = ["-t", "--topic"], description = ["Topic name"], required = true)
    var topic: String = ""

    @CommandLine.Option(names = ["-p", "--partitions"], description = ["Partition limit"], required = true)
    var partitions: Int = 1

    @CommandLine.Option(names = ["-r", "--replicates"], description = ["Replicate limit"], required = true)
    var replicates: Int = 1

    override fun run() {
        if (host.isBlank()) {
            throw IllegalArgumentException("host cannot be blank")
        }

        if (topic.isBlank()) {
            throw IllegalArgumentException("topic cannot be blank")
        }

        if (partitions <= 0) {
            throw IllegalArgumentException("partition limit must be > 0")
        }

        if (replicates <= 0) {
            throw IllegalArgumentException("replicate limit must be > 0")
        }

        var client: Client? = null
        try {
            client = ClientFactory.buildOstaraClient(host)
            val metadataWriter = client.metadataWriter
            val promise: Promise<CreateTopicResponse> = metadataWriter.createTopic(topic, partitions, replicates)

            val response: CreateTopicResponse = promise.get()
            println(if (response.ack == 1) "create topic successfully, topic=$topic" else "failed to create topic, topic=$topic, host=$host")
        } catch (t: Throwable) {
            println("failed to create topic, topic=$topic host=$host error=${t.localizedMessage}")
        }

        client?.shutdownGracefully()
    }
}
