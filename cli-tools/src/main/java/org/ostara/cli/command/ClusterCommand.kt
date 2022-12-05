package org.ostara.cli.command

import org.ostara.cli.Alias
import org.ostara.cli.ClientChannelFactory
import org.ostara.cli.ClientFactory
import org.ostara.client.internal.Client
import org.ostara.client.internal.ClientChannel
import picocli.CommandLine
import picocli.CommandLine.Command
import java.util.function.Predicate
import java.util.stream.Collectors

@Command(
    name = "clusterList",
    sortOptions = false,
    version = ["1.0.0"],
    mixinStandardHelpOptions = true,
    description = ["query cluster info"]
)
class ClusterCommand : Runnable, Alias {

    @CommandLine.Option(
        names = ["-b", "--host"],
        description = ["Socket address, e.g. 127.0.0.1:9127"],
        required = true
    )
    var host: String = "127.0.0.1:9127"

    @CommandLine.Option(
        names = ["-n", "--name"],
        description = ["node name"],
        required = false
    )
    var name: String = ""

    override fun run() {
        if (host.isBlank()) {
            throw IllegalArgumentException("host cannot be blank")
        }

        var client: Client? = null
        var clientChannel: ClientChannel? = null
        try {
            client = ClientFactory.buildOstaraClient(host)
            val metadataWriter = client.metadataWriter

            clientChannel = ClientChannelFactory.buildClientChannel(client, host)
            var records = metadataWriter.queryNodeRecord(clientChannel)

            if (name.isNotBlank()) {
                records = records.stream().filter(Predicate.isEqual(name)).collect(Collectors.toSet())
            }
            println(records)
        } catch (t: Throwable) {
            println("failed to query node info, client-channel=$clientChannel, ${t.localizedMessage}")
        }

        client?.shutdownGracefully()
    }
}