package org.meteor.cli.cluster;

import io.netty.util.internal.StringUtil;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.meteor.cli.support.Command;
import org.meteor.cli.support.CommandException;
import org.meteor.cli.support.FormatPrint;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientChannel;
import org.meteor.common.message.Node;
import org.meteor.remote.proto.ClusterInfo;
import org.meteor.remote.proto.NodeMetadata;
import org.meteor.remote.util.NetworkUtil;


/**
 * ClusterListCommand is a concrete implementation of the Command interface.
 * It provides functionality to retrieve and display information about nodes in a broker cluster.
 *
 *
 * +--------+-----------+------+-----------------------+---------+-------+---------+------------------+
 * | id     | host      | port | registrationTimestamp | cluster | state | auxData | ledgerThroughput |
 * +--------+-----------+------+-----------------------+---------+-------+---------+------------------+
 * | meteor | 127.0.0.1 | 9527 | 07:59:59.999          | meteor  | UP    | {}      | {}               |
 * +--------+-----------+------+-----------------------+---------+-------+---------+------------------+
 */
public class ClusterListCommand implements Command {
    /**
     * Returns the name of the command related to cluster information retrieval.
     *
     * @return a string representing the command name.
     */
    @Override
    public String name() {
        return "cluster-info";
    }

    /**
     * Provides a description of the cluster node information retrieval command.
     *
     * @return a string describing the action of fetching cluster node information from the broker cluster.
     */
    @Override
    public String description() {
        return "Get cluster node info form broker cluster";
    }

    /**
     * Builds and adds the necessary options for the command.
     *
     * @param options The current set of command options.
     * @return The updated set of options with broker and cluster options added.
     */
    @Override
    public Options buildOptions(Options options) {
        Option brokerOpt =
                new Option("b", "-broker", true, "The broker address that is can connect to the broker cluster");
        brokerOpt.setRequired(true);
        options.addOption(brokerOpt);

        Option option = new Option("c", "-cluster", true, "The cluster name that is use to filter the cluster info");
        option.setRequired(false);
        options.addOption(option);
        return options;
    }

    /**
     *
     */
    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {
        List<Node> nodes = new ArrayList<>();
        try {
            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b');
                if (StringUtil.isNullOrEmpty(addr)) {
                    throw new IllegalArgumentException(
                            "Meteor-cli illegal argument exception, broker-addr cannot be empty.");
                }
                SocketAddress socketAddress = NetworkUtil.switchSocketAddress(addr);
                ClientChannel clientChannel = client.getActiveChannel(socketAddress);
                ClusterInfo clusterInfo = client.queryClusterInfo(clientChannel);
                Map<String, NodeMetadata> nodesMap = clusterInfo.getNodesMap();
                List<NodeMetadata> metadata = new ArrayList<>(nodesMap.values());

                System.out.println(
                        STR."\{currentTime()} [Thread.currentThread().getName()] INFO \{ClusterListCommand.class.getName()} - Print the cluster metadata options: ");
                for (NodeMetadata nodeMetadata : metadata) {
                    nodes.add(
                            new Node(nodeMetadata.getId(),
                                    nodeMetadata.getHost(),
                                    nodeMetadata.getPort(),
                                    -1,
                                    nodeMetadata.getClusterName(), "UP")
                    );
                }
            }

            if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c');
                if (StringUtil.isNullOrEmpty(clusterName)) {
                    nodes = nodes.stream().filter(n -> n.getCluster().equals(clusterName)).toList();
                }
            }
            formatPrint(nodes);
        } catch (Exception e) {
            System.err.println(
                    STR."\{currentTime()} [Thread.currentThread().getName()] ERROR \{ClusterListCommand.class.getName()} - \{e.getMessage()}");
            throw new CommandException("Execute cluster command[clusters] error", e);
        }
    }

    /**
     * Formats and prints the details of the provided list of nodes.
     *
     * @param nodes the list of nodes to be formatted and printed.
     */
    private void formatPrint(List<Node> nodes) {
        String[] title =
                {"id", "host", "port", "registrationTimestamp", "cluster", "state", "auxData", "ledgerThroughput"};
        FormatPrint.formatPrint(nodes, title);
    }
}
