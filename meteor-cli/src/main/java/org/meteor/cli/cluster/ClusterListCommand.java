package org.meteor.cli.cluster;

import io.netty.util.internal.StringUtil;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.meteor.cli.core.Command;
import org.meteor.client.ClientChannel;
import org.meteor.client.internal.Client;
import org.meteor.common.message.Node;
import org.meteor.remote.proto.ClusterInfo;
import org.meteor.remote.proto.NodeMetadata;
import org.meteor.remote.util.NetworkUtil;

public class ClusterListCommand implements Command {

    @Override
    public String name() {
        return "clusters";
    }

    @Override
    public String description() {
        return "Get cluster node info form broker cluster";
    }

    @Override
    public Options buildOptions(Options options) {
        Option brokerOpt = new Option("b", "--broker", true, "The broker address that is can connect to the broker cluster");
        brokerOpt.setRequired(true);
        options.addOption(brokerOpt);

        Option option = new Option("c", "--cluster", true, "The cluster name that is use to filter the cluster info");
        option.setRequired(false);
        options.addOption(option);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {
        try {
            List<Node> nodes = new ArrayList<>();

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b');
                if (StringUtil.isNullOrEmpty(addr)) {
                    throw new IllegalArgumentException();
                }
                SocketAddress socketAddress = NetworkUtil.switchSocketAddress(addr);
                ClientChannel clientChannel = client.getActiveChannel(socketAddress);
                ClusterInfo clusterInfo = client.queryClusterInfo(clientChannel);
                Map<String, NodeMetadata> nodesMap = clusterInfo.getNodesMap();
                List<NodeMetadata> metadata = new ArrayList<>(nodesMap.values());

                System.out.printf("%s [%s] INFO %S - Print the cluster metadata options: \n",
                        newDate(), Thread.currentThread().getName(), ClusterListCommand.class.getName());
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
            System.out.printf("%s [%s] INFO %S - %s \n", newDate(), Thread.currentThread().getName(), ClusterListCommand.class.getName(), gson.toJson(nodes));
        } catch (Exception e) {
            System.out.printf("%s [%S] ERROR %s - %s \n", newDate(), Thread.currentThread().getName(), ClusterListCommand.class.getName(), e.getCause().getMessage());
            throw new RuntimeException(e);
        }
    }
}
