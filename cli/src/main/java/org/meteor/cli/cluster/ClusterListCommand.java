package org.meteor.cli.cluster;

import com.google.gson.Gson;
import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.meteor.cli.Command;
import org.meteor.client.internal.Client;
import org.meteor.client.internal.ClientChannel;
import org.meteor.common.message.Node;
import org.meteor.remote.proto.ClusterInfo;
import org.meteor.remote.proto.NodeMetadata;
import org.meteor.remote.util.NetworkUtils;

import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@SuppressWarnings("all")
public class ClusterListCommand implements Command {
    private static String newDate() {
        SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");
        return format.format(new Date());
    }

    @Override
    public String name() {
        return "clusters";
    }

    @Override
    public String description() {
        return "node list all of cluster";
    }

    @Override
    public Options buildOptions(Options options) {
        Option option = new Option("c", "cluster", true, "cluster name");
        option.setRequired(false);
        options.addOption(option);

        Option brokerOpt = new Option("b", "broker", true, "cluster broker addr");
        brokerOpt.setRequired(true);
        options.addOption(brokerOpt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {
        try {
            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b');
                if (StringUtil.isNullOrEmpty(addr)) {
                    throw new IllegalArgumentException();
                }
                SocketAddress socketAddress = NetworkUtils.switchSocketAddress(addr);
                ClientChannel clientChannel = client.fetchChannel(socketAddress);
                ClusterInfo clusterInfo = client.queryClusterInfo(clientChannel);
                Map<String, NodeMetadata> nodesMap = clusterInfo.getNodesMap();
                List<NodeMetadata> metadata = new ArrayList<>(nodesMap.values());

                System.out.printf("%s [%s] INFO %S - Print the cluster metadata options: \n",
                        newDate(), Thread.currentThread().getName(), ClusterListCommand.class.getName());

                List<Node> nodes = new ArrayList<>();
                for (NodeMetadata nodeMetadata : metadata) {
                    nodes.add(
                            new Node(nodeMetadata.getId(),
                                    nodeMetadata.getHost(),
                                    nodeMetadata.getPort(),
                                    -1,
                                    nodeMetadata.getClusterName(), "UP")
                    );
                }
                Gson gson = new Gson();
                System.out.printf("%s [%s] INFO %S - %s \n",
                        newDate(), Thread.currentThread().getName(), ClusterListCommand.class.getName(), gson.toJson(nodes));
            }
        } catch (Exception e) {
            System.out.printf("%s [%S] ERROR %s - %s \n", newDate(), Thread.currentThread().getName(), ClusterListCommand.class.getName(), e.getCause().getMessage());
            throw new RuntimeException(e);
        }
    }
}
