package org.ostara.cli;

import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.ostara.client.internal.Client;
import org.ostara.client.internal.ClientChannel;
import org.ostara.remote.proto.TopicInfo;
import org.ostara.remote.util.NetworkUtils;

import java.net.SocketAddress;
import java.util.Map;

public class TopicListCommand implements Command{
    @Override
    public String name() {
        return "TopicList";
    }

    @Override
    public String description() {
        return "Get topics from server";
    }

    @Override
    public Options buildOptions(Options options) {
        Option brokerOpt = new Option("b", "broker address", true, "which broker server");
        brokerOpt.setRequired(true);
        options.addOption(brokerOpt);

        Option clusterOpt = new Option("c", "cluster", true, "which cluster name");
        clusterOpt.setRequired(true);
        options.addOption(clusterOpt);

        Option topicOpt = new Option("t", "topic", true, "which topic name");
        topicOpt.setRequired(true);
        options.addOption(topicOpt);

        Option ledgerOpt = new Option("l", "ledger", true, "which ledger id");
        ledgerOpt.setRequired(true);
        options.addOption(ledgerOpt);

        Option partitionOpt = new Option("p", "partition", true, "which partition id");
        partitionOpt.setRequired(true);
        options.addOption(partitionOpt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {
        try {
            if (commandLine.hasOption('b')) {
                String address = commandLine.getOptionValue("b").trim();
                if (StringUtil.isNullOrEmpty(address)) {
                    throw new IllegalStateException();
                }
                SocketAddress socketAddress = NetworkUtils.switchSocketAddress(address);
                ClientChannel clientChannel = client.fetchChannel(socketAddress);

                if (commandLine.hasOption("t")) {
                    String topic = commandLine.getOptionValue("t").trim();
                    Map<String, TopicInfo> topicInfos = StringUtil.isNullOrEmpty(topic)
                            ? client.queryTopicInfos(clientChannel, topic)
                            : client.queryTopicInfos(clientChannel);

                    if (topicInfos.isEmpty()) {
                        System.out.print("Topic info is empty");
                        return;
                    }

                    System.out.println(topicInfos);
                }
            }

        } catch (Throwable t){
            System.out.println(t.getMessage());
        }
    }
}
