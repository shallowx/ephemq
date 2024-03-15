package org.meteor.cli.ledger;

import com.google.protobuf.ProtocolStringList;
import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.meteor.cli.Command;
import org.meteor.client.internal.Client;
import org.meteor.client.internal.ClientChannel;
import org.meteor.remote.proto.ClusterInfo;
import org.meteor.remote.proto.NodeMetadata;
import org.meteor.remote.proto.PartitionMetadata;
import org.meteor.remote.proto.TopicInfo;
import org.meteor.remote.proto.server.CalculatePartitionsResponse;
import org.meteor.remote.util.NetworkUtil;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class MigrateLedgerPlanCommand implements Command {
    @Override
    public String name() {
        return "mlp";
    }

    @Override
    public String description() {
        return "create migrate plan";
    }

    @Override
    public Options buildOptions(Options options) {
        Option brokerOpt = new Option("ba", "broker-address", true, "which broker server");
        brokerOpt.setRequired(true);
        options.addOption(brokerOpt);

        Option partitionOpt = new Option("ob", "original-broker", true, "original broker");
        partitionOpt.setRequired(true);
        options.addOption(partitionOpt);

        Option replicaOpt = new Option("eb", "exclude-broker", true, "exclude broker");
        replicaOpt.setRequired(true);
        options.addOption(replicaOpt);

        Option configOpt = new Option("vc", "verify-completed", true, "verify completed");
        configOpt.setRequired(true);
        options.addOption(configOpt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {
        try {
            if (commandLine.hasOption('b')) {
                boolean verify = false;
                String original = null;
                if (commandLine.hasOption('o')) {
                    original = commandLine.getOptionValue('o').trim();
                }
                List<String> excludes = null;
                if (commandLine.hasOption('e')) {
                    String exclude = commandLine.getOptionValue('e').trim();
                    excludes = Arrays.asList(exclude.split(","));
                }
                if (commandLine.hasOption('v')) {
                    verify = true;
                }
                String addr = commandLine.getOptionValue('b');
                if (StringUtil.isNullOrEmpty(addr)) {
                    throw new IllegalArgumentException();
                }
                SocketAddress socketAddress = NetworkUtil.switchSocketAddress(addr);
                ClientChannel clientChannel = client.getActiveChannel(socketAddress);

                ClusterInfo clusterInfo = client.queryClusterInfo(clientChannel);
                List<String> ids = clusterInfo.getNodesMap().values().stream().map(NodeMetadata::getId).collect(Collectors.toList());
                if (original != null) {
                    ids.remove(original);
                }

                if (excludes != null && !excludes.isEmpty()) {
                    ids.removeAll(excludes);
                }

                if (ids.isEmpty()) {
                    System.out.printf("%s [%s] WARN %s - cluster node does not exists \n", newDate(), Thread.currentThread().getName(), MigrateLedgerPlanCommand.class.getName());
                    return;
                }
                List<MigrateLedger> infos = new LinkedList<>();
                Map<String, TopicInfo> topicInfos = client.queryTopicInfos(clientChannel);
                if (topicInfos == null || topicInfos.isEmpty()) {
                    System.out.printf("%s [%s] WARN %s - topic does not exists \n", newDate(), Thread.currentThread().getName(), MigrateLedgerPlanCommand.class.getName());
                    return;
                }
                int limit = 0;
                if (!verify) {
                    String finalOriginal = original;
                    int sum = topicInfos.values().stream().mapToInt(topicInfo ->
                            (int) topicInfo.getPartitionsMap().entrySet().stream()
                                    .filter(entry -> entry.getValue().getReplicaNodeIdsList().contains(finalOriginal)).count()).sum();
                    Scanner scanner = new Scanner(System.in);
                    limit = Math.min(Integer.parseInt(scanner.next()), sum);
                }
                CalculatePartitionsResponse response = client.calculatePartitions();
                Map<String, Integer> partitions = response.getPartitionsMap();
                ConcurrentMap<String, Integer> map = new ConcurrentHashMap<>(partitions);
                compareTo(map, ids);

                int current = 0;
                for (TopicInfo topicInfo : topicInfos.values()) {
                    for (PartitionMetadata partitionMetadata : topicInfo.getPartitionsMap().values()) {
                        ProtocolStringList replicas = partitionMetadata.getReplicaNodeIdsList();
                        if (replicas.contains(original)) {
                            if (current++ >= limit && !verify) {
                                break;
                            }

                            List<String> replicaBrokers = replicas.stream().parallel().collect(Collectors.toList());
                            String destination = select(map, replicaBrokers);
                            infos.add(new MigrateLedger(partitionMetadata.getTopicName(), partitionMetadata.getId(), original, destination));
                        }
                    }
                }
                System.out.printf("%s [%s] INFO %s - %s \n", newDate(), Thread.currentThread().getName(), MigrateLedgerPlanCommand.class.getName(), gson.toJson(infos));
            }
        } catch (Throwable t) {
            System.out.printf("%s [%s] ERROR %s - %s \n", newDate(), Thread.currentThread().getName(), MigrateLedgerPlanCommand.class.getName(), t.getCause().getMessage());
            throw new RuntimeException(t);
        }
    }

    private void compareTo(Map<String, Integer> partitions, List<String> ids) {
        Set<String> brokers = partitions.keySet();
        brokers.removeIf(broker -> !ids.contains(broker));
        for (String broker : ids) {
            if (!brokers.contains(broker)) {
                partitions.put(broker, 0);
            }
        }
    }

    private String select(Map<String, Integer> partitions, List<String> relicBrokers) {
        List<Map.Entry<String, Integer>> list = new ArrayList<>(partitions.entrySet());
        Map.Entry<String, Integer> entries = list.stream().filter(entry -> !relicBrokers.contains(entry.getKey()))
                .sorted(Comparator.comparingInt(Map.Entry::getValue))
                .toList().get(0);
        String broker = entries.getKey();
        partitions.put(broker, entries.getValue() + 1);

        return broker;
    }
}
