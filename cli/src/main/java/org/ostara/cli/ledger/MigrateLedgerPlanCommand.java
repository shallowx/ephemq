package org.ostara.cli.ledger;

import com.google.gson.Gson;
import com.google.protobuf.ProtocolStringList;
import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.ostara.cli.Command;
import org.ostara.client.internal.Client;
import org.ostara.client.internal.ClientChannel;
import org.ostara.remote.proto.ClusterInfo;
import org.ostara.remote.proto.NodeMetadata;
import org.ostara.remote.proto.PartitionMetadata;
import org.ostara.remote.proto.TopicInfo;
import org.ostara.remote.proto.server.CalculatePartitionsResponse;
import org.ostara.remote.util.NetworkUtils;

import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@SuppressWarnings("all")
public class MigrateLedgerPlanCommand implements Command {
    private static String newDate() {
        SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");
        return format.format(new Date());
    }

    @Override
    public String name() {
        return "migratePlan";
    }

    @Override
    public String description() {
        return "create migrate plan file";
    }

    @Override
    public Options buildOptions(Options options) {
        Option brokerOpt = new Option("b", "broker address", true, "which broker server");
        brokerOpt.setRequired(true);
        options.addOption(brokerOpt);

        Option partitionOpt = new Option("o", "original broker", true, "original broker");
        partitionOpt.setRequired(true);
        options.addOption(partitionOpt);

        Option replicaOpt = new Option("e", "exclude broker", true, "exclude broker");
        partitionOpt.setRequired(true);
        options.addOption(replicaOpt);

        Option configOpt = new Option("v", "verify completed", true, "verify completed");
        partitionOpt.setRequired(true);
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
                SocketAddress socketAddress = NetworkUtils.switchSocketAddress(addr);
                ClientChannel clientChannel = client.fetchChannel(socketAddress);

                ClusterInfo clusterInfo = client.queryClusterInfo(clientChannel);
                List<String> ids = clusterInfo.getNodesMap().values().stream().map(NodeMetadata::getId).collect(Collectors.toList());
                if (original != null) {
                    ids.remove(original);
                }

                if (excludes != null && !excludes.isEmpty()) {
                    ids.removeAll(excludes);
                }

                if (ids.isEmpty()) {
                    return;
                }
                List<MigrateLedger> infos = new LinkedList<>();
                Map<String, TopicInfo> topicInfos = client.queryTopicInfos(clientChannel);
                if (topicInfos == null || topicInfos.isEmpty()) {
                    return;
                }
                int limit = 0;
                if (!verify) {
                    String finalOriginal = original;
                    int sum = topicInfos.values().stream().mapToInt(topicInfo ->
                            (int) topicInfo.getPartitionsMap().entrySet().stream()
                                    .filter(entry -> entry.getValue().getReplicaNodeIdsList().contains(finalOriginal)).count()).sum();
                    Scanner scanner = new Scanner(System.in);
                    limit = Integer.parseInt(scanner.next());
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

                Gson gson = new Gson();
                System.out.printf("%s [%s] INFO %s - %s \n", newDate(), Thread.currentThread().getName(), MigrateLedgerPlanCommand.class.getName(), gson.toJson(infos));
            }
        } catch (Throwable t) {
            System.out.printf("%s [%s] ERROR %s - %s \n", newDate(), Thread.currentThread().getName(), MigrateLedgerPlanCommand.class.getName(), t.getCause().getMessage());
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

    private String select(Map<String, Integer> partitions, List<String> relicaBrokres) {
        List<Map.Entry<String, Integer>> list = new ArrayList<>(partitions.entrySet());
        Map.Entry<String, Integer> entries = list.stream().filter(entry -> !relicaBrokres.contains(entry.getKey()))
                .sorted(Comparator.comparingInt(Map.Entry::getValue))
                .collect(Collectors.toList()).get(0);
        String beoker = entries.getKey();
        partitions.put(beoker, entries.getValue() + 1);

        return beoker;
    }
}
