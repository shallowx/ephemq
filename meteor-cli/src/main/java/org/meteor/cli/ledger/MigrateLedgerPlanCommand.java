package org.meteor.cli.ledger;

import com.google.gson.Gson;
import com.google.protobuf.ProtocolStringList;
import io.netty.util.internal.StringUtil;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.meteor.cli.support.Command;
import org.meteor.cli.support.CommandException;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientChannel;
import org.meteor.remote.proto.ClusterInfo;
import org.meteor.remote.proto.NodeMetadata;
import org.meteor.remote.proto.PartitionMetadata;
import org.meteor.remote.proto.TopicInfo;
import org.meteor.remote.proto.server.CalculatePartitionsResponse;
import org.meteor.remote.util.NetworkUtil;

/**
 * A command to create a migration plan from a broker cluster. This command
 * identifies the partitions that need to be migrated and the target brokers.
 */
public class MigrateLedgerPlanCommand implements Command {

    /**
     * A static final instance of the Gson class used for converting Java objects to JSON and vice versa.
     * <p>
     * This variable is used within the MigrateLedgerPlanCommand class to facilitate JSON parsing and serialization.
     */
    private static final Gson GSON = new Gson();

    /**
     * Returns the name of the command.
     *
     * @return the name of the command, which is "migrate-plan".
     */
    @Override
    public String name() {
        return "migrate-plan";
    }

    /**
     * Provides a description of the MigrateLedgerPlanCommand command.
     *
     * @return A string indicating the purpose of the MigrateLedgerPlanCommand.
     */
    @Override
    public String description() {
        return "Create migrate plan from the broker cluster";
    }

    /**
     * Builds and configures command line options for the MigrateLedgerPlanCommand.
     *
     * @param options the initial Options object to which new options are added
     * @return the modified Options object with the new options added
     */
    @Override
    public Options buildOptions(Options options) {
        Option brokerOpt =
                new Option("b", "-broker", true, "The broker address that is can connect to the broker cluster");
        brokerOpt.setRequired(true);
        options.addOption(brokerOpt);

        Option partitionOpt =
                new Option("ob", "-original-broker", true, "The original broker is the broker name of migrated out");
        partitionOpt.setRequired(true);
        options.addOption(partitionOpt);

        Option replicaOpt = new Option("eb", "-exclude-broker", true, "The broker is the excluded broker name");
        replicaOpt.setRequired(true);
        options.addOption(replicaOpt);

        return options;
    }

    /**
     * Executes the*/
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
                    System.out.printf("%s %s WARN %s - cluster node does not exists", currentTime(), Thread.currentThread().getName(), MigrateLedgerPlanCommand.class.getName());
                    return;
                }
                List<MigrateLedger> infos = new ObjectArrayList<>();
                Map<String, TopicInfo> topicInfos = client.queryTopicInfos(clientChannel);
                if (topicInfos == null || topicInfos.isEmpty()) {
                    System.out.printf("%s %s WARN %s - topic does not exists", currentTime(), Thread.currentThread().getName(), MigrateLedgerPlanCommand.class.getName());
                    return;
                }
                int limit = 0;
                if (!verify) {
                    String finalOriginal = original;
                    int sum = topicInfos.values().stream().mapToInt(topicInfo ->
                            (int) topicInfo.getPartitionsMap().entrySet().stream()
                                    .filter(
                                            entry -> entry.getValue().getReplicaNodeIdsList().contains(finalOriginal)
                                    ).count()).sum();
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
                System.out.printf("%s %s INFO %s - %s", currentTime(), Thread.currentThread().getName(), MigrateLedgerPlanCommand.class.getName(), GSON.toJson(infos));
            }
        } catch (Throwable t) {
            System.out.printf("%s %s ERROR - %s - %s", currentTime(), Thread.currentThread().getName(), MigrateLedgerPlanCommand.class.getName(), t.getMessage());
            throw new CommandException("Execution migrate ledger plan command[mlp] failed", t);
        }
    }

    /**
     * Modifies the partition map by retaining only the partitions whose keys are present
     * in the provided list of ids. Any ids not present in the partition map are added
     * to the map with a value of 0.
     *
     * @param partitions the map of broker identifiers to partition counts
     * @param ids the list of broker identifiers to be retained in the partitions map
     */
    private void compareTo(Map<String, Integer> partitions, List<String> ids) {
        Set<String> brokers = partitions.keySet();
        brokers.removeIf(broker -> !ids.contains(broker));
        for (String broker : ids) {
            if (!brokers.contains(broker)) {
                partitions.put(broker, 0);
            }
        }
    }

    /**
     * Selects the next broker for migration, given the current partition distributions and a list of brokers to exclude.
     *
     */
    private String select(Map<String, Integer> partitions, List<String> relicBrokers) {
        List<Map.Entry<String, Integer>> list = new ArrayList<>(partitions.entrySet());
        Map.Entry<String, Integer> entries = list.stream().filter(entry -> !relicBrokers.contains(entry.getKey()))
                .sorted(Comparator.comparingInt(Map.Entry::getValue))
                .toList().getFirst();
        String broker = entries.getKey();
        partitions.put(broker, entries.getValue() + 1);

        return broker;
    }
}
