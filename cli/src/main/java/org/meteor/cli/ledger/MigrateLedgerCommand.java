package org.meteor.cli.ledger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.meteor.cli.Command;
import org.meteor.client.internal.Client;
import org.meteor.common.util.StringUtils;
import org.meteor.remote.proto.server.MigrateLedgerResponse;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

@SuppressWarnings("all")
public class MigrateLedgerCommand implements Command {
    private static final ExecutorService retry = Executors.newSingleThreadExecutor(new DefaultThreadFactory("migrate-retry-thread"));

    private static String newDate() {
        SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");
        return format.format(new Date());
    }

    @Override
    public String name() {
        return "migrateLedger";
    }

    @Override
    public String description() {
        return "migrate ledger";
    }

    @Override
    public Options buildOptions(Options options) {
        Option bOpt = new Option("b", "broker address", true, "which broker server");
        bOpt.setRequired(true);
        options.addOption(bOpt);

        Option originalOpt = new Option("o", "original broker", true, "original broker");
        originalOpt.setRequired(true);
        options.addOption(originalOpt);

        Option topicOpt = new Option("t", "topic", true, "which topic name");
        topicOpt.setRequired(true);
        options.addOption(topicOpt);


        Option partitionOpt = new Option("p", "partition", true, "which partition id");
        partitionOpt.setRequired(true);
        options.addOption(partitionOpt);

        Option explainOpt = new Option("e", "explain file", true, "explain file");
        explainOpt.setRequired(true);
        options.addOption(explainOpt);

        Option destOpt = new Option("d", "destination broker", true, "destination broker");
        destOpt.setRequired(true);
        options.addOption(destOpt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {
        try {
            String file = null;
            if (commandLine.hasOption('e')) {
                file = commandLine.getOptionValue('e');
                if (!StringUtils.isNullOrEmpty(file)) {
                    String content = FileUtils.readFileToString(new File(file), StandardCharsets.UTF_8);
                    Gson gson = new Gson();
                    List<MigrateLedger> infos = gson.fromJson(content, new TypeToken<List<MigrateLedger>>() {
                    }.getType());
                    if (infos == null || infos.isEmpty()) {
                        return;
                    }

                    for (MigrateLedger info : infos) {
                        try {
                            MigrateLedgerResponse response = client.migrateLedger(info.getTopic(), info.getPartition(), info.getFrom(), info.getTo());
                            if (response.getSuccess()) {
                                System.out.printf("%s [%s] INFO %s - Migrate ledger successfully, topic=%s partition=%s \n", newDate(), Thread.currentThread().getName(), MigrateLedgerCommand.class.getName(), info.getTopic(), info.getPartition());
                                continue;
                            }
                            throw new IllegalStateException(String.format("Migrate ledger failure, and try again later, topic=%s partition=%s", info.getTopic(), info.getPartition()));
                        } catch (Exception e) {
                            System.out.printf("%s [%s] ERROR %s-%s", newDate(), Thread.currentThread().getName(), MigrateLedgerPlanCommand.class.getName(), e.getMessage());
                            retry(client, info.getTopic(), info.getPartition(), info.getFrom(), info.getTo());
                        }
                    }
                }
            }
        } catch (Throwable t) {
            System.out.printf("%s [%s] ERROR %s-%s", newDate(), Thread.currentThread().getName(), MigrateLedgerPlanCommand.class.getName(), t.getMessage());
        }
    }

    private void retry(Client client, String topic, int partition, String original, String destination) throws ExecutionException, InterruptedException {
        Future<?> future = retry.submit(() -> {
            try {
                client.migrateLedger(topic, partition, original, destination);
                return CompletableFuture.completedFuture(true);
            } catch (Exception e) {
                System.out.printf("Migrate ledger retry failure, and try again later, topic=%s partition=%s", topic, partition);
                return CompletableFuture.completedFuture(false);
            }
        });

        if (future.get() == Boolean.FALSE) {
            retry(client, topic, partition, original, destination);
            return;
        }
    }
}
