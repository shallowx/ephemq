package org.meteor.cli.ledger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.meteor.cli.support.Command;
import org.meteor.cli.support.CommandException;
import org.meteor.cli.support.FormatPrint;
import org.meteor.client.core.Client;
import org.meteor.common.util.StringUtil;
import org.meteor.remote.proto.server.MigrateLedgerResponse;

public class MigrateLedgerCommand implements Command {
    private static final ExecutorService retry = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("migrate-retry-thread").factory()
    );

    @Override
    public String name() {
        return "migrate";
    }

    @Override
    public String description() {
        return "The migrate ledger command is used to migrate any broker to other broker";
    }

    @Override
    public Options buildOptions(Options options) {
        Option bOpt = new Option("b", "-broker", true, "The broker address that is can connect to the broker cluster");
        bOpt.setRequired(true);
        options.addOption(bOpt);

        Option originalOpt =
                new Option("ob", "-original-broker", true, "The original broker is the broker name of migrated out");
        originalOpt.setRequired(true);
        options.addOption(originalOpt);

        Option topicOpt = new Option("t", "-topic", true, "The topic is the name of migrated out");
        topicOpt.setRequired(true);
        options.addOption(topicOpt);


        Option partitionOpt = new Option("p", "-partition", true, "The partition is the id of migrated out");
        partitionOpt.setRequired(true);
        options.addOption(partitionOpt);

        Option explainOpt =
                new Option("ef", "-explain-file", true, "The file is explain file that will over other commands");
        explainOpt.setRequired(true);
        options.addOption(explainOpt);

        Option destOpt = new Option("db", "-destination-broker", true,
                "The destination broker is the broker name of  the migration destination");
        destOpt.setRequired(true);
        options.addOption(destOpt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {
        try {
            String file;
            if (commandLine.hasOption('e')) {
                file = commandLine.getOptionValue('e');
                if (!StringUtil.isNullOrEmpty(file)) {
                    String content = FileUtils.readFileToString(new File(file), StandardCharsets.UTF_8);
                    Gson gson = new Gson();
                    List<MigrateLedger> infos = gson.fromJson(content, new TypeToken<List<MigrateLedger>>() {
                    }.getType());
                    if (infos == null || infos.isEmpty()) {
                        System.out.printf(
                                "%s [%s] INFO %s - Migrate ledger successfully, partition ledger doex not exists \n",
                                currentTime(), Thread.currentThread().getName(), MigrateLedgerCommand.class.getName());
                        return;
                    }

                    for (MigrateLedger info : infos) {
                        try {
                            MigrateLedgerResponse response = client.migrateLedger(info.getTopic(), info.getPartition(), info.getFrom(), info.getTo());
                            if (response.getSuccess()) {
                                String[] title = {"topic", "partition", "from", "to"};
                                List<MigrateLedger> migrateLedgers = new ArrayList<>();
                                migrateLedgers.add(info);
                                FormatPrint.formatPrint(migrateLedgers, title);
                                continue;
                            }
                            throw new CommandException(
                                    String.format("Migrate ledger failure, and try again later, topic=%s partition=%s",
                                            info.getTopic(), info.getPartition()));
                        } catch (Exception e) {
                            System.out.printf("%s [%s] ERROR %s-%s", currentTime(), Thread.currentThread().getName(),
                                    MigrateLedgerPlanCommand.class.getName(), e.getMessage());
                            retry(client, info.getTopic(), info.getPartition(), info.getFrom(), info.getTo());
                        }
                    }
                }
            }
        } catch (Throwable t) {
            System.out.printf("%s [%s] ERROR %s-%s", currentTime(), Thread.currentThread().getName(),
                    MigrateLedgerPlanCommand.class.getName(), t.getMessage());
            throw new CommandException("Execution migrate ledger command[ml] failed", t);
        }
    }

    private void retry(Client client, String topic, int partition, String original, String destination) throws ExecutionException, InterruptedException {
        System.out.printf(
                "Migrate ledger[topic=%s partition=%d original-broker=%s destination-broker=%s] failed, and try again"
                        + " after %d s, if you want to stop it, ant you can execute `Ctrl-C`",
                topic, partition, original, destination, 30);
        TimeUnit.SECONDS.sleep(30);
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
        }
    }
}
