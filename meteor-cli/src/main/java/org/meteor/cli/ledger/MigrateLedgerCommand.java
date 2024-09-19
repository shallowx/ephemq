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

/**
 * The MigrateLedgerCommand class implements the Command interface and is used to migrate
 * a ledger from one broker to another within a broker cluster.
 */
public class MigrateLedgerCommand implements Command {

    /**
     * Gson instance used for JSON serialization and deserialization
     * within the MigrateLedgerCommand class.
     * <p>
     * This instance is declared as a static and final field
     * to ensure that the same Gson object is shared across all
     * instances of the class, providing a consistent and
     * thread-safe way to handle JSON operations.
     */
    private static final Gson gson = new Gson();

    /**
     * An ExecutorService instance that's used to handle retry operations in virtual threads.
     * This executor service creates new threads with the naming pattern "migrate-retry-thread".
     */
    private static final ExecutorService retry = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("migrate-retry-thread").factory()
    );

    /**
     * Returns the name of the command.
     *
     * @return the name of the command, which is "migrate"
     */
    @Override
    public String name() {
        return "migrate";
    }

    /**
     * Provides a description of the migrate ledger command.
     *
     * @return a brief description of what the migrate ledger command does, specifically
     *         stating that it is utilized to migrate any broker to another broker.
     */
    @Override
    public String description() {
        return "The migrate ledger command is used to migrate any broker to other broker";
    }

    /**
     * Builds and adds the necessary command line options for the migration process.
     *
     * @param options The initial set of options to which new options will be added.
     * @return The updated set of options with added migration-specific parameters.
     */
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

    /**
     * Executes the migrate ledger command.
     *
     * @param commandLine the command line arguments
     * @param options additional options for the command
     * @param client the client to interact with the ledger service
     * @throws Exception if an error occurs during execution
     */
    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {
        try {
            String file;
            if (commandLine.hasOption('e')) {
                file = commandLine.getOptionValue('e');
                if (!StringUtil.isNullOrEmpty(file)) {
                    String content = FileUtils.readFileToString(new File(file), StandardCharsets.UTF_8);

                    List<MigrateLedger> infos = gson.fromJson(content, new TypeToken<List<MigrateLedger>>() {
                    }.getType());
                    if (infos == null || infos.isEmpty()) {
                        System.out.printf(STR."\{currentTime()} [\{Thread.currentThread()
                                .getName()}] INFO \{MigrateLedgerCommand.class.getName()} - Migrate ledger successfully, partition ledger doex not exists");
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
                                    STR."[:: topic:\{info.getTopic()}, partition:\{info.getPartition()}]Migrate ledger failure, and try again later");
                        } catch (Exception e) {
                            System.err.println(STR."\{currentTime()} [\{Thread.currentThread()
                                    .getName()}] ERROR \{MigrateLedgerPlanCommand.class.getName()} - \{e.getMessage()}");
                            retry(client, info.getTopic(), info.getPartition(), info.getFrom(), info.getTo());
                        }
                    }
                }
            } else {
                throw new IllegalArgumentException("Meteor-cli illegal argument exception, [-e] args cannot be empty.");
            }
        } catch (Throwable t) {
            System.out.println(STR."\{currentTime()} [\{Thread.currentThread()
                    .getName()}] ERROR \{MigrateLedgerPlanCommand.class.getName()} - \{t.getMessage()}");
            throw new CommandException("Execution migrate ledger command[:migrate] failed", t);
        }
    }

    /**
     * Attempts to retry the migration of a ledger from an original broker to a destination broker for a given
     * topic and partition if the initial migration attempt fails. The method waits for 30 seconds before
     * each retry attempt. If a retry fails, it will recursively attempt again.
     *
     * @param client The client instance used to perform the ledger migration.
     * @param topic The topic associated with the ledger to be migrated.
     * @param partition The partition number of the ledger to be migrated.
     * @param original The original broker from which the ledger is being migrated.
     * @param destination The destination broker to which the ledger is being migrated.
     * @throws ExecutionException If the migration task throws an exception.
     * @throws InterruptedException If the current thread is interrupted while waiting.
     */
    private void retry(Client client, String topic, int partition, String original, String destination) throws ExecutionException, InterruptedException {
        System.out.println(
                STR."Migrate ledger[topic:\{topic}, partition:\{partition}, original-broker:\{original}, destination-broker:\{destination}] failed, and try again"
                        + STR." after 30 s, if you want to stop it, ant you can execute `Ctrl-C`");
        TimeUnit.SECONDS.sleep(30);
        Future<?> future = retry.submit(() -> {
            try {
                client.migrateLedger(topic, partition, original, destination);
                return CompletableFuture.completedFuture(true);
            } catch (Exception e) {
                System.out.println(
                        STR."[:: topic:\{topic}, partition:\{partition}]Migrate ledger retry failure, and try again later");
                return CompletableFuture.completedFuture(false);
            }
        });

        if (future.get() == Boolean.FALSE) {
            retry(client, topic, partition, original, destination);
        }
    }
}
