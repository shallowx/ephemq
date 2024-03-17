package org.meteor.cli.core;

import com.google.gson.Gson;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.meteor.client.internal.Client;

import java.text.SimpleDateFormat;
import java.util.Date;

public interface Command {
    Gson gson = new Gson();
    SimpleDateFormat FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");

    String name();

    String description();

    Options buildOptions(final Options options);

    void execute(final CommandLine commandLine, final Options options, Client client) throws Exception;

    default String newDate() {
        return FORMAT.format(new Date());
    }
}
