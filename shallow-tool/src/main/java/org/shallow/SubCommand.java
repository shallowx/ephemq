package org.shallow;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public interface SubCommand {
    String name();
    String desc();
    Options buildOptions(final Options options);
    void execute(final CommandLine cmdLine, final Options options) throws Exception;
}
