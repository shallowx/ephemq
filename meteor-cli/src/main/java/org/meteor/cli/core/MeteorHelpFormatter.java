package org.meteor.cli.core;

import java.io.PrintWriter;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class MeteorHelpFormatter extends HelpFormatter {

    @Override
    public void printOptions(PrintWriter pw, int width, Options options, int leftPad, int descPad) {
        for (Option option : options.getOptions()) {
            StringBuilder builder = new StringBuilder();
            if (option.getOpt() == null) {
                builder.append("   ").append(getLongOptPrefix()).append(option.getLongOpt());
            } else {
                builder.append(getOptPrefix()).append(option.getOpt());
                if (option.hasLongOpt()) {
                    builder.append(',').append(getLongOptPrefix()).append(option.getLongOpt());
                }
            }
            if (option.hasArg()) {
                String argName = option.getArgName();
                if (argName != null && !argName.isEmpty()) {
                    builder.append(' ').append(argName);
                }
            }
            String description = option.getDescription();
            pw.println(formatDescription(builder.toString(), description, width, leftPad, descPad));
        }
    }

    private String formatDescription(String option, String description, int width, int leftPad, int descPad) {
        StringBuilder sb = new StringBuilder();
        sb.append(createPadding(leftPad));
        sb.append(option);
        if (description != null) {
            if (option.length() > leftPad + descPad) {
                sb.append("\n");
                sb.append(createPadding(leftPad + descPad));
            } else {
                sb.append(createPadding(width - (leftPad + option.length())));
            }
            sb.append(description);
        }
        return sb.toString();
    }
}
