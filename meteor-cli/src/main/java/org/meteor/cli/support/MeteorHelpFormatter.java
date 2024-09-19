package org.meteor.cli.support;

import java.io.PrintWriter;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * This class extends HelpFormatter to provide custom formatting for command-line options help output.
 */
public class MeteorHelpFormatter extends HelpFormatter {

    /**
     * Prints the formatted options to the provided PrintWriter.
     *
     * @param pw      the PrintWriter to write the options to.
     * @param width   the maximum width of a line.
     * @param options the Options object containing the command-line options.
     * @param leftPad the number of spaces to pad on the left.
     * @param descPad the number of spaces between the option and its description.
     */
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

    /**
     * Formats a command-line option and its description into a string with specified width and padding.
     *
     * @param option The command-line option to format.
     * @param description The description of the command-line option.
     * @param width The total width of the line to format.
     * @param leftPad The number of spaces to pad on the left of the option.
     * @param descPad The number of spaces to pad between the option and its description.
     * @return A formatted string representing the command-line option and its description.
     */
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
