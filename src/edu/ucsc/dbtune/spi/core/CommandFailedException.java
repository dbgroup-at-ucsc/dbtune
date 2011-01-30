package edu.ucsc.dbtune.spi.core;

import java.util.List;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class CommandFailedException extends RuntimeException {
    private static final long serialVersionUID = 0;

    public CommandFailedException(List<String> args, List<String> outputLines) {
        super(formatMessage(args, outputLines));
    }

    public static String formatMessage(List<String> args, List<String> outputLines) {
        final StringBuilder result = new StringBuilder();
        result.append("Command failed:");
        for (String arg : args) {
            result.append(" ").append(arg);
        }
        for (String outputLine : outputLines) {
            result.append("\n  ").append(outputLine);
        }
        return result.toString();
    }
}
