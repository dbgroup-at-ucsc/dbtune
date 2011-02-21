package edu.ucsc.dbtune.spi.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hsanchez@ucsc.edu (Huascar A. Sanchez)
 */
public abstract class Console implements Printer {
    static final long DAY_MILLIS    = 1000 * 60 * 60 * 24;
    static final long HOUR_MILLIS   = 1000 * 60 * 60;
    static final long WARNING_HOURS = 12;
    static final long FAILURE_HOURS = 48;

    private boolean useColor;
    private boolean ansi;
    private boolean verbose;
    protected String indent;
    protected CurrentLine currentLine = CurrentLine.NEW;
    protected final ClearScreenHistory out = new ClearScreenHistory(System.out);
    protected ClearScreenHistory.Mark currentVerboseMark;
    protected ClearScreenHistory.Mark currentStreamMark;

    private Console() {}

    public void setIndent(String indent) {
        this.indent = indent;
    }

    public static Console streaming(){
        return Installer.STREAMING;
    }

    public static Console multiplexing(){
        return Installer.MULTIPLEXING;
    }

    public void setUseColor(boolean useColor, int passColor, int warnColor, int failColor) {
        this.useColor = useColor;
        Color.PASS.setCode(passColor);
        Color.WARN.setCode(warnColor);
        Color.FAIL.setCode(failColor);
        Color.COMMENT.setCode(34);
    }

    public void setAnsi(boolean ansi) {
        this.ansi = ansi;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public synchronized void verbose(String s) {
        /*
         * terminal does't support overwriting output, so don't print
         * verbose message unless requested.
         */
        if (!verbose && !ansi) {
            return;
        }
        /*
         * When writing verbose output in the middle of streamed output, keep
         * the streamed mark location. That way we can remove the verbose output
         * later without losing our position mid-line in the streamed output.
         */
        ClearScreenHistory.Mark savedStreamMark = currentLine == CurrentLine.STREAMED_OUTPUT
                ? out.mark()
                : currentStreamMark;
        newLine();
        currentStreamMark = savedStreamMark;

        currentVerboseMark = out.mark();
        out.print(s);
        currentLine = CurrentLine.VERBOSE;
    }

    public synchronized void warn(String message) {
        warn(message, Collections.<String>emptyList());
    }

    /**
     * Warns, and also puts a list of strings afterwards.
     * @param message
     *      text to be displayed on screen
     * @param list
     *      a list of strings to put afterwards message.
     */
    public synchronized void warn(String message, List<String> list) {
        newLine();
        out.println(colorString("Warning: " + message, Color.WARN));
        for (String item : list) {
            out.println(colorString(indent + item, Color.WARN));
        }
    }

    public synchronized void info(String s) {
        newLine();
        out.println(s);
    }

    public synchronized void info(String message, Throwable throwable) {
        newLine();
        out.println(message);
        throwable.printStackTrace(System.out);
    }

    /**
     * Begins streaming output for the named action.
     * @param name
     *      name of action
     */
    public void action(String name) {}

    /**
     * Begins streaming output for the named outcome.
     * @param name
     *      name of outcome
     */
    public void outcome(String name) {}

    /**
     * Appends the action output immediately to the stream when streaming is on,
     * or to a buffer when streaming is off. Buffered output will be held and
     * printed only if the outcome is unsuccessful.
     * @param outcomeName
     *      name of outcome
     * @param output
     *      output text
     */
    public abstract void streamOutput(String outcomeName, String output);

    /**
     * Hook to flush anything streamed via {@link #streamOutput}.
     * @param outcomeName
     *      outcome name
     */
    protected void flushBufferedOutput(String outcomeName) {}

    /**
     * Prints the action output with appropriate indentation.
     * @param streamedOutput
     *      streamed output
     */
    public synchronized void streamOutput(CharSequence streamedOutput) {
        if (streamedOutput.length() == 0) {
            return;
        }

        String[] lines = messageToLines(streamedOutput.toString());

        if (currentLine == CurrentLine.VERBOSE && currentStreamMark != null && ansi) {
            currentStreamMark.reset();
            currentStreamMark = null;
        } else if (currentLine != CurrentLine.STREAMED_OUTPUT) {
            newLine();
            out.print(indent);
            out.print(indent);
        }
        out.print(lines[0]);
        currentLine = CurrentLine.STREAMED_OUTPUT;

        for (int i = 1; i < lines.length; i++) {
            newLine();

            if (lines[i].length() > 0) {
                out.print(indent);
                out.print(indent);
                out.print(lines[i]);
                currentLine = CurrentLine.STREAMED_OUTPUT;
            }
        }
    }

    /**
     * Inserts a linebreak if necessary.
     */
    protected void newLine() {
        currentStreamMark = null;

        if (currentLine == CurrentLine.VERBOSE && !verbose && ansi) {
            /*
             * Verbose means we leave all verbose output on the screen.
             * Otherwise we overwrite verbose output when new output arrives.
             */
            currentVerboseMark.reset();
        } else if (currentLine != CurrentLine.NEW) {
            out.print("\n");
        }

        currentLine = CurrentLine.NEW;
    }

    /**
     * Status of a currently-in-progress line of output.
     */
    enum CurrentLine {

        /**
         * The line is blank.
         */
        NEW,

        /**
         * The line contains streamed application output. Additional streamed
         * output may be appended without additional line separators or
         * indentation.
         */
        STREAMED_OUTPUT,

        /**
         * The line contains the name of an action or outcome. The outcome's
         * result (such as "OK") can be appended without additional line
         * separators or indentation.
         */
        NAME,

        /**
         * The line contains verbose output, and may be overwritten.
         */
        VERBOSE,
    }

    /**
     * Returns an array containing the lines of the given text.
     * @param message
     *      message to be displayed on screen.
     * @return message in splitted form.
     */
    private String[] messageToLines(String message) {
        // pass Integer.MAX_VALUE so split doesn't trim trailing empty strings.
        return message.split("\r\n|\r|\n", Integer.MAX_VALUE);
    }

    private enum Color {
        PASS, FAIL, WARN, COMMENT;

        int code = 0;

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }
    }

    protected String colorString(String message, Color color) {
        return useColor ? ("\u001b[" + color.getCode() + ";1m" + message + "\u001b[0m") : message;
    }

    /**
     * Console has not been initialized.
     */
    static class InvalidConsole extends Console {

        @Override
        public void streamOutput(String outcomeName, String output) {
            throw new IllegalStateException("Call Console.init() first");
        }
    }

    /**
     * This console prints output as it's emitted. It supports at most one
     * action at a time.
     */
    static class StreamingConsole extends Console {
        private String currentName;

        @Override
        public synchronized void action(String name) {
            newLine();
            out.print("Action " + name);
            currentName = name;
            currentLine = CurrentLine.NAME;
        }

        /**
         * Prints the beginning of the named outcome.
         */
        @Override
        public synchronized void outcome(String name) {
            // if the outcome and action names are the same, omit the outcome name
            if (name.equals(currentName)) {
                return;
            }

            currentName = name;
            newLine();
            out.print(indent + name);
            currentLine = CurrentLine.NAME;
        }

        @Override
        public synchronized void streamOutput(String outcomeName, String output) {
            streamOutput(output);
        }
    }

    /**
     * This console buffers output, only printing when a result is found. It
     * supports multiple concurrent actions.
     */
    static class MultiplexingConsole extends Console {
        private final Map<String, StringBuilder> bufferedOutputByOutcome = new HashMap<String, StringBuilder>();

        @Override
        public synchronized void streamOutput(String outcomeName, String output) {
            StringBuilder buffer = bufferedOutputByOutcome.get(outcomeName);
            if (buffer == null) {
                buffer = new StringBuilder();
                bufferedOutputByOutcome.put(outcomeName, buffer);
            }

            buffer.append(output);
        }

        @Override
        protected synchronized void flushBufferedOutput(String outcomeName) {
            newLine();
            out.print(indent + outcomeName);
            currentLine = CurrentLine.NAME;

            StringBuilder buffer = bufferedOutputByOutcome.remove(outcomeName);
            if (buffer != null) {
                streamOutput(buffer);
            }
        }
    }

    /** Lazy constructed Singleton */
    static class Installer {
        static final Console STREAMING      = new StreamingConsole();
        static final Console MULTIPLEXING   = new MultiplexingConsole();
    }
}
