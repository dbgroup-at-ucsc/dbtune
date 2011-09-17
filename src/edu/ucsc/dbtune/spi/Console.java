package edu.ucsc.dbtune.spi.core;

import edu.ucsc.dbtune.util.Instances;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public abstract class Console implements Printer {
    private boolean useColor;
    private boolean ansi;
    private boolean verbose;
    protected String indent;
    protected CurrentLine currentLine = CurrentLine.NEW;
    protected final ClearScreenHistory out = new ClearScreenHistory(System.out);
    protected ClearScreenHistory.Mark currentVerboseMark;
    protected ClearScreenHistory.Mark currentStreamMark;
    protected List<String> cachedErrors;

    private Console() {}

    /**
     * @return
     *      a new Console that prints output as it's emitted.
     */
    public static Console streaming(){
        return Installer.STREAMING;
    }

    /**
     * @return
     *      a new Console that buffers output, only printing when a result is found.
     */
    public static Console multiplexing(){
        return Installer.MULTIPLEXING;
    }

    /**
     * begins streaming output for the named action.
     * @param name
     *      name of action
     */
    public void action(String name) {}

    /**
     * collects exceptions that have been thrown within a method block.
     * This collection of exceptions will be printed on the screen
     * when calling {@link #warnAll(String)}.
     *
     * @param cause
     *   exception that was fired.
     */
    public void catchError(Throwable cause){
        if(cachedErrors == null) {
            cachedErrors = Instances.newList();
        }

        cachedErrors.add(cause.getLocalizedMessage());
    }

    /**
     * colors message using a given color.
     * @param message
     *      message to be colored.
     * @param color
     *      desired color.
     * @return
     *      colored message.
     */
    protected String colorString(String message, Color color) {
        return useColor ? ("\u001b[" + color.getCode() + ";1m" + message + "\u001b[0m") : message;
    }


    @Override
    public synchronized void error(String message, Throwable throwable) {
        newLine();
        out.println(colorString("Error: " + message, Color.ERROR));
        throwable.printStackTrace(System.out);
    }

    public void error(String message){
        newLine();
        out.println(colorString("Error: " + message, Color.ERROR));
    }

    /**
     * hook to flush anything streamed via {@link #streamOutput}.
     * @param outcomeName
     *      outcome name
     */
    protected void flushBufferedOutput(String outcomeName) {}

    @Override
    public synchronized void info(String message) {
        newLine();
        out.println(colorString("Info: " + message, Color.INFO));
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

    /**
     * inserts a linebreak if necessary.
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
     * begins streaming output for the named outcome.
     * @param name
     *      name of outcome
     */
    public void outcome(String name) {}

    /**
     * sets the desired indent when printing on the screen.
     * @param indent
     *      desired indent.
     */
    public void setIndent(String indent) {
        this.indent = indent;
    }

    /**
     * appends the action output immediately to the stream when streaming is on,
     * or to a buffer when streaming is off. Buffered output will be held and
     * printed only if the outcome is unsuccessful.
     * @param outcomeName
     *      name of outcome
     * @param output
     *      output text
     */
    public abstract void streamOutput(String outcomeName, String output);

    /**
     * prints the action output with appropriate indentation.
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
     * sets the colors to be used when printing messages on the screen.
     * @param infoColor
     *      info messages color
     * @param warnColor
     *      warning messages color
     * @param failColor
     *      error messages color
     */
    public void setUseColor(int infoColor, int warnColor, int failColor) {
        enableColor();
        Color.INFO.setCode(infoColor);
        Color.WARN.setCode(warnColor);
        Color.ERROR.setCode(failColor);
        Color.LOG.setCode(34);
    }

    /**
     * disable colored output.
     */
    public void disableColor(){
        this.useColor = false;
    }

    /**
     * enable colored output.
     */
    public void enableColor(){
        this.useColor = true;
    }

    /**
     * use ANSI escape sequences to remove intermediate output?
     * @param ansi
     *    a flag that indicates if we are using ANSI escape sequences.
     */
    public void setAnsi(boolean ansi) {
        this.ansi = ansi;
    }

    /**
     * use verbose messages.
     * @param verbose
     *    a flag that indicates if we are printing verbose messages on the screen.
     */
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public synchronized void dot() {
        out.print(colorString(".", Color.ERROR));
    }

    @Override
    public synchronized void verbose(String message) {
        /*
         * terminal does't support overwriting output, so don't print
         * verbose message unless requested.
         */
        if (!verbose && !ansi) {
            return;
        }

        //  When writing verbose output in the middle of streamed output, keep
        //  the streamed mark location. That way we can remove the verbose output
        //  later without losing our position mid-line in the streamed output.
        ClearScreenHistory.Mark savedStreamMark = currentLine == CurrentLine.STREAMED_OUTPUT
                ? out.mark()
                : currentStreamMark;
        newLine();
        currentStreamMark = savedStreamMark;

        currentVerboseMark = out.mark();
        out.print(message);
        currentLine = CurrentLine.VERBOSE;
    }

    @Override
    public synchronized void warn(String message) {
        warn(message, Collections.<String>emptyList());
    }


    @Override
    public void log(String message) {
        newLine();
        out.println(colorString(message, Color.LOG));
    }

    @Override
    public void skip() {
        currentLine = CurrentLine.NAME;
        newLine();
    }

    /**
     * warns, and also puts a list of strings afterwards.
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

    /**
     * warns, and also puts a list of strings which represent
     * the cached exceptions during a method's execution.
     * @param message
     *      text to be displayed on screen.
     */
    public synchronized void warnAll(String message){
        warn(message, cachedErrors);
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
     * messages's colors.
     */
    private enum Color {
        INFO, ERROR, WARN, LOG;

        int code = 0;

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }
    }

    /**
     * This console prints output as it's emitted. It supports at most one
     * action at a time.
     */
    static class StreamingConsole extends Console {
        private String currentName;
        {
            setUseColor(32/*green*/, 33/*yellow*/, 31/*red*/);
            setAnsi(!"dumb".equals(System.getenv("TERM")));
            setIndent("  ");
            setVerbose(false);
            disableColor();
        }

        @Override
        public synchronized void action(String name) {
            newLine();
            out.print(colorString("Action: " + name, Color.LOG));
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
        {
            setUseColor(32/*green*/, 33/*yellow*/, 31/*red*/);
            setAnsi(!"dumb".equals(System.getenv("TERM")));
            setIndent("  ");
            setVerbose(false);
            disableColor();
        }

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

    /** Lazy constructed Singleton (thread-safe) */
    static class Installer {
        static final Console STREAMING      = new StreamingConsole();
        static final Console MULTIPLEXING   = new MultiplexingConsole();
    }
}
