package edu.ucsc.satuning.console;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public abstract class Console {
    private static Console NULL_CONSOLE = new Console() {
        @Override public void streamOutput(String outcomeName, String output) {
            throw new IllegalStateException("Call Console.init() first");
        }
    };

    private boolean         useColor;
    private boolean         verbose;
    protected String        indent;
    protected CurrentLine   currentLine = CurrentLine.NEW;

    private Console() {}


    public static void init(boolean streaming) {
        Installer.INSTANCE = streaming ? new StreamingConsole() : new MultiplexingConsole();
    }

    public static Console getInstance(){
        return Installer.INSTANCE;
    }

    /**
     * Begins streaming output for the named action.
     */
    public void action(String name) {}
    
    

    protected String colorString(String message, Color color) {
        return useColor ? ("\u001b[" + color.getCode() + ";1m" + message + "\u001b[0m") : message;
    }

    private void eraseCurrentLine() {
        System.out.print(useColor ? "\u001b[2K\r" : "\n");
        System.out.flush();
    }


    public synchronized void info(String s) {
        newLine();
        System.out.println(s);
    }

    /**
     * Hook to flush anything streamed via {@link #streamOutput}.
     * @param outcomeName
     *      name of outcome stream
     */
    protected void flushBufferedOutput(String outcomeName) {}
    
    private String generateSparkLine(List<ResultValue> resultValues) {
        StringBuilder sb = new StringBuilder();
        for (ResultValue resultValue : resultValues) {
            if (resultValue == ResultValue.OK) {
                sb.append(colorString("\u2713", Color.PASS));
            } else if (resultValue == ResultValue.FAIL) {
                sb.append(colorString("X", Color.FAIL));
            } else {
                sb.append(colorString("-", Color.WARN));
            }
        }
        return sb.toString();
    }

    
    public synchronized void info(String message, Throwable throwable) {
        newLine();
        System.out.println(message);
        throwable.printStackTrace(System.out);
    }
    

    /**
     * Returns an array containing the lines of the given text.
     */
    private String[] messageToLines(String message) {
        // pass Integer.MAX_VALUE so split doesn't trim trailing empty strings.
        return message.split("\r\n|\r|\n", Integer.MAX_VALUE);
    }


    public synchronized void nativeOutput(String s) {
        info("[native] " + s);
    }

    /**
     * Inserts a linebreak if necessary.
     */
    protected void newLine() {
        if (currentLine == CurrentLine.NEW) {
            return;
        } else if (currentLine == CurrentLine.VERBOSE) {
            // --verbose means "leave all the verbose output on the screen".
            if (!verbose) {
                // Otherwise we overwrite verbose output whenever something new arrives.
                eraseCurrentLine();
                currentLine = CurrentLine.NEW;
                return;
            }
        }

        System.out.println();
        currentLine = CurrentLine.NEW;
    }

    /**
     * Begins streaming output for the named outcome.
     */
    public void outcome(String name) {}


    /**
     * Prints the action output with appropriate indentation.
     */
    protected void printOutput(CharSequence streamedOutput) {
        if (streamedOutput.length() == 0) {
            return;
        }

        String[] lines = messageToLines(streamedOutput.toString());

        if (currentLine != CurrentLine.STREAMED_OUTPUT) {
            newLine();
            System.out.print(indent);
            System.out.print(indent);
        }
        System.out.print(lines[0]);
        currentLine = CurrentLine.STREAMED_OUTPUT;

        for (int i = 1; i < lines.length; i++) {
            newLine();

            if (lines[i].length() > 0) {
                System.out.print(indent);
                System.out.print(indent);
                System.out.print(lines[i]);
                currentLine = CurrentLine.STREAMED_OUTPUT;
            }
        }
    }

    /**
     * Writes the action's outcome.
     */
    public synchronized void printResult(String outcomeName, Result result, ResultValue resultValue) {
        if (result != Result.SUCCESS || resultValue != ResultValue.OK) {
            streamOutput(outcomeName, "\n" + colorString("testing this feature", Color.COMMENT));
        }
        
        flushBufferedOutput(outcomeName);
        if (currentLine == CurrentLine.NAME) {
            System.out.print(" ");
        } else {
            System.out.print("\n" + indent + outcomeName + " ");
        }

        if (resultValue == ResultValue.OK) {
            System.out.println(colorString("OK (" + result + ")", Color.PASS));
        } else if (resultValue == ResultValue.FAIL) {
            System.out.println(colorString("FAIL (" + result + ")", Color.FAIL));
        } else if (resultValue == ResultValue.IGNORE) {
            System.out.println(colorString("SKIP (" + result + ")", Color.WARN));
        }

        currentLine = CurrentLine.NEW;
    }

    public void setIndent(String indent) {
        this.indent = indent;
    }

    public void setUseColor(boolean useColor, int passColor, int warnColor, int failColor) {
        this.useColor = useColor;
        Color.PASS.setCode(passColor);
        Color.WARN.setCode(warnColor);
        Color.FAIL.setCode(failColor);
        Color.COMMENT.setCode(34);
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }
    
    /**
     * Appends the action output immediately to the stream when streaming is on,
     * or to a buffer when streaming is off. Buffered output will be held and
     * printed only if the outcome is unsuccessful.
     */
    public abstract void streamOutput(String outcomeName, String output);

    public synchronized void verbose(String s) {
        newLine();
        System.out.print(s);
        System.out.flush();
        currentLine = CurrentLine.VERBOSE;
    }

    /**
     * Warns, and also puts a list of strings afterwards.
     */
    public synchronized void warn(String message, List<String> list) {
        newLine();
        System.out.println(colorString("Warning: " + message, Color.WARN));
        for (String item : list) {
            System.out.println(colorString(indent + item, Color.WARN));
        }
    }

    public synchronized void warn(String message) {
        newLine();
        System.out.println(colorString("Warning: " + message, Color.WARN));
    }
    
    /**
     * Instance Installer.
     */
    private static class Installer {
        private static Console INSTANCE = NULL_CONSOLE;
        private Installer(){}
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
        VERBOSE
    }

    /**
     *  Color of a currently-done line of output.
     */
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

    /**
     * This console prints output as it's emitted. It supports at most one
     * action at a time.
     */
    private static class StreamingConsole extends Console {
        private String currentName;

        @Override
        public synchronized void action(String name) {
            newLine();
            System.out.print("Action " + name);
            System.out.flush();
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
            System.out.print(indent + name);
            System.out.flush();
            currentLine = CurrentLine.NAME;
        }

        @Override
        public synchronized void streamOutput(String outcomeName, String output) {
            printOutput(output);
        }
    }

    /**
     * This console buffers output, only printing when a result is found. It
     * supports multiple concurrent actions.
     */
    private static class MultiplexingConsole extends Console {
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
            System.out.print(indent + outcomeName);
            currentLine = CurrentLine.NAME;

            StringBuilder buffer = bufferedOutputByOutcome.remove(outcomeName);
            if (buffer != null) {
                printOutput(buffer);
            }
        }

    }
}
