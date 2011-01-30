/****************************************************************************
 * Copyright 2010 Huascar A. Sanchez                                        *
 *                                                                          *
 * Licensed under the Apache License, Version 2.0 (the "License");          *
 * you may not use this file except in compliance with the License.         *
 * You may obtain a copy of the License at                                  *
 *                                                                          *
 *     http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                          *
 * Unless required by applicable law or agreed to in writing, software      *
 * distributed under the License is distributed on an "AS IS" BASIS,        *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 * See the License for the specific language governing permissions and      *
 * limitations under the License.                                           *
 ****************************************************************************/
package edu.ucsc.dbtune.spi.core;

import edu.ucsc.dbtune.util.Strings;
import edu.ucsc.dbtune.util.Threads;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * An out of process executable.
 */
public final class Command {
    private final Printer log;
    private final List<String> args;
    private final Map<String, String> env;
    private final File workingDirectory;
    private final boolean permitNonZeroExitStatus;
    private final PrintStream tee;
    private volatile Process process;

    public Command(Printer log, String... args) {
        this(log, Arrays.asList(args));
    }

    public Command(Printer log, List<String> args) {
        this.log = log;
        this.args = new ArrayList<String>(args);
        this.env = Collections.emptyMap();
        this.workingDirectory = null;
        this.permitNonZeroExitStatus = false;
        this.tee = null;
    }

    private Command(Builder builder) {
        this.log = builder.log;
        this.args = new ArrayList<String>(builder.args);
        this.env = builder.env;
        this.workingDirectory = builder.workingDirectory;
        this.permitNonZeroExitStatus = builder.permitNonZeroExitStatus;
        this.tee = builder.tee;
        if (builder.maxLength != -1) {
            String string = toString();
            if (string.length() > builder.maxLength) {
                throw new IllegalStateException("Maximum command length " + builder.maxLength
                                                + " exceeded by: " + string);
            }
        }
    }

    public void start() throws IOException {
        if (isStarted()) {
            throw new IllegalStateException("Already started!");
        }

        log.verbose("executing " + this);

        ProcessBuilder processBuilder = new ProcessBuilder()
                .command(args)
                .redirectErrorStream(true);
        if (workingDirectory != null) {
            processBuilder.directory(workingDirectory);
        }

        processBuilder.environment().putAll(env);

        process = processBuilder.start();
    }

    public boolean isStarted() {
        return process != null;
    }

    public InputStream getInputStream() {
        if (!isStarted()) {
            throw new IllegalStateException("Not started!");
        }

        return process.getInputStream();
    }

    public List<String> gatherOutput()
            throws IOException, InterruptedException {
        if (!isStarted()) {
            throw new IllegalStateException("Not started!");
        }

        BufferedReader in = new BufferedReader(
                new InputStreamReader(getInputStream(), "UTF-8"));
        List<String> outputLines = new ArrayList<String>();
        String outputLine;
        while ((outputLine = in.readLine()) != null) {
            if (tee != null) {
                tee.println(outputLine);
            }
            outputLines.add(outputLine);
        }

        if (process.waitFor() != 0 && !permitNonZeroExitStatus) {
            StringBuilder message = new StringBuilder();
            for (String line : outputLines) {
                message.append("\n").append(line);
            }
            throw new CommandFailedException(args, outputLines);
        }

        return outputLines;
    }

    public List<String> execute() {
        try {
            start();
            return gatherOutput();
        } catch (IOException e) {
            throw new RuntimeException("Failed to execute process: " + args, e);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while executing process: " + args, e);
        }
    }

    /**
     * Executes a command with a specified timeout. If the process does not
     * complete normally before the timeout has elapsed, it will be destroyed.
     *
     * @param timeoutSeconds how long to wait, or 0 to wait indefinitely
     * @return the command's output, or null if the command timed out
     * @throws java.util.concurrent.TimeoutException
     *      unable to execute with timeout due to the stated reasons.
     */
    public List<String> executeWithTimeout(int timeoutSeconds)
            throws TimeoutException {
        if (timeoutSeconds == 0) {
            return execute();
        }

        try {
            return executeLater().get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while executing process: " + args, e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            destroy();
        }
    }

    /**
     * Executes the command on a new background thread. This method returns
     * immediately.
     *
     * @return a future to retrieve the command's output.
     */
    public Future<List<String>> executeLater() {
        ExecutorService executor = Threads.fixedThreadsExecutor("command", 1, true, log);
        Future<List<String>> result = executor.submit(new Callable<List<String>>() {
            public List<String> call() throws Exception {
                start();
                return gatherOutput();
            }
        });
        executor.shutdown();
        return result;
    }

    /**
     * Destroys the underlying process and closes its associated streams.
     */
    public void destroy() {
        if (process == null) {
            return;
        }

        process.destroy();
        try {
            process.waitFor();
            int exitValue = process.exitValue();
            log.verbose("received exit value " + exitValue + " from destroyed command " + this);
        } catch (IllegalThreadStateException destroyUnsuccessful) {
            log.warn("couldn't destroy " + this);
        } catch (InterruptedException e) {
            log.warn("couldn't destroy " + this);
        }
    }

    @Override public String toString() {
        String envString = !env.isEmpty() ? (Strings.join(env.entrySet(), " ") + " ") : "";
        return envString + Strings.join(args, " ");
    }

    public static class Builder {
        private final Printer log;
        private final List<String> args = new ArrayList<String>();
        private final Map<String, String> env = new LinkedHashMap<String, String>();
        private File workingDirectory;
        private boolean permitNonZeroExitStatus = false;
        private PrintStream tee = null;
        private int maxLength = -1;

        public Builder(Printer log) {
            this.log = log;
        }

        public Builder args(Object... args) {
            return args(Arrays.asList(args));
        }

        public Builder args(Collection<?> args) {
            for (Object object : args) {
                this.args.add(object.toString());
            }
            return this;
        }

        public Builder env(String key, String value) {
            env.put(key, value);
            return this;
        }

        /**
         * Sets the working directory from which the command will be executed.
         * This must be a <strong>local</strong> directory; Commands run on
         * remote devices (ie. via {@code adb shell}) require a local working
         * directory.
         * @param workingDirectory new working directory
         * @return  builder
         */
        public Builder workingDirectory(File workingDirectory) {
            this.workingDirectory = workingDirectory;
            return this;
        }

        public Builder tee(PrintStream printStream) {
            tee = printStream;
            return this;
        }

        public Builder maxLength(int maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        public Command build() {
            return new Command(this);
        }

        public List<String> execute() {
            return build().execute();
        }
    }
}
