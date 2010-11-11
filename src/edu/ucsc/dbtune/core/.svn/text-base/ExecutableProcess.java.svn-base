/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */
package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.spi.core.Supplier;
import edu.ucsc.dbtune.util.Instances;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public final class ExecutableProcess {
    private final List<String>          args;
    private final Map<String, String>   env;
    private final File                  workingDirectory;
    private final boolean               permitNonZeroExitStatus;
    private final PrintStream           tee;
    private final boolean               nativeOutput;
    private volatile Process            process;

    public ExecutableProcess(String... args) {
        this(Arrays.asList(args));
    }

    public ExecutableProcess(List<String> args) {
        this.args                    = new ArrayList<String>(args);
        this.env                     = Collections.emptyMap();
        this.workingDirectory        = null;
        this.permitNonZeroExitStatus = false;
        this.tee                     = null;
        this.nativeOutput            = false;
    }

    private ExecutableProcess(Builder builder) {
        this.args                    = new ArrayList<String>(builder.args);
        this.env                     = builder.env;
        this.workingDirectory        = builder.workingDirectory;
        this.permitNonZeroExitStatus = builder.permitNonZeroExitStatus;
        this.tee                     = builder.tee;
        if (builder.maxLength != -1) {
            String string = toString();
            if (string.length() > builder.maxLength) {
                throw new IllegalStateException("Maximum command length " + builder.maxLength
                                                + " exceeded by: " + string);
            }
        }
        this.nativeOutput = builder.nativeOutput;
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
            if (nativeOutput) {
                System.out.println(outputLine);
            }
            outputLines.add(outputLine);
        }

        if (process.waitFor() != 0 && !permitNonZeroExitStatus) {
            StringBuilder message = new StringBuilder();
            for (String line : outputLines) {
                message.append("\n").append(line);
            }
            throw new ProcessExecutableException(args, outputLines);
        }

        return outputLines;
    }

    public boolean isStarted() {
        return process != null;
    }

    public static String join(Iterable<?> objects, String delimiter) {
        Iterator<?> i = objects.iterator();
        if (!i.hasNext()) {
            return "";
        }

        StringBuilder result = new StringBuilder();
        result.append(i.next());
        while(i.hasNext()) {
            result.append(delimiter).append(i.next());
        }
        return result.toString();
    }

    public void start() throws IOException {
        if (isStarted()) {
            throw new IllegalStateException("Already started!");
        }

        System.out.println("executing " + this);

        ProcessBuilder processBuilder = new ProcessBuilder()
                .command(args)
                .redirectErrorStream(true);
        if (workingDirectory != null) {
            processBuilder.directory(workingDirectory);
        }

        processBuilder.environment().putAll(env);

        process = processBuilder.start();
    }

    @Override
    public String toString() {
        String envString = !env.isEmpty() ? (join(env.entrySet(), " ") + " ") : "";
        return envString + join(args, " ");
    }
    

    public static class Builder implements Supplier<ExecutableProcess> {
        private final List<String> args = new ArrayList<String>();
        private final Map<String, String> env = Instances.newLinkedHashMap();
        private File workingDirectory;
        private boolean permitNonZeroExitStatus = false;
        private PrintStream tee = null;
        private boolean nativeOutput;
        private int maxLength = -1;

        public Builder args(Object... objects) {
            for (Object object : objects) {
                args(object.toString());
            }
            return this;
        }

        public Builder setNativeOutput(boolean nativeOutput) {
            this.nativeOutput = nativeOutput;
            return this;
        }

        public Builder args(String... args) {
            return args(Arrays.asList(args));
        }

        public Builder args(Collection<String> args) {
            this.args.addAll(args);
            return this;
        }

        public Builder env(String key, String value) {
            env.put(key, value);
            return this;
        }

        /**
         * Sets the working directory from which the process will be executed.
         * This must be a <strong>local</strong> directory; Processes run on
         * remote devices (ie. via {@code adb shell}) require a local working
         * directory.
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

        public ExecutableProcess get() {
            return new ExecutableProcess(this);
        }

        public List<String> execute() {
            return get().execute();
        }
    }

}
