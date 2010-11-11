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

import java.util.List;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class ProcessExecutableException extends RuntimeException {
    private static final long serialVersionUID = 0;
    
    private final List<String> args;
    private final List<String> outputLines;

    public ProcessExecutableException(List<String> args, List<String> outputLines) {
        super(prettyPrint(args, outputLines));
        this.args           = args;
        this.outputLines    = outputLines;
    }

    /**
     * @return the list of args passed to the failed process.
     */
    public List<String> getArgs() {
        return args;
    }

    /**
     * @return a list of output lines returned by failed process.
     */
    public List<String> getOutputLines() {
        return outputLines;
    }

    public static String prettyPrint(List<String> args, List<String> outputLines) {
        StringBuilder result = new StringBuilder();
        result.append("Process failed:");
        for (String arg : args) {
            result.append(" ").append(arg);
        }
        for (String outputLine : outputLines) {
            result.append("\n  ").append(outputLine);
        }
        return result.toString();
    }

}
