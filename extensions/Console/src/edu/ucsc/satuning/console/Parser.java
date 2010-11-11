package edu.ucsc.satuning.console;

import java.util.List;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface Parser {
    /**
     * @return {@code true} if all the options were parsed correctly. false otherwise.
     */
    boolean allSucess();
    
    /**
     * parses the command-line arguments 'args', setting the @Option fields of the 'optionSource' provided to the constructor.
     * @param args
     *      command-line arguments.
     * @return
     *      a list of the positional arguments (targets) left over after processing all options.
     */
    List<String> parse(String... args);

    void printUsage();
}
