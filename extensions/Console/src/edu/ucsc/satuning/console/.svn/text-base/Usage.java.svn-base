package edu.ucsc.satuning.console;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.Map;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface Usage {
    /**
     * highlight an array of options that you'd like to
     * display on the console.
     * @param options
     *      option names.
     */
    void highlight(String[] options);

    /**
     * described an option as long as this option has been set to be highlighted.
     * @param option
     *      option to be described
     * @param description
     *      description of option.
     */
    void describe(String[] option, String description);

    /**
     * check if an option is required (i.e., non null val).
     * @param option
     *      option name
     * @return
     *      {@code true} if the option is a required one. false otherwise.
     */
    boolean isRequired(String option);

    /**
     * reset any saved usage info.
     */
    void reset();

    /**
     * prints the API usage.
     */
    void print();

    /**
     * prints the API usage.
     * @param out
     *      the desired print stream.
     */
    void print(PrintStream out);
}
