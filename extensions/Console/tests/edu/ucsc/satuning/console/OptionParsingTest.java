package edu.ucsc.satuning.console;

import org.junit.Test;

import java.io.File;

import static junit.framework.Assert.assertSame;
import static org.junit.Assert.assertFalse;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class OptionParsingTest {

    @Test
    public void testBasicScenario() throws Exception {
        Options       options = new Options();
        OptionParser  parser  = new OptionParser(options, new SatuningUsage());
        final String[] args = new String[]{"--no-dir","--mode", "OPT"};
        parser.parse(args);

        assertFalse("should be false", options.dir);
        assertSame("should be equal", options.mode, "OPT");
        parser.reset();
    }

    @SuppressWarnings({"UnusedDeclaration"})
    static class Options {
        @Option(values = {"--root"})
        private final File root = new File(System.getProperty("user.dir"));

        // $ points to an existing option.
        @Describe("subdirectory of $root")
        @Option(values = {"--subdir"})
        private File subDir = new File(System.getProperty("user.dir"));

        @Option(values = {"--dir" })
        private Boolean dir = true;

        @Option(values = {"--mode" })
        private String mode = "WFIT";

        @Describe("max hot set size")
        @Option(values = {"--hot" })
        private Boolean hot = true;

        @Describe("max number of states")
        @Option(values = {"--states" })
        private Integer states = 1;

        @Option(values = {"--log" })
        private boolean log = true;

        @Describe("lag of the slow admin")
        @Option(values = {"--lag" })
        private Boolean lag = true;

        @Describe("dbms to use, e.g., pg, ibm")
        @Option(values = {"--dbms" })
        private String dbms = "pg";

        @Option(values = {"--workload" })
        private String workload = "home/huascarsanchez";
    }

}
