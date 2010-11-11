package edu.ucsc.satuning;

import edu.ucsc.satuning.console.Console;
import edu.ucsc.satuning.console.Describe;
import edu.ucsc.satuning.console.Option;
import edu.ucsc.satuning.console.OptionParser;
import edu.ucsc.satuning.console.SatuningUsage;
import edu.ucsc.satuning.console.Usage;

import java.io.File;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Satuning {
    private final Options       options;
    private final OptionParser  parser;

    /**
     * run Satuning console.
     * @param args
     *      program arguments.
     * @throws Exception
     *      unexpected error has occurred.
     */
    public static void main(String[] args) throws Exception {        
        final Satuning satuning = new Satuning(new Options(), new SatuningUsage());
        if(!satuning.parseArgs(args)){
            System.out.println("Hy");
            satuning.printUsage();
            return;
        }

        boolean allSuccess = satuning.run();
        System.exit(allSuccess ? 0 : 1);
    }

    Satuning(Options options, Usage usage){
        this.options    = options;
        parser          = new OptionParser(options, usage);
    }

    private void printUsage() {
        parser.printUsage();
    }

    private boolean parseArgs(String[] args) {
        parser.parse(args);
        return parser.allSucess();
    }


    private boolean run() throws Exception {
        System.out.println(System.getProperty("java.class.path"));
        Console.init(options.stream);
        Console.getInstance().setUseColor(options.color, options.passColor, options.warnColor, options.failColor);
        Console.getInstance().setVerbose(options.verbose);
        Console.getInstance().setIndent(options.indent);
        final SatuningService driver = new SatuningService();
        return driver.buildAndRun(options.log, Mode.WFIT);
    }

    /**
     *  Set of of options needed to run Satuning API.
     */
    @SuppressWarnings({"UnusedDeclaration"})
    static class Options {
        @Option(values = { "--color" })
        private boolean color = true;

        @Option(values = { "--compare-to-tag" }, required = false, savedInTag = false)
        private String compareToTag = null;

        @Option(values = {"--dbms" })
        private String dbms = "pg";

        @Describe("absolute path of experiment (alternative to 'subdir' option)")
        @Option(values = {"--dir" })
        private Boolean dir = true;

        @Option(values = { "--fail-color" })
        private int failColor = 31; // red

        @Option(values = {"--hot" })
        private Boolean hot = true;

        @Option(values = { "--indent" })
        private String indent = "  ";        

        @Describe("lag of the slow admin")
        @Option(values = {"--lag" })
        private int lag = 60000;

        @Option(values = {"--log" }, required = false)
        private boolean log = true;

        @Option(values = {"--mode" })
        private String mode = "WFIT";

        @Option(values = { "--run-tag" }, required = false, savedInTag = false)
        private String runTag = null;

        @Option(values = { "--stream" })
        private boolean stream = true;

        // $ points to an existing option.
        @Describe("subdirectory of $root")
        @Option(values = {"--subdir"})
        private File subDir = new File(System.getProperty("user.dir"));

        @Option(values = {"--states" })
        private Integer states = 1;

        @Option(values = { "--pass-color" })
        private int passColor = 32; // green

        @Option(values = { "--tag-dir" })
        private File tagDir = new File(System.getProperty("user.dir") + "/tags");

        @Option(values = { "--tag" }, required = false, savedInTag = false)
        private String tagName = null;

        @Option(values = { "--tag-overwrite" }, savedInTag = false)
        private boolean tagOverwrite = false;

        @Option(values = { "--verbose" })
        private boolean verbose;

        @Option(values = { "--warn-color" })
        private int warnColor = 33; // yellow

        @Option(values = {"--workload" })
        private String workload = System.getProperty("user.dir") + "/workloads";

        public boolean isLogginEnabled(){
            return log;
        }
    }
}
