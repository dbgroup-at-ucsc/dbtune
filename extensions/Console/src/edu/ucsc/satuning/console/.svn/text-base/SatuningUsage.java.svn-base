package edu.ucsc.satuning.console;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class SatuningUsage implements Usage {
    private final Map<OptionNames, String>  usage;
    private final Set<OptionNames>          requireSet;
    private final String                    title;

    SatuningUsage(String title){
        this.title      = title;
        usage           = new HashMap<OptionNames, String>();
        requireSet      = new HashSet<OptionNames>();
    }

    public SatuningUsage(){
        this("Satuning");
    }

    public void addRequired(OptionNames optNames){
        if(!requireSet.contains(optNames)){
            requireSet.add(optNames);
        }
    }

    @Override
    public void print(){
        print(System.out);
    }

    @Override
    public void highlight(String[] options) {
        addRequired(OptionNames.of(options));
    }

    @Override
    public void describe(String[] option, String description) {
        if(description == null) return;
        final OptionNames x = OptionNames.of(option);
        if(requireSet.contains(x)){
            // once this app is running, u cannot change descriptions
            if(!usage.containsKey(x)){
                usage.put(x, description);
            }
        }
    }

    @Override
    public void reset() {
        requireSet.clear();
        usage.clear();
    }


    public boolean isRequired(String option) {
        for(OptionNames each : requireSet){
            for(String eachName : each.names){
                if(eachName.equals(option)) return true;
            }
        }

        return false;
    }

    @Override
    public void print(PrintStream out) {
        final StringBuilder whatIsMissing = new StringBuilder();
        for(Iterator<OptionNames> optItr = requireSet.iterator(); optItr.hasNext();){
            final OptionNames eachRequiredOpt = optItr.next();
            final StringBuilder opts = new StringBuilder();
            for(Iterator<String> itr = Arrays.asList(eachRequiredOpt.names).iterator(); itr.hasNext();){
                opts.append(itr.next());
                if(itr.hasNext()){
                    opts.append(", ");
                }
            }
            whatIsMissing.append("[").append(opts.toString()).append("]").append(optItr.hasNext() ? ", " : " ");
        }

        if(whatIsMissing.toString().length() > 0){
            out.println(title + " option(s) " + whatIsMissing.toString() + "is/are required.");
        }        

        out.println("Usage: Satuning [options]... ");
        out.println();
        out.println("GENERAL OPTIONS");
        out.println();
        out.println("  --subdir <file|directory>: specify a subdirectory of " + System.getProperty("user.dir") + ".");
        out.println("      file: the path of a target file (e.g., output.txt or any other file with different extension)");
        out.println("      directory: the path of a target directory where you have certain files of interest");
        out.println("      Default is: " + System.getProperty("user.dir") + "/pgtests");
        out.println();
        out.println("  --dir: absolute path of experiment (alternative to 'subdir' option).");
        out.println("      Disable with --no-dir if you want no absolute path of experiment to be used.");
        out.println();
        out.println("  --stream: stream output as it is emitted.");        
        out.println();
        out.println("  --native-output: print out native output (prefixed with \"[native]\").");
        out.println();
        out.println("  --mode <type>: synonym for algorithm mode. ");
        out.println("      type: it could be one of WFIT, OPT, WFIT_OPT, BC, PROFILE_ONLINE, PROFILE_OFFLINE, ");
        out.println("      CANDGEN_OFFLINE, GOOD, BAD, NOVOTE, SLOW, SLOW_NOVOTE, AUTO.");
        out.println();
        out.println("  --hot: specify the max hot set size.");
        out.println("      Disable with --no-hot if you want no max hot set size to be used.");
        out.println();
        out.println("  --states <integer>: maximum number of states.");
        out.println("      Default is: 1 state");
        out.println();
        out.println("  --log: flag to do log post-processing.");
        out.println("      Disable with --no-log if you want no logging to be used.");
        out.println();
        out.println("  --lag <milliseconds>: lag time of the slow admin.");
        out.println("      Default is: 60000 milliseconds");
        out.println();
        out.println("  --dbms <dbms name>: add the .jar to both build and execute classpaths.");
        out.println("      dbms name: dbms to use, e.g., pg, ibm");
        out.println("      Default is: pg");
        out.println();
        out.println("  --workload <directory>: directory in which to find workloads");
        out.println("      configuration information, unless they've been put explicitly");
        out.println("      elsewhere.");
        out.println("      Default is: " + System.getProperty("user.dir") + "/workloads");
        out.println();
        out.println("  --tag-dir <directory>: directory in which to find tag information.");
        out.println("      Default is: " + System.getProperty("user.dir") + "/tags");
        out.println();
        out.println("  --tag <tag name>: creates a tag recording the arguments to this");
        out.println("      invocation of Satuning so that it can be rerun later.");
        out.println();
        out.println("  --run-tag <tag name>: runs Satuning with arguments as specified by the");
        out.println("      tag. Any arguments supplied for this run will override those");
        out.println("      supplied by the tag.");
        out.println();
        out.println("  --compare-to-tag <tag name>: compares the results of this run with");
        out.println("      the results saved when the tag was created. Defaults to the value");
        out.println("      of --run-tag if that argument is given.");
        out.println("  --tag-overwrite: allow --tag to overwrite an existing tag.");
        out.println();
        out.println("  --verbose: turn on persistent verbose output.");
        out.println();
        out.println("  --indent: amount to indent action result output. Can be set to ''");
        out.println("      (aka empty string) to simplify output parsing.");
        out.println("      Default is: '" + "  " + "'");

    }

    static class OptionNames {
        String[] names;

        static OptionNames of(String[] values){
            final OptionNames opn = new OptionNames();
            opn.names = values;
            return opn;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(names);
        }

        @Override
        public boolean equals(Object obj) {
            if(!(obj instanceof OptionNames)) return false;
            final OptionNames other = (OptionNames)obj;
            return names == other.names;
        }
    }
}
