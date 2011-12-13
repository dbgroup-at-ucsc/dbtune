package edu.ucsc.dbtune.advisor.db2;

/**
 * Generates a recommendation according to the db2advis program
 * 
 * This is a typical invocation:
 * 
 *    db2advis -d karlsch -i workload.txt -m I -l -1 -k OFF -f -o recommendation.txt -a karlsch/****
 * 
 * Where
 *   -d karlsch = use database "karlsch"
 *   -i workload.txt = use queries in workload.txt
 *   -m I = recommend indexes
 *   -l -1 = no space budget
 *   -k OFF = no workload compression
 *   -f = drop previously existing simulated catalog tables (???)
 *   -o recommendation.xml = put recommendation into recommendation.xml
 *   -a karlsch/**** = username/password
 *   
 * The workload file must be an SQL file with semicolon-delimited queries 
 * We don't create an output xml file ... just read the db2advis output directly 
 */
//todo(Huascar) make it a stateful object rather than leaving it like a util class.
public class DB2Advisor
{
    /*
    private static final Pattern INDEX_HEADER_PATTERN    = Pattern.compile("^-- index\\[\\d+\\],\\s+(.+)MB");
    private static final Pattern INDEX_STATEMENT_PATTERN = Pattern.compile("^\\s*CREATE.+(IDX\\d*)\\\"");
    private static final Pattern START_INDEXES_PATTERN   = Pattern.compile("^-- LIST OF RECOMMENDED INDEXES");
    private static final Pattern END_INDEXES_PATTERN     = Pattern.compile("^-- RECOMMENDED EXISTING INDEXES");

    public static FileInfo createAdvisorFile(Connection conn, String advisorPath, int budget, File workloadFile) throws IOException, SQLException {
        final String cmd = getCmd(advisorPath, budget, workloadFile, false);
        final String cleanCmd = getCmd(advisorPath, budget, workloadFile, true);

        System.out.println("Running db2advis on " + workloadFile);
        System.out.println("command = " + cleanCmd);

        Process prcs = Runtime.getRuntime().exec(cmd);
        
        FileInfo info;
        InputStream in   = new BufferedInputStream(prcs.getInputStream());
        InputStream err  = new BufferedInputStream(prcs.getErrorStream());
        String errString = "";
        try {
            info        = new FileInfo(in);
            errString   = Files.readStream(err);
        } finally {
            in.close();
            err.close();
        }
        
        while (true) {
            try {
                prcs.waitFor();
                break;
            } catch (InterruptedException e) {
                System.out.println("InterruptedException"+ " Cause: " + e.toString());
            }
        }
        int rc = prcs.exitValue();
        if (rc != 0){
            throw new SQLException("db2advis returned code "+rc+"\n"+errString);
        }
        
        return info;
    }

    private static String getCmd(String advisorPath, int budget, File inFile, boolean clean)
    {
        throw new RuntimeException("not implemented yet"); // will fix later

        final String db   = Environment.getInstance().getDatabaseName();
        final String pw   = Environment.getInstance().getPassword();
        final String user = Environment.getInstance().getUsername();
        
        return advisorPath
               +" -d "+db
               +" -a "+user+"/"+(clean?"****":pw)
               +" -i "+inFile
               +" -l "+budget
               +" -m I -f -k OFF";
    }
    
    public static class FileInfo {
        List<IndexInfo> indexList;
        
        @SuppressWarnings("unused")
        private String output;
        
        private FileInfo(InputStream stream) throws IOException, SQLException {
            indexList = new ArrayList<IndexInfo>();
            output    = processFile(stream, indexList);
        }
        
        public Configuration getCandidates(Connection conn) throws SQLException {
            throw new SQLException("not implemented yet");
        }
        
        public int getMegabytes() {
            double total = 0;
            for (IndexInfo info : indexList) 
                total += info.megabytes;
            System.out.println("Total size = " + total);
            return (int) Math.round(total);
        }
        
        private static String processFile(InputStream stream, List<IndexInfo> indexList) throws IOException, SQLException { 
            List<String> lines = Files.getLines(stream); // splits the file into individual lines
            Iterator<String> iter   = lines.iterator();
            Matcher headerMatcher   = INDEX_HEADER_PATTERN.matcher("");
            Matcher startMatcher    = START_INDEXES_PATTERN.matcher("");
            Matcher endMatcher      = END_INDEXES_PATTERN.matcher("");
            Matcher createMatcher   = INDEX_STATEMENT_PATTERN.matcher("");
            while (iter.hasNext()) {
                String line = iter.next();
                startMatcher.reset(line);
                if (startMatcher.find())
                    break;
            }
            
            String str = "";
            while (iter.hasNext()) {
                String line = iter.next();
                endMatcher.reset(line);
                if (endMatcher.find())
                    break;
                else {
                    headerMatcher.reset(line);
                    if (headerMatcher.find()) {
                        createMatcher.reset(iter.next()); // advanced iterator! 
                        if (!createMatcher.find())
                            throw new SQLException("Unexpected advisor file format");
                        
                        String indexName = createMatcher.group(1);
                        double indexMegabytes = Double.parseDouble(headerMatcher.group(1));
                        indexList.add(new IndexInfo(indexName, indexMegabytes));
                        System.out.println(indexMegabytes + "\t" + indexName + "\t" + line);
                    }
                }
                str += line + "\n";
            }
            return str;
        }
    }
    
    private static class IndexInfo {
        String name;
        double megabytes;
        
        IndexInfo(String n, double m) {
            name = n;
            megabytes = m;
        }
    }
    */
}
