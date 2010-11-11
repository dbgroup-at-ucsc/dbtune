package edu.ucsc.satuning.util;

import java.util.Map;
import java.util.WeakHashMap;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Environment {
    private static final String PG_TESTS  = "pgtests";
    private static final String DB2_ADVIS = "db2Advis";
    private final Map<String, String> container;
    Environment(){
        container = new WeakHashMap<String, String>();
    }

    public static Environment defaultEnvironment(){
        final Environment env = new Environment();
        env.buildDirectoryPathPerKey(PG_TESTS);
        env.buildDirectoryPathPerKey(DB2_ADVIS);
        return env;
    }

    public String getPgDirectoryPath(){
        return container.get(PG_TESTS);
    }

    public String getDB2AdvisPath(){
        return container.get(DB2_ADVIS);
    }

    void buildDirectoryPathPerKey(String key){
        container.put(key, format(System.getProperty("user.dir")) + format(key));
    }
    
    private String format(String value){
        return value.endsWith("/") ? value : value + '/';
    }

    @Override
    public String toString() {
        return container.toString();
    }
}
