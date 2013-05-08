package edu.ucsc.dbtune.seq.bip;

public class WorkloadLoaderSettings {
    //all data goes here
    public static String dataRoot = System.getProperty("user.home") + "/dbtune";
    //INUM space cache
    public static String cacheRoot = dataRoot + "/paper/cache";
    public static String jdbcUrl = "jdbc:db2://localhost:50001/";
    public static String username = "db2inst2";
    public static String password = "db2inst1admin";
    public static String workloadsDir = "resources/workloads/db2";
}
