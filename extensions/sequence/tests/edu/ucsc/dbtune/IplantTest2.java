package edu.ucsc.dbtune;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

import java.io.File;
import java.io.PrintStream;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.seq.bip.WorkloadLoaderSettings;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.Workload;

public class IplantTest2 {
    static String[] tableNames = { "r_coll_main", "r_data_main", "r_meta_main",
            "r_microsrvc_main", "r_microsrvc_ver", "r_objt_access",
            "r_objt_audit", "r_objt_deny_access", "r_objt_metamap",
            "r_quota_main", "r_quota_usage", "r_resc_group", "r_resc_main",
            "r_rule_base_map", "r_rule_dvm", "r_rule_dvm_map", "r_rule_exec",
            "r_rule_fnm", "r_rule_fnm_map", "r_rule_main", "r_server_load",
            "r_server_load_digest", "r_specific_query",
            "r_ticket_allowed_groups", "r_ticket_allowed_hosts",
            "r_ticket_allowed_users", "r_ticket_main", "r_tokn_main",
            "r_user_auth", "r_user_group", "r_user_main", "r_user_password",
            "r_user_session_key", "r_zone_main", "rcore_attributes",
            "rcore_fk_relations", "rcore_schemas", "rcore_tables",
            "rcore_uschema_attr", "rcore_user_schemas", };

    static Vector<String> split(String s) {
        Vector<String> v = new Vector<String>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'') {
                sb.append(c);
                for (int j = i + 1; j < s.length(); j++) {
                    c = s.charAt(j);
                    sb.append(c);
                    if (c == '\'') {
                        if (j + 1 == s.length() || s.charAt(j + 1) != '\'') {
                            i = j;
                            break;
                        } else {
                            sb.append(s.charAt(j + 1));
                            j++;
                        }
                    }
                }
            } else if (c == ';') {
                v.add(sb.toString());
                sb = new StringBuilder();
            } else {
                sb.append(c);
            }
        }
        if (sb.length() > 0)
            v.add(sb.toString());
        return v;
    }

    static void covertLog() throws Exception {
        File dir = new File(WorkloadLoaderSettings.dataRoot + "/iplant/logs");
        String[] ss = { "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", };
        int total = 0;
        int update = 0;
        int select = 0;
        int insert = 0;
        int delete = 0;
        int copy = 0;
        int noDup = 0;
        HashSet<String> hash = new HashSet<String>();
        PrintStream workload = new PrintStream(
                "/home/wangrui/workspace/bipBranch/resources/workloads/db2/iplant/workload.sql");
        for (String name : ss) {
            int n = 0;
            PrintStream ps = new PrintStream(
                    "/home/wangrui/workspace/bipBranch/resources/workloads/db2/iplant/"
                            + name + ".sql");
            String[] lines = Rt.readFileAsLines(new File(dir, "postgresql-"
                    + name + ".log"));
            for (int i = 0; i < lines.length; i++) {
                String line = lines[i];
                if (line.startsWith("WARNING:"))
                    continue;
                if (line.startsWith("ERROR:"))
                    continue;
                if (line.startsWith("DETAIL:"))
                    continue;
                if (line.startsWith("HINT:"))
                    continue;
                if (line
                        .startsWith("LOG:  unexpected EOF on client connection"))
                    continue;
                int t = line.indexOf("statement:");
                if (t < 0)
                    t = line.indexOf("STATEMENT:");
                if (t < 0)
                    throw new Error(line);
                line = line.substring(t + "statement:".length()).trim();
                for (int j = i + 1; j < lines.length; j++) {
                    if (lines[j].startsWith("\t")) {
                        // Rt.np(lines[j]);
                        line += "\n" + lines[j].substring(1);
                        i = j;
                    } else {
                        break;
                    }
                }
                for (String s : split(line)) {
                    s = s.trim();
                    if (s.length() == 0)
                        continue;
                    s = s.replaceAll(Pattern.quote("currval('R_ObjectID')"),
                            "previous value for iplant.R_ObjectID");
                    s = s.replaceAll(Pattern.quote("\"public\"."), "");
                    s = s.replaceAll(Pattern.quote("\"iplant.r_ticket_main\""),
                            "iplant.r_ticket_main");
                    s = s
                            .replaceAll(Pattern
                                    .quote("select nextval('R_ObjectID')"),
                                    "select next value for iplant.R_ObjectID from iplant.rcore_attributes");
                    s = s.replaceAll(Pattern.quote("nextval('R_ObjectID')"),
                            "next value for iplant.R_ObjectID");
                    for (String tableName : tableNames) {
                        s = s.replaceAll(tableName.toUpperCase(), "iplant."
                                + tableName.toUpperCase());
                        s = s.replaceAll(tableName.toLowerCase(), "iplant."
                                + tableName.toLowerCase());
                    }
                    if (s.equalsIgnoreCase("commit") || s.startsWith("BEGIN")
                            || s.startsWith("SET"))
                        s = "-- " + s;
                    else if (s.contains("pg_stat_")
                            || s.contains("pg_database")) {
                        Rt.np(s);
                        continue;
                    } else {
                        n++;
                        if (s.toLowerCase().startsWith("select"))
                            select++;
                        else if (s.toLowerCase().startsWith("update"))
                            update++;
                        else if (s.toLowerCase().startsWith("insert"))
                            insert++;
                        else if (s.toLowerCase().startsWith("delete"))
                            delete++;
                        else if (s.toLowerCase().startsWith("copy"))
                            copy++;
                        else
                            Rt.np(s);
                        if (!hash.contains(s)) {
                            hash.add(s);
                            noDup++;
                            if (!s.toLowerCase().startsWith("update")
                                    && !s.toLowerCase().startsWith("copy")
                                    && !s.startsWith("select * from \"iplant."))
                                workload.println(s + ";");
                        }
                    }
                    if (!s.toLowerCase().startsWith("copy"))
                        ps.println(s + ";");
                }
            }
            ps.close();
            Rt.np(name + " " + n);
            total += n;
        }
        workload.close();
        Rt.np("select=" + select);
        Rt.np("update=" + update);
        Rt.np("insert=" + insert);
        Rt.np("delete=" + delete);
        Rt.np("copy=" + copy);
        Rt.np("total=" + total);
        Rt.np("noDup=" + noDup);
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        // for (String s : split("abc'abc;''';bcd'';def")) {
        // Rt.np(s);
        // }
        covertLog();
        Environment en = Environment.getInstance();
        en.setProperty("username", "db2inst1");
        en.setProperty("password", "db2inst1admin");
        en.setProperty("workloads.dir", "resources/workloads/db2");
        DatabaseSystem db;
        String dbName = "iplant";
        en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
        db = newDatabaseSystem(en);

        String query = "update iplant.R_COLL_MAIN set coll_inheritance='1', modify_ts='01338793200' where coll_id= previous value for iplant.R_ObjectID;";
        int queryId = 0;
        Workload workload = new Workload("", new StringReader(query));
        String sql = workload.get(queryId).getSQL();
        Rt.np("SQL:");
        Rt.p(sql);

        Set<Index> indexes = new HashSet<Index>();
        CandidateGenerator candGen = new OptimizerCandidateGenerator(
                getBaseOptimizer(db.getOptimizer()));
        indexes = candGen.generate(workload);

        InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();
        ExplainedSQLStatement db2plan = db2optimizer.explain(sql, indexes);
        Rt.np("DB2 plan:");
        Rt.p(db2plan.getPlan());
    }
}
