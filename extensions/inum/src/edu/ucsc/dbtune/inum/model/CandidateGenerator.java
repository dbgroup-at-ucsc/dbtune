package edu.ucsc.dbtune.inum.model;

import Zql.ParseException;
import Zql.ZQuery;
import Zql.ZUtils;
import Zql.ZqlParser;
import com.google.common.collect.Multimap;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.Vector;

/**
 * Created by IntelliJ IDEA.
 * User: Debabrata Dash
 * Date: Feb 23, 2006
 * Time: 8:13:23 PM
 * The file is a copyright of CMU, all rights reserved.
 */
public class CandidateGenerator implements Serializable {
    private Vector sqlStatements;
    public ArrayList query_descriptors;
    public ArrayList candidates;
    public ArrayList candidatesPerQuery;
    public HashSet indexMemory;
    public PhysicalConfiguration universe;
    public HashSet perQueryHash;

    //public HashMap indexSizes;

    public CandidateGenerator(File file) throws ParseException {
        ZqlParser parser = null;
        ZUtils.addCustomFunction("to_char",2);
        ZUtils.addCustomFunction("datepart", 2);
        ZUtils.addCustomFunction("dateadd", 3);
        ZUtils.addCustomFunction("substring", 3);
        ZUtils.addCustomFunction("power",2);
        ZUtils.addCustomFunction("round",2);
        ZUtils.addCustomFunction("cast",1);
        ZUtils.addCustomFunction("abs",1);
        ZUtils.addCustomFunction("cos",1);
        ZUtils.addCustomFunction("sqrt",1);
        ZUtils.addCustomFunction("Log10",1);

        try {
            parser = new ZqlParser(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            assert false : e.getMessage();
        }
        query_descriptors = new ArrayList();
        sqlStatements = parser.readStatements();
        candidates = new ArrayList();
        indexMemory = new HashSet();
        candidatesPerQuery = new ArrayList();
        perQueryHash = new HashSet();
    }

    public void getInterestingOrders() {
        for (int i = 0; i < sqlStatements.size(); i++) {
            ZQuery query = (ZQuery) sqlStatements.elementAt(i);
            QueryDesc QD = new QueryDesc();
            QD.parsed_query = query;
            //save the query info ...
            query_descriptors.add(QD);
            // System.out.println("query.toString() = " + query.toString());
            ColumnsGatherer gen = new ColumnsGatherer(query);

            //process interesting orders from the parsed representation
            //Set interesting_columns = gen.getInterestingOrders();
            //Set used_columns = gen.getUsedColumns();

            QD.group_by = new PhysicalConfiguration(gen.getGroupByOrders());
            QD.used = new PhysicalConfiguration(gen.associateColumnsWithTables(gen.getUsedColumns()));


            if (QD.group_by == null)
                QD.group_by = new PhysicalConfiguration(gen.getOrderByOrders());
            else
                QD.group_by.addConfiguration(new PhysicalConfiguration(gen.getOrderByOrders()));

            QD.interesting_orders = new PhysicalConfiguration(gen.associateColumnsWithTables(gen.getInterestingOrders()));
            //printout results for inspection
            if (QD.group_by != null) {
                QD.interesting_orders.addConfiguration(QD.group_by);
            }

            Set tables = gen.getQueryTables();
            for (Iterator iterator = tables.iterator(); iterator.hasNext();) {
                String tableName = (String) iterator.next();
                if(QD.interesting_orders.getIndexesForTable(tableName) == null) {
                    QD.interesting_orders.addIndex(new Index(tableName, new LinkedHashSet()));
                }
            }
            // get the list of tables and if there is no interesting order column on the table.


            // System.out.println("getInterestingOrders: used: " + QD.used.toString());
            // System.out.println("getInterestingOrders: interesting: " + QD.interesting_orders.toString());
        }
    }

    public void generateCandidateIndexesPerQuery() {
        PhysicalConfiguration phconfig = new PhysicalConfiguration();
        List phidxes = generateCandidatesFromConfigPerQuery(phconfig);

        for (ListIterator pi = phidxes.listIterator(); pi.hasNext();) {
            Configuration C = (Configuration) pi.next();
            String index = C.getSingleTableSet().toString();
            if (!perQueryHash.contains(index)) {
                candidatesPerQuery.add(C);
                perQueryHash.add(index);
            }
        }

        for (int i = 0; i < sqlStatements.size(); i++) {
            PhysicalConfiguration config = null;
            ZQuery query = (ZQuery) sqlStatements.elementAt(i);
            System.out.println("" + (i+1) + " -- query.toString() = " + query.toString());
            ColumnsGatherer gen = new ColumnsGatherer(query);
            Set columns = gen.getUsedColumns();
            Multimap<String,String> splitColumns = gen.associateColumnsWithTables(columns);

            config = new PhysicalConfiguration(splitColumns);

            ArrayList perQuery = generateCandidatesFromConfigPerQuery(config);
            perQuery.addAll(coveringIndexes(columns));
            //System.out.println("perQuery: " + i + perQuery);

            for (ListIterator pi = perQuery.listIterator(); pi.hasNext();) {
                Configuration C = (Configuration) pi.next();
                String index = C.getSingleTableSet().toString();
                if (!perQueryHash.contains(index)) {
                    candidatesPerQuery.add(C);
                    perQueryHash.add(index);
                    //System.out.println("perQuery: adding: " + i + " " + index);
                }
            }
        }

        //this.universe = config;
        //indexMemory.clear();
        perQueryHash.clear();
        System.out.println("Generated " +candidatesPerQuery.size() + " candidates");
    }

    public List coveringIndexes (Set usedColumns) {
        // this returns the covering index for
        Configuration config = associateColumnsWithTables(usedColumns);

        List covering = new ArrayList();
        for (final String tableName : config.getTableNames()) {
            LinkedHashSet set = config.getSingleTableSet(tableName);
            for (Iterator iterator = set.iterator(); iterator.hasNext();) {
                String col = (String) iterator.next();

                LinkedHashSet newCover = new LinkedHashSet();
                newCover.add(col);
                newCover.addAll(set);

                if(newCover.size() > 16) {
                    int i = 0;
                    for (Iterator iterator1 = newCover.iterator(); iterator1.hasNext();) {
                        String s = (String) iterator1.next();
                        if(i++ >= 16) {
                            iterator1.remove();
                        }
                    }
                }
                Index index = new Index(tableName, (newCover));
                covering.add(index);
            }
        }

        return covering;
    }


    public void generateCandidateIndexes() {
    }

    public void generateUniverse() {
    }

    private ArrayList generateCandidatesFromConfigPerQuery(PhysicalConfiguration config) {
        final int[] count = new int[]{0};
        ArrayList result = new ArrayList();
        return result;
    }

    public void processColumns(Object[] columns, String table_name) {
        Arrays.sort(columns, 1, columns.length);
        LinkedHashSet columnSet = new LinkedHashSet((List) Arrays.asList(columns));
        String key = columnSet.toString();
        if (indexMemory.contains(key)) {
            return;
        }
        indexMemory.add(key);

        //a set of columns from a given table that correspond to an index ...
        Configuration config = new Configuration();
        config.addColumnSet(table_name, columnSet);
        //System.out.println("processColumns: adding config" + config);
        candidates.add(config);
    }

    public Configuration associateColumnsWithTables(Set columns) {
        return null;
    }

    public static void main(String[] args) throws ParseException, IOException {
        String fileName = args[0];

        File file = new File(fileName);
        if (!file.exists() || !file.canRead()) {
            System.err.println("Invalid file: " + file);
            System.exit(1);
        }

        CandidateGenerator cg = new CandidateGenerator(file);

        cg.generateCandidateIndexesPerQuery();
        System.out.println("cg.candidates.size() = " + cg.candidatesPerQuery.size());
        PrintWriter bw = new PrintWriter(new FileWriter("indexes.list"));
        for (int i = 0; i < cg.candidatesPerQuery.size(); i++) {
            Configuration conf = (Configuration) cg.candidatesPerQuery.get(i);
            bw.println(conf);
        }
        bw.close();
        // cg.getInterestingOrders();
    }
}

