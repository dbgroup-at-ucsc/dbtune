package edu.ucsc.dbtune.inum.linprog;

import Zql.ParseException;
import edu.ucsc.dbtune.inum.CostEstimator;
import edu.ucsc.dbtune.inum.autopilot.autopilot;
import edu.ucsc.dbtune.inum.model.Configuration;
import edu.ucsc.dbtune.inum.model.Index;
import edu.ucsc.dbtune.inum.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.model.Plan;
import edu.ucsc.dbtune.inum.model.QueryDesc;
import edu.ucsc.dbtune.inum.model.WorkloadProcessor;
import ilog.concert.IloException;
import ilog.cplex.IloCplex;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Mar 9, 2007
 * Time: 5:56:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class CPlex {
    private String fileName;

    public CPlex(String str) {
        this.fileName = str;
    }

    public Map run() throws IloException, IOException {

        File solFile = new File(fileName+".sol");
        File probFile = new File(fileName);

        if (!solFile.exists() || solFile.lastModified() < probFile.lastModified()) {
            IloCplex cplex = new IloCplex();
            cplex.importModel(fileName);
            if(!cplex.solve()) {
                System.err.println("Could not solve the problem");
                throw new IloException("Unable to solve the problem");
            }
            cplex.writeSolution(fileName+".sol");
        }

        // get the lines where value is higher.
        List values = parseSolution(fileName + ".sol");

        // get configurations from the problem file.
        Map maps = parseProblemForVariables(values);

        // System.out.println("definitions = " + definitions);
        // Configuration.saveConfigsToFile(fileName+".cfgs", configs);
        // return new Configuration(configs);

        return maps;
    }

    private Map parseProblemForVariables(List values) throws IOException {
        Set variableSet = new HashSet(values);
        Map map = new HashMap();
        BufferedReader reader = new BufferedReader(new FileReader(fileName));

        Pattern pattern = Pattern.compile("^(\\w+)\\s+\\\\MODEL::(.*)$");
        Pattern listPattern = Pattern.compile("\\[([^\\[]+)\\] (Q:\\d+)");
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (!line.startsWith("y") && !line.startsWith("x")) continue;
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                String variable = matcher.group(1);
                if (variableSet.contains(variable)) {
                    if (variable.startsWith("x")) {
                        Matcher listMatcher = listPattern.matcher(matcher.group(2));
                        if (listMatcher.matches()) {
                            String[] idxes = listMatcher.group(1).split(", ");
                            map.put(listMatcher.group(2), idxes);
                            map.put("x"+listMatcher.group(2), variable+" "+matcher.group(2));
                        }
                    } else {
                        map.put(variable, Configuration.loadConfigFromString(matcher.group(2)));
                    }
                }
            }
        }

        reader.close();
        return map;
    }

    private List parseSolution(String s) throws IOException {
        List variables = new ArrayList();
        BufferedReader reader = new BufferedReader(new FileReader(s));
        String regex = "^  <variable\\s+name=\"([xy]\\d+)\"\\s+index.*value=\"([^\"]+)\"";
        Pattern pattern = Pattern.compile(regex);
        String line = null;
        while ((line = reader.readLine()) != null) {
            Matcher matcher = pattern.matcher(line);
            if (!matcher.find()) {
                continue;
            }

            Double value = Double.parseDouble(matcher.group(2));
            if (value > 0.5) {
                variables.add(matcher.group(1));
            }
        }

        reader.close();
        return variables;
    }

    private void processGeneratedIndexes(String queryFile, PhysicalConfiguration config) throws ParseException, SQLException, IOException {
        CostEstimator CE = new CostEstimator(queryFile);
        WorkloadProcessor proc = CE.proc;
        autopilot ap = new autopilot();
        ap.init_database();

        ap.implement_configuration(config);
        System.out.println("Implemented Indexes: ");
        for (Iterator<Index> iterator = config.indexes(); iterator.hasNext();) {
            Index index = iterator.next();

            if(index.isImplemented())
                System.out.println(index.getImplementedName() + ", " + index);
        }

        double total = 0;
        for (int i = 0; i < proc.query_descriptors.size(); i++) {
            QueryDesc queryDesc = (QueryDesc) proc.query_descriptors.get(i);
            Plan plan = ap.optimizer_cost(queryDesc.parsed_query.toString(), false, true);
            
            double cost = plan.getTotalCost();
            System.out.println(queryDesc.emptyCost + ", " + i + " = " + cost);
            // CE.EstimateExhaustiveCost(Arrays.asList(queryDesc), config);
            total += cost;
        }
        System.out.println("total = " + total);
        ap.drop_configuration(config);
    }    

    public static void main(String[] args) throws IOException, IloException, SQLException, ParseException {
        CPlex cplex = new CPlex(args[1]);
         Map map = cplex.run();
         printMap(map);
        PhysicalConfiguration config = cplex.getTotalConfiguration(map);
        cplex.processGeneratedIndexes(args[0], config);
//        cplex.processGeneratedIndexes(args[1], new Configuration());
    }

    private static void printMap(Map map) {
        List strings = new ArrayList();
        for (Iterator iterator = map.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry entry = (Map.Entry) iterator.next();

            String key = (String) entry.getKey();

            if (key.charAt(0) == 'Q') {
                String[] idxes = (String[]) entry.getValue();
                StringBuffer buffer = new StringBuffer(key).append(" ").append(map.get("x"+key)).append(" [");

                for (int i = 0; i < idxes.length; i++) {
                    String idx = idxes[i];

                    if (i != 0) buffer.append(", ");
                    buffer.append(map.get("y" + idx));
                }
                buffer.append("]");
                strings.add(buffer.toString());
            } else if(key.charAt(0) == 'y') {
                strings.add(key + " = " + entry.getValue());
            }
        }

        Collections.sort(strings);
        for(Object o: strings) {
            System.out.println(o);
        }
    }

    public PhysicalConfiguration getTotalConfiguration(Map map) {
        PhysicalConfiguration config = new PhysicalConfiguration();
        for (Iterator iterator = map.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry entry = (Map.Entry) iterator.next();

            String key = (String) entry.getKey();

            if (key.charAt(0) == 'y') {
                config.addIndex((Index) entry.getValue());
            }
        }

        return config;
    }
}