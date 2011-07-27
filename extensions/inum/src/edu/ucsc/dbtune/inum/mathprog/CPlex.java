package edu.ucsc.dbtune.inum.mathprog;

import Zql.ParseException;
import edu.ucsc.dbtune.inum.Config;
import edu.ucsc.dbtune.inum.EnumerationGenerator;
import edu.ucsc.dbtune.inum.IndexAccessGenerator;
import edu.ucsc.dbtune.inum.autopilot.autopilot;
import edu.ucsc.dbtune.inum.commons.Pair;
import edu.ucsc.dbtune.inum.greedy.GreedyResult;
import edu.ucsc.dbtune.inum.model.Index;
import edu.ucsc.dbtune.inum.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.model.Plan;
import edu.ucsc.dbtune.inum.model.QueryDesc;
import edu.ucsc.dbtune.inum.model.WorkloadProcessor;
import ilog.concert.IloException;
import ilog.concert.IloLPMatrix;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloLinearNumExprIterator;
import ilog.concert.IloNumVar;
import ilog.concert.IloObjective;
import ilog.concert.IloRange;
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
    public Map values;
    public Map definitions;
    public static final Pattern pattern = Pattern.compile("(c\\d+|y\\d+_\\d+|y\\d+)");
    private CophyConstraints constr;

    public CPlex(String str, CophyConstraints constr) {
        this.fileName = str;
        this.constr = constr;
    }

    public void run() throws IloException, IOException {
        File solFile = new File(fileName + ".sol");
        File probFile = new File(fileName);
        System.out.println("solFile = " + solFile);

        if (!solFile.exists() || solFile.lastModified() < probFile.lastModified()) {
            IloCplex cplex = new IloCplex();
            cplex.importModel(fileName);

            if (!cplex.solve()) {
                System.err.println("Could not solve the problem");
                throw new IloException("Unable to solve the problem");
            }

            IloNumVar []vars = getCostAndIndexVariables(cplex);
            double[] vals = cplex.getValues(vars);
            values = new HashMap();
            for (int i = 0; i < vals.length; i++) {
                double value = vals[i];
                String name = vars[i].getName();

                if(value > 0)
                    values.put(name, value);
            }
        }

        definitions = parseProblemForVariables(values);
    }

    private IloNumVar[] getCostAndIndexVariables(IloCplex cplex) throws IloException {
        List<IloNumVar> variables = new ArrayList();
        IloLPMatrix matrix =getMatrix(cplex);
        IloNumVar vars[] = matrix.getNumVars();
        for (int i = 0; i < vars.length; i++) {
            IloNumVar var = vars[i];
            if(pattern.matcher(var.getName()).matches()) {
                variables.add(var);
            }
        }

        return variables.toArray(new IloNumVar[variables.size()]);
    }


    public List<ParetoPoint> runSoft() throws IloException, IOException {
        Set retSet = new HashSet();
        File solFile = new File(fileName + ".sol");
        File probFile = new File(fileName);
        System.out.println("solFile = " + solFile);

        if (!solFile.exists() || solFile.lastModified() < probFile.lastModified()) {
            IloCplex cplex = new IloCplex();
            cplex.importModel(fileName);

            ParetoPoint leftExtreme = runOneSoftObjective(cplex, 0, 1);
            ParetoPoint rightExtreme = runOneSoftObjective(cplex, 1, 0);

            retSet.add(leftExtreme);
            retSet.add(rightExtreme);

            recurseForParetoPoints(cplex, leftExtreme, rightExtreme, retSet);
        }

        List retList = new ArrayList();
        for (Iterator iterator = retSet.iterator(); iterator.hasNext();) {
            ParetoPoint point = (ParetoPoint) iterator.next();
            retList.add(point);
            System.out.println("point = " + point);
        }
        
        return retList;
    }

    private void recurseForParetoPoints(IloCplex cplex, ParetoPoint leftExtreme, ParetoPoint rightExtreme, Set retSet) throws IloException {
        if(retSet.size() == 10) return;
        double slope = getSlope(leftExtreme, rightExtreme);
        ParetoPoint point = runOneSoftObjective(cplex, 1, slope);
        retSet.add(point);
        double cost = point.getCost();
        double diff1 = getNormalizedDiff(cost, leftExtreme.getCost());
        double diff2 = getNormalizedDiff(rightExtreme.getCost(), cost);

        if(diff1 < 0.0001 || diff2 < 0.0001) {
            // too close to the other points.
            return;
        }

        if(diff1 > 0.05) {
            recurseForParetoPoints(cplex, leftExtreme, point, retSet);
        }

        if(diff2 > 0.05) {
            recurseForParetoPoints(cplex, point, rightExtreme, retSet);
        }
    }

    private static double getNormalizedDiff(double d1, double d2) {
        return Math.abs((d1-d2)*2/(d1+d2));
    }

    private double getSlope(ParetoPoint leftExtreme, ParetoPoint rightExtreme) {
        return (leftExtreme.getCost() - rightExtreme.getCost()) / (rightExtreme.getStorage() - leftExtreme.getStorage());
    }

    private ParetoPoint runOneSoftObjective(IloCplex cplex, double cValue, double sValue) throws IloException {
        setParameters(cplex, constr);
        long time = System.currentTimeMillis();
        Pair<IloNumVar, IloNumVar> pair = modifyCplex(cplex, cValue, sValue);
        if (!cplex.solve()) {
            System.err.println("Could not solve the problem");
            throw new IloException("Unable to solve the problem");
        }
        time = (System.currentTimeMillis()-time);

        double cost = cplex.getValue(pair.getLeft());
        double storage = cplex.getValue(pair.getRight());

        System.out.println("cost = " + cost);
        System.out.println("storage = " + storage);
        ParetoPoint point = new ParetoPoint(cost, storage, time, cValue, sValue);
        return point;
    }

    private void setParameters(IloCplex cplex, CophyConstraints constr) throws IloException {
        cplex.setParam(IloCplex.IntParam.ClockType, 2);
        cplex.setParam(IloCplex.DoubleParam.TiLim, constr.maxTime);
        cplex.setParam(IloCplex.DoubleParam.EpGap, Math.max(((double)constr.accuracy)/100, 1e-3));
    }

    public Pair<IloNumVar, IloNumVar> modifyCplex(IloCplex cplex, double cCoefficient, double sCoefficient) throws IloException {
        IloNumVar cVar = null, sVar = null;
        Iterator iter = cplex.getModel().iterator();
        while (iter.hasNext()) {
            Object o = iter.next();
            if(o instanceof IloLPMatrix) {
                IloLPMatrix matrix = (IloLPMatrix) o;
                IloRange range = matrix.getRange(matrix.getNrows() -1 );
                if(range.getName().equals("sizeC")) {
                    matrix.removeRow(matrix.getNrows()-1);
                }
            } else if (o instanceof IloObjective) {
                IloObjective objective = (IloObjective) o;
                IloLinearNumExpr expr = (IloLinearNumExpr) objective.getExpr();

                IloLinearNumExprIterator iter1 = expr.linearIterator();
                while (iter1.hasNext()) {
                    IloNumVar var = iter1.nextNumVar();
                    if(var.getName().equals("c")) {
                        cVar = var;
                        iter1.setValue(cCoefficient);
                    } else if(var.getName().equals("s")) {
                        sVar = var;
                        iter1.setValue(sCoefficient);
                    }
                }
                objective.setExpr(expr);
            }
        }
        //System.out.println("cplex = " + cplex);
        return Pair.of(cVar,sVar);
    }

    public IloLPMatrix getMatrix(IloCplex cplex) throws IloException {
        Iterator iter = cplex.getModel().iterator();
        while (iter.hasNext()) {
            Object o = iter.next();
            if (o instanceof IloLPMatrix) {
                IloLPMatrix matrix = (IloLPMatrix) o;
                return matrix;
            }
        }

        return null;
    }
    
    private Map parseProblemForVariables(Map values) throws IOException {
        Map map = new HashMap();
        BufferedReader reader = new BufferedReader(new FileReader(fileName));

        Pattern pattern = Pattern.compile("(\\w+)\\s+\\\\MODEL:(.*)$");
        Pattern planPattern = Pattern.compile("plans(\\d+): (.*) = 1$");
        //Pattern listPattern = Pattern.compile("\\[([^\\[]+)\\] (Q:\\d+)");
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("y")) {
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    String variable = matcher.group(1);
                    if (values.containsKey(variable)) {
                        map.put(variable, PhysicalConfiguration.loadIndexFromString(matcher.group(2)));
                    }
                }
            } else if (line.startsWith("plans")) {
                Matcher matcher = planPattern.matcher(line);
                if(matcher.find()) {
                    String query = matcher.group(1);
                    String plans[] = matcher.group(2).split(" \\+ ");
                    for (int i = 0; i < plans.length; i++) {
                        String plan = plans[i];
                        map.put(plan, query);
                    }
                }
            }
        }

        reader.close();
        return map;
    }

    private Map parseSolution(String s) throws IOException {
        Map variables = new HashMap();
        BufferedReader reader = new BufferedReader(new FileReader(s));
        String regex = "^  <variable\\s+name=\"(c\\d+|y\\d+_\\d+)\"\\s+index.*value=\"([^\"]+)\"";
        Pattern pattern = Pattern.compile(regex);
        String line = null;

        while ((line = reader.readLine()) != null) {
            Matcher matcher = pattern.matcher(line);
            if (!matcher.find()) {
                continue;
            }

            Float value = Float.parseFloat(matcher.group(2));
            if (value > 0) {
                String var = matcher.group(1);

                if(var.startsWith("y")) {
                    String idx = var.substring(0, var.indexOf('_'));
                    variables.put(idx, value);
                }
                
                variables.put(var, value);
            }
        }

        reader.close();
        return variables;
    }

    // todo(Huascar) refine hook method
    public GreedyResult processGeneratedIndexes(String queryFile, WorkloadProcessor proc) throws ParseException, IOException {
        EnumerationGenerator.loadConfigEnumerations(queryFile, proc.query_descriptors);
        Map indexSizes = IndexAccessGenerator.loadIndexSizes(queryFile);

        float costs[] = new float[proc.query_descriptors.size()];
        List<Map<String, Index>> usedIndexes = new ArrayList();

        for (int i = 0; i < proc.query_descriptors.size(); i++) {
            usedIndexes.add(new HashMap());
        }

        for (Iterator mapIterator = values.keySet().iterator(); mapIterator.hasNext();) {
            String str = (String) mapIterator.next();

            if(str.startsWith("y") && str.contains("_")) {
                // this is the index and plan.
                String []parts = str.split("_");
                Index idx = (Index) definitions.get(parts[0]);
                if(idx == null) {
                    continue;
                }

                idx.setTheSize((Integer) indexSizes.get(idx.getKey()));

                String query = (String) definitions.get("o"+parts[1]);
                int queryIdx = Integer.parseInt(query);
                usedIndexes.get(queryIdx).put(idx.getTableName(), idx);
            } else if(str.startsWith("c")) {
                str = str.substring(1);
                String query = (String) definitions.get("o"+str);
                int queryIdx = Integer.parseInt(query);
                values.get("c"+str);
                costs[queryIdx] = ((Double) values.get("c"+str)).floatValue();
            }
        }

        GreedyResult res = new GreedyResult();
        res.queryCosts = costs;
        res.queryDescs = proc.query_descriptors;
        res.usedIndexes = usedIndexes;

        return res;
    }

    public GreedyResult processGeneratedIndexes(String queryFile) throws ParseException, IOException {
      return processGeneratedIndexes(queryFile, new WorkloadProcessor(queryFile));
    }

    public GreedyResult processGeneratedIndexes(String queryFile, PhysicalConfiguration config) throws ParseException, SQLException, IOException {
        WorkloadProcessor proc = new WorkloadProcessor(new File(Config.WORKLOAD_DIR, queryFile));
        autopilot pilot = new autopilot();
        pilot.init_database();
        EnumerationGenerator.loadConfigEnumerations(queryFile, proc.query_descriptors);
        float costs[] = new float[proc.query_descriptors.size()];
        List<Map<String, Index>> usedIndexes = new ArrayList();
        Map<String, Index> map = new HashMap();

        pilot.implement_configuration(config);
        float totalIndexSize = 0;
        System.out.println("Implemented Indexes: ");
        for (Iterator iterator = config.indexes(); iterator.hasNext();) {
            Index index = (Index) iterator.next();
            totalIndexSize += pilot.getIndexSize(index);
            System.out.println(index.getTableName() + "." + index.getImplementedName() + " = " + pilot.getIndexSize(index));
            map.put(index.getImplementedName(), index);
        }

        double total = 0, totalEmptyCost = 0;
        for (int i = 0; i < proc.query_descriptors.size(); i++) {
            QueryDesc queryDesc = (QueryDesc) proc.query_descriptors.get(i);
            Plan mhjPlan = pilot.optimizer_cost(queryDesc.parsed_query.toString(), false, false);
            Plan nljPlan = pilot.optimizer_cost(queryDesc.parsed_query.toString(), false, true);
            Plan realPaln = mhjPlan;
            if (nljPlan.getTotalCost() < mhjPlan.getTotalCost()) {
                realPaln = nljPlan;
            }

            Set set = realPaln.getAccessedEntries();
            costs[i] = realPaln.getTotalCost();
            Map usedMap = new HashMap();
            for (Iterator<Map.Entry<String, Index>> iterator = map.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Index> entry = iterator.next();
                if (set.contains(entry.getKey())) {
                    usedMap.put(entry.getValue().getTableName(), entry.getValue());
                }
            }

            usedIndexes.add(usedMap);
            System.out.println("cost = " + realPaln.getTotalCost() + ", emptyCost = " + queryDesc.emptyCost);
            total += realPaln.getTotalCost();
            totalEmptyCost += queryDesc.emptyCost;
        }
        System.out.println("total = " + total + ", totalEmptyCost = " + totalEmptyCost);
        System.out.println("totalIndexSize = " + totalIndexSize);
        pilot.drop_configuration(config);

        GreedyResult res = new GreedyResult();
        res.queryCosts = costs;
        res.queryDescs = proc.query_descriptors;
        res.usedIndexes = usedIndexes;

        pilot.dispose();
        return res;
    }

    public static void main(String[] args) throws IOException, IloException, SQLException, ParseException {
        CPlex cplex = new CPlex(args[1], CophyConstraints.getDefaultConstraints());
        cplex.runSoft();
        printMap(cplex.definitions);
        //PhysicalConfiguration config = cplex.getTotalConfiguration();
        //cplex.processGeneratedIndexes(args[0]);
        //cplex.processGeneratedIndexes(args[0], new PhysicalConfiguration());
    }

    private static void printMap(Map map) {
        List strings = new ArrayList();
        for (Iterator iterator = map.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry entry = (Map.Entry) iterator.next();

            String key = (String) entry.getKey();

            if (key.charAt(0) == 'Q') {
                String[] idxes = (String[]) entry.getValue();
                StringBuffer buffer = new StringBuffer(key).append(" ").append(map.get("x" + key)).append(" [");

                for (int i = 0; i < idxes.length; i++) {
                    String idx = idxes[i];

                    if (i != 0) buffer.append(", ");
                    buffer.append(map.get("y" + idx));
                }
                buffer.append("]");
                strings.add(buffer.toString());
            } else if (key.charAt(0) == 'y') {
                strings.add(key + " = " + entry.getValue());
            }
        }

        Collections.sort(strings);
        for (Object o : strings) {
            System.out.println(o);
        }
    }

    public PhysicalConfiguration getTotalConfiguration() {
        Map map = definitions;
        
        PhysicalConfiguration config = new PhysicalConfiguration();
        for (Iterator iterator = map.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry entry = (Map.Entry) iterator.next();

            String key = (String) entry.getKey();

            if (key.charAt(0) == 'y') {
                Index index = (Index) entry.getValue();
                if (index != null)
                    config.addIndex(index);
            }
        }

        return config;
    }
}