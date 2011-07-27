package edu.ucsc.dbtune.inum.linprog;

import edu.ucsc.dbtune.inum.model.Index;
import edu.ucsc.dbtune.inum.model.QueryDesc;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;

//code to generate a linear model given queries, configs, etc. 

public class LinearModel {

    ArrayList queries;//an array of query structs.
    ArrayList configs;//an array of configurations.
    ArrayList candidates;//an array of index candidates
    LinCand[] candidateArray;

    int queryCount;
    int configCount;
    int constraintCount;

    public static int currentCandidate;
    public static int currentQ;


    public void printCandidates() {
        System.out.println("candidates");
        for (int i = 0; i < candidateArray.length; i++)
            System.out.println(candidateArray[i]);
    }

    public void printConfigs() {
        System.out.println("configs\n" + configs);

    }


    public LinBlock objectiveBlock() {

        LinBlock result = LinBlock.zero(1, configs.size() + candidates.size());

        for (int i = 0; i < configs.size(); i++) {
            LinConfig C = (LinConfig) configs.get(i);
            result.block[0][i] = C.cost;
        }
        return result;
    }

    //return upper bounds instead of benefits
    public LinBlock objectiveBlockUpper() {

        LinBlock result = LinBlock.zero(1, configs.size() + candidates.size());

        for (int i = 0; i < configs.size(); i++) {
            LinConfig C = (LinConfig) configs.get(i);
            result.block[0][i] = C.max_benefit;
        }
        return result;
    }


    public LinBlock rightHandSideSimple(float storage_constraint) {
        LinBlock result = LinBlock.zero(queries.size() + queries.size() * candidates.size() + 1, 1);

        //	System.out.println("cand size " + candidates.size() + " " + candidates + " " + result.rows);

        //first the constraints corresponding to queries.
        for (int i = 0; i < queries.size(); i++)
            result.block[i][0] = 1;
        //then the constraints for the candidates
        for (int i = 0; i < queries.size() * candidates.size(); i++)
            result.block[queries.size() + i][0] = 0;

        result.block[result.rows - 1][0] = storage_constraint;

        return result;
    }


    public LinBlock rightHandSideSimpleRow(float storage_constraint, int theRow) {
        //LinBlock result = LinBlock.zero(queries.size()+queries.size()*candidates.size()+1,1);
        LinBlock result = LinBlock.zero(1, 1);
        //	System.out.println("cand size " + candidates.size() + " " + candidates + " " + result.rows);
        //first the constraints corresponding to queries.
        if ((theRow >= 0) && (theRow < queries.size()))
            result.block[0][0] = 1;
        //for (int i=0; i<queries.size(); i++)
        //result.block[i][0]=1;
        //then the constraints for the candidates
        if ((theRow >= queries.size()) && (theRow < queries.size() + queries.size() * candidates.size()))
            result.block[0][0] = 0;
        //for (int i=0; i<queries.size()*candidates.size(); i++)
        //result.block[queries.size() + i][0]=0;
        if (theRow == queries.size() + queries.size() * candidates.size())
            result.block[0][0] = storage_constraint;
        //result.block[result.rows-1][0] = storage_constraint;
        return result;
    }


    public LinBlock candidateConstraintBlock(int ID, int qID) {

        LinCand theCand = candidateArray[ID];
        LinBlock result = LinBlock.zero(1, configs.size() + candidates.size());
        //some of those configs must be used by the query, must be added for that query.
        boolean used = false;
        for (ListIterator ci = theCand.containing_configs.listIterator(); ci.hasNext();) {

            int configID = ((Integer) ci.next()).intValue();
            LinConfig C = (LinConfig) configs.get(configID);
            //if (C.queryID == qID) {
            result.block[0][configID] = 1;
            used = true;
            //}
        }

        result.block[0][configs.size() + ID] = -1;

        if (!used)
            result = null;

        return result;
    }


    public LinBlock constraintRightSide() {
        ArrayList rightSide = new ArrayList();
        int rows = 0;
        for (int i = 0; i < queries.size(); i++) {
            LinBlock B = queryRightSide(i);
            rightSide.add(B);
            rows += B.rows;
        }
        LinBlock result = LinBlock.zero(rows, 1);
        int currentRow = 0;
        for (int i = 0; i < rightSide.size(); i++) {
            LinBlock B1 = (LinBlock) rightSide.get(i);
            result.copy(currentRow, 0, B1);
            currentRow += B1.rows;
        }
        return result;
    }

    public LinBlock constraintMatrixSimpleRow(int theRow) {

        int rows = queries.size() + queries.size() * candidates.size() + 1;
        int columns = configs.size() + candidates.size();

        LinBlock result = LinBlock.zero(1, columns);
        int currentColumn = 0;
        if ((theRow >= 0) && (theRow < queries.size())) {
            for (int q = 0; q <= theRow; q++) {
                LinQuery Q = (LinQuery) queries.get(q);
                for (int j = 0; j < Q.configCount; j++) {
                    if (q == theRow)
                        result.block[0][currentColumn++] = 1;
                    else
                        currentColumn++;
                }
            }
        } else {
            if (theRow == rows - 1) {
                //fill in the storage constraint
                for (int ci = 0; ci < candidateArray.length; ci++)
                    result.block[0][configs.size() + ci] = candidateArray[ci].size;
            } else {
                //nextconfig
                int currentCandidate = (theRow - queries.size()) % candidates.size();
                int currentQuery = (theRow - queries.size()) / candidates.size();

                LinBlock B1 = candidateConstraintBlock(currentCandidate, currentQuery);
                if (B1 != null)
                    result.copy(0, 0, B1);
                else
                    result = null;
            }
        }

        return result;
    }

    public String getIndexSizeConstraint(float size) {
        StringBuffer buffer = new StringBuffer("size: ");
        boolean first = true;
        for (int i = 0; i < candidateArray.length; i++) {
            LinCand cand = candidateArray[i];
            if (cand.used > 0) {
                if (first) {
                    first = false;
                } else {
                    buffer.append(" + ");
                }
                buffer.append(cand.size).append(" y").append(i);
            }
        }
        buffer.append(" <= " + size);
        return buffer.toString();
    }


    public LinBlock queryRightSide(int queryID) {

        //one column per candidate
        int columns = candidates.size();
        LinQuery Q = (LinQuery) queries.get(queryID);
        int configCount = Q.configCount;
        int[] configSizes = new int[Q.configCount];

        int rows = 1;
        int first = Q.firstConfigID;
        int last = first + Q.configCount - 1;
        for (int i = first; i <= last; i++) {
            //one row per config per constituent index.
            LinConfig C = (LinConfig) configs.get(i);
            rows += C.indexes.size();
            configSizes[i - first] = C.indexes.size();
        }

        LinBlock result = new LinBlock(rows, 1);
        for (int i = 0; i < rows; i++)
            result.block[i][0] = 0;
        result.block[0][0] = 1;
        return result;
    }


    //just initialize the structure, use individual functions to add stuff..
    public LinearModel() {
        queries = new ArrayList();
        configs = new ArrayList();
        candidates = new ArrayList();
        queryCount = 0;
        configCount = 0;

        currentCandidate = -1;
        currentQ = 0;
    }


    public void usedCandidates() {
        int count = 0;
        for (int i = 0; i < candidateArray.length; i++) {
            if (candidateArray[i].used == 1) {
                count++;
            }
        }
        System.out.println("usedCandidates: " + count);

    }


    //init functions. 
    //first add the candidates and then the configs for each query. 
    public void addCandidates(ArrayList candidates, ArrayList sizes) {
        ListIterator si = sizes.listIterator();
        for (ListIterator ci = candidates.listIterator(); ci.hasNext();) {
            LinCand theNewCand = new LinCand();
            Index singleColConfig = (Index) ci.next();
            theNewCand.index = singleColConfig;
            LinkedHashSet columnSet = singleColConfig.getColumns();
            theNewCand.ID = columnSet.toString().hashCode();
            theNewCand.size = ((Number) si.next()).floatValue();
            this.candidates.add(theNewCand);
            //System.out.println("addCandidates: adding: " + columnSet.toString() + " " + theNewCand.ID);
        }

        candidateArray = (LinCand[]) this.candidates.toArray(new LinCand[this.candidates.size()]);

        //the candidates are sorted alphabetically so that we can
        //match them to configurations when we construct the constraints.
        Arrays.sort(candidateArray, new Comparator() {
            public int compare(Object o1, Object o2) {
                LinCand c1 = (LinCand) o1;
                LinCand c2 = (LinCand) o2;

                if (c1.ID > c2.ID) {
                    return 1;
                } else if (c1.ID == c2.ID) {
                    return 0;
                } else {
                    return -1;
                }
            }
        });
    }

    public void addQueryConfigs(int qId, QueryDesc QD, List queryConfigs, float max_benefit, CPlexBuffer buf) {
        queries = new ArrayList();
        configs = new ArrayList();
        for (int i = 0; i < candidateArray.length; i++) {
            LinCand linCand = candidateArray[i];
            linCand.containing_configs.clear();
        }

        //first add the query spec.
        LinQuery newQuery = new LinQuery();
        newQuery.ID = qId;

        //managing the number of configs goes after we eliminate duplicates
        //newQuery.configCount = queryConfigs.size();
        //configCount+=queryConfigs.size();


        newQuery.QD = QD;
        queries.add(newQuery);
        //queryCount++;
        //add the link .
        newQuery.firstConfigID = configCount;

        HashSet configSet = new HashSet();

        newQuery.configCount = 0;
        for (ListIterator ci = queryConfigs.listIterator(); ci.hasNext();) {
            ConfigPair C = (ConfigPair) ci.next();

            LinConfig newConfig = new LinConfig();

            //then add the config info...
            newConfig.ID = configCount;
            newConfig.config = C.config;
            newConfig.queryID = qId;
            newConfig.cost = C.cost;
            newConfig.indexes = new ArrayList();
            newConfig.max_benefit = max_benefit;

            //and now: the indexes ...
            for (Iterator<Index> iter = newConfig.config.indexes(); iter.hasNext();) {

                Index idx = iter.next();
                LinCand keyCand = new LinCand();
                keyCand.ID = idx.getKey().hashCode();

                int indexID = Arrays.binarySearch(candidateArray, keyCand, new Comparator() {
                    public int compare(Object o1, Object o2) {
                        LinCand c1 = (LinCand) o1;
                        LinCand c2 = (LinCand) o2;
                        if (c1.ID > c2.ID) {
                            return 1;
                        } else if (c1.ID == c2.ID) {
                            return 0;
                        } else {
                            return -1;
                        }
                    }
                });
                if (!((indexID >= 0) && (indexID <= candidateArray.length - 1)))
                    System.out.println("error could not find index " + idx.getKey() + " " + keyCand.ID);

                //System.out.println("addConfig: config " + C + " found " + columns + " " + keyCand.ID);
                newConfig.indexes.add(indexID);

                //done with the indexes, add the config structure

                //quick patch, but doublecheck if we've seen that configuration
                String signature = newConfig.signature();
                //System.out.println("Linearmodel: signature: " + signature);
                if (!configSet.contains(signature)) {
                    configSet.add(signature);
                    configs.add(newConfig);
                    newQuery.configCount++;

                    //update also pointers from the indexes --> configurations

                    for (ListIterator ii = newConfig.indexes.listIterator(); ii.hasNext();) {
                        indexID = (Integer) ii.next();
                        candidateArray[indexID].used = 1;
                        candidateArray[indexID].containing_configs.add(new Integer(configs.size() - 1));
                    }
                }
            }
        }

        int rows = 1 + candidates.size();

        StringBuffer buffer = new StringBuffer("c");
        buffer.append(constraintCount++).append(": ");
        boolean needPlus = false;
        LinBlock lb = this.constraintMatrixSimpleRow(0);
        for (int i = 0; i < lb.columns; i++) {
            if (lb.block[0][i] > 0.0) {
                if (needPlus)
                    buffer.append(" + ");
                else
                    needPlus = true;
                buffer.append(lb.block[0][i]).append(" x").append(configCount + i);
            }
        }

        buffer.append(" <= 1.0");
        if (needPlus)
            buf.getCons().println(buffer);

        for (int i = 1; i < rows; i++) {
            if (candidateArray[i - 1].used == 0) continue;

            buffer = new StringBuffer("c");
            lb = this.constraintMatrixSimpleRow(i);
            if (lb == null) continue;
            needPlus = false;
            buffer.append(constraintCount++).append(": ");
            for (int j = 0; j < lb.columns; j++) {
                if (lb.block[0][j] != 0.0) {
                    if (needPlus) {
                        if (lb.block[0][j] > 0) buffer.append(" + ");
                        else buffer.append("   ");
                    } else {
                        needPlus = true;
                    }
                    buffer.append(lb.block[0][j]);
                    if (j < configs.size())
                        buffer.append(" x").append(configCount + j);
                    else
                        buffer.append(" y").append(j - configs.size());
                }
            }
            buffer.append(" <= 0.0");
            if (needPlus)
                buf.getCons().println(buffer);
        }
        buf.getCons().flush();

        // add to the binary.
        for (int i = 0; i < configs.size(); i++) {
            buf.getBin().println("x" + (configCount + i) + " \\MODEL::" + configs.get(i));
            if (configCount + i != 0) {
                buf.getObj().print(" + ");
            }
            buf.getObj().print(((LinConfig) configs.get(i)).cost + " x" + (configCount + i));
        }

        configCount += newQuery.configCount;
    }


    public LinCand[] getCandidateArray(){
      return candidateArray;
    }
}
