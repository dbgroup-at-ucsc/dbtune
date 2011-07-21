package edu.ucsc.dbtune.inum.autopilot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class MSPlanDesc extends PlanDesc implements Serializable {
    private static final long serialVersionUID = -6598030869878841280L;
    public int nodeId;
    public int parent;
    public String physicalOp;
    public String argument;
    public float estimateRows;
    public float estimateIO;
    public float estimateCPU;
    public float totalSubtreeCost;

    public static final int INDEX_SCAN = 1;
    public static final int INDEX_SEEK = 1 << 1;
    public static final int TABLE_SCAN = 1 << 2;
    public static final int BKMK_LOOKUP = 1 << 3;
    public static final int HASH_MATCH = 1 << 4;
    public static final int MERGE_JOIN = 1 << 5;
    public static final int NESTED_LOOP = 1 << 6;
    public static final int FILTER = 1 << 7;


    private transient int opType = -1;
    private transient boolean optCalc = false;
    private transient boolean tblCalc = false;
    private String tableName = null;
    private static final Pattern OBJECT_TPCH_DBO = Pattern.compile("OBJECT:\\(\\[\\w+\\]\\.\\[dbo\\].\\[(\\w+)\\]");
    private static final Pattern idxPattern = Pattern.compile("\\[(idxdesigner_\\w+_\\d+|_dta_\\S+\\d+|(?:o_|p_|ps_|n_|r_)\\w+)\\]");
    private static final Pattern tblPattern = Pattern.compile("idxdesigner_(\\w+)_\\d+");
    //private static final Pattern OBJECT_TPCH_DBO = Pattern.compile("\\[(\\w+)\\].");


    public MSPlanDesc(int nodeId, int parent, String p, String a, float er, float eio, float ecpu, float total) {
        super(nodeId, parent, p, a, total);
        this.nodeId = nodeId;
        this.parent = parent;
        physicalOp = p;
        argument = a;
        estimateRows = er;
        estimateIO = eio;
        estimateCPU = ecpu;
        totalSubtreeCost = total;


    }

    public String getTable() {
        if (!tblCalc) {
            tableName = identifyTable(argument);
            tblCalc = true;
        }

        return tableName;
    }

    private String identifyTable(String argument) {
        if (argument == null) return null;
        Matcher matcher = OBJECT_TPCH_DBO.matcher(argument);
        if (matcher.find()) {
            String tableName = matcher.group(1);
            return tableName;
        }

        return null;
    }

    public int getOpType() {
        if (!optCalc) {
            if (physicalOp == null) {
                opType = 0;
            } else if (physicalOp.contains("Hash Match")) {
                opType = HASH_MATCH;
            } else if (physicalOp.contains("Merge Join")) {
                opType = MERGE_JOIN;
            } else if (physicalOp.contains("Nested Loop")) {
                opType = NESTED_LOOP;
            } else if (physicalOp.contains("Index Scan")) {
                opType = INDEX_SCAN;
            } else if (physicalOp.contains("Index Seek")) {
                opType = INDEX_SEEK;
            } else if (physicalOp.contains("Table Scan")) {
                opType = TABLE_SCAN;
            } else if (physicalOp.contains("Bookmark Lookup")) {
                opType = BKMK_LOOKUP;
            } else if (physicalOp.contains("Filter")) {
                opType = FILTER;
            } else {
                opType = 0;
            }
            optCalc = true;
        }

        return opType;
    }

    public boolean isScan() {
        return ((getOpType() & INDEX_SCAN) != 0);
    }


    public boolean isSeek() {
        return ((getOpType() & INDEX_SEEK) != 0);

    }

    public boolean isFilter() {
        return ((getOpType() & FILTER) != 0);
    }


    public boolean isJoin() {
        return ((getOpType() & (HASH_MATCH | NESTED_LOOP | MERGE_JOIN)) != 0);
    }


    public boolean isLeaf() {
        return (getOpType() & (INDEX_SCAN | INDEX_SEEK | TABLE_SCAN)) != 0;
    }

    public String toString() {
        return "node = " + nodeId + " parent = " + parent + " physicalOp = " + physicalOp + ", argument = " + argument
                + ", estimateRows = " + estimateRows + ", estimateCPU = " + estimateCPU + "; estimateIO = " + estimateIO +
                ", totaSubtreeCost = " + totalSubtreeCost;
    }

    public void setPhysicalOp(String s) {
        this.physicalOp = s;
        opType = -1;
    }

    public static List getIndexesUsed(List plans) {
        List list = new ArrayList();
        for (int i = 0; i < plans.size(); i++) {
            MSPlanDesc desc = (MSPlanDesc) plans.get(i);
            String idxName = desc.getIndexName();
            list.add(idxName);
        }

        return list;
    }

    public String getIndexName() {
        String idxName = null;
        if(this.argument.contains("[PK_PART]")) {
            this.argument = this.argument.replace("[PK_PART]", "[idxdesigner_part_1001]");
        }
        if ((this.getOpType() & (INDEX_SCAN | INDEX_SEEK)) != 0) {
            Matcher matcher = idxPattern.matcher(this.argument);
            if(matcher.find()) {
                idxName = matcher.group(1);
            }
        }
        return idxName;
    }

    public String getTableNameFromIndexName(String indexName) {
        Matcher matcher = tblPattern.matcher(indexName);
        if(matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    public static List[] splitPlansForQueries(List combinedPlans) {
        List listOfPlans = new ArrayList();
        List currentPlan = null;
        for (int i = 0; i < combinedPlans.size(); i++) {
            MSPlanDesc desc = (MSPlanDesc) combinedPlans.get(i);
            if(desc.parent == 0) {
                if(currentPlan != null) listOfPlans.add(currentPlan);
                currentPlan = new ArrayList();
            }
            currentPlan.add(desc);
        }
        if(currentPlan != null) listOfPlans.add(currentPlan);

        return (List[]) listOfPlans.toArray(new List[listOfPlans.size()]);
    }

}
