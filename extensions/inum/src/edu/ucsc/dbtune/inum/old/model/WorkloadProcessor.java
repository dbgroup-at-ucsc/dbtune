package edu.ucsc.dbtune.inum.old.model;

import Zql.ParseException;
import Zql.ZConstant;
import Zql.ZExp;
import Zql.ZExpression;
import Zql.ZFromItem;
import Zql.ZGroupBy;
import Zql.ZOrderBy;
import Zql.ZQuery;
import Zql.ZSelectItem;
import Zql.ZqlParser;
import com.google.common.collect.Multimap;
import edu.ucsc.dbtune.inum.old.Config;
import edu.ucsc.dbtune.inum.old.autopilot.PostgresGlobalInfo;
import edu.ucsc.dbtune.inum.old.commons.Cloner;
import edu.ucsc.dbtune.inum.old.commons.ZqlUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.
 * User: Debabrata Dash
 * Date: Feb 23, 2006
 * Time: 8:13:23 PM
 * The file is a copyright of CMU, all rights reserved.
 */
public class WorkloadProcessor implements Serializable {
    private Vector sqlStatements;
    public ArrayList<QueryDesc> query_descriptors;
    public ArrayList<Index> candidates;
    public ArrayList<Index> candidatesPerQuery;
    public HashSet indexMemory;
    public PhysicalConfiguration universe;
    public HashSet perQueryHash;

    public static final Pattern DATE_PATTERN = Pattern.compile("\\d\\d\\d\\d-\\d{1,2}-\\d{1,2}");
    public ArrayList materializedViews;
    public HashMap mava_costs;

  private static final AtomicReference<PostgresGlobalInfo> GLOBAL_INFO = new AtomicReference<PostgresGlobalInfo>();

    static {
        ZqlUtils.prepareZqlParser();
    }

    public WorkloadProcessor(String fileName) throws ParseException {
        this(new File(Config.WORKLOAD_DIR, fileName));
    }

    public WorkloadProcessor(Vector statements) {
        _initWorkloadProcessor(statements);
    }
    
    public WorkloadProcessor(File file) throws ParseException {
        ZqlParser parser = null;

        try {
            parser = new ZqlParser(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            assert false : e.getMessage();
        }
        Vector statements = parser.readStatements();
        _initWorkloadProcessor(statements);
    }

    private void _initWorkloadProcessor(Vector statements) {
        sqlStatements = statements;
        query_descriptors = new ArrayList();
        candidates = new ArrayList();
        indexMemory = new HashSet();
        candidatesPerQuery = new ArrayList();
        perQueryHash = new HashSet();

        universeAndInterestingOrdersProcessing();
    }

    protected void universeAndInterestingOrdersProcessing(){
        getInterestingOrders();
        generateUniverse();
    }

    protected Vector<ZQuery> getSQLStatements(){
      return (Vector<ZQuery>)sqlStatements;
    }

    //C:here is set in config all the interesting indexes per each query
    public void getInterestingOrders() {
        if (!query_descriptors.isEmpty()) return;

        for (int i = 0; i < sqlStatements.size(); i++) {
            ZQuery query = (ZQuery) sqlStatements.elementAt(i);
            QueryDesc QD = new QueryDesc();
            QD.queryString = query.toString();
            QD.parsed_query = query;
            //save the query info ...
            query_descriptors.add(QD);
            // System.out.println("query.toString() = " + query.toString());
            //C: this will hold the columns names from the query: from select, where clause,joins,  groupby, orderby
            ColumnsGatherer gen = new ColumnsGatherer(query); //

            //process interesting orders from the parsed representation
            //Set interesting_columns = gen.getInterestingOrders();
            //Set used_columns = gen.getUsedColumns();

            //C: will contain the tables used by the query
            QD.used = new PhysicalConfiguration(gen.associateColumnsWithTables(gen.getUsedColumns()));

            //C: the tables which columns are in the group by
            if (gen.getGroupByOrders() != null)
                QD.group_by = new PhysicalConfiguration(gen.getGroupByOrders());

           //C: if group by is null, then get the tables which have columns in order by clause
            if (QD.group_by == null)
                QD.group_by = new PhysicalConfiguration(gen.getOrderByOrders());

            //C: tables with have columns in join
            QD.interesting_orders = new PhysicalConfiguration(gen.associateColumnsWithTables(gen.getInterestingOrders()));

            //printout results for inspection
            if (QD.group_by != null) {
                // System.out.println("getInterestingOrders: groupby: " + QD.group_by.toString());
                for (Iterator iterator = QD.group_by.indexes(); iterator.hasNext();) {
                    Index index = (Index) iterator.next();
                    // QD.interesting_orders.addIndex(index);
                    if(QD.interesting_orders.getIndexedTableNames().contains(index.getTableName())) {
                        Index idx = QD.interesting_orders.getFirstIndexForTable(index.getTableName());
                        idx.getColumns().addAll(index.getColumns());
                    } else {
                        QD.interesting_orders.addIndex(index);
                    }
                }
            }

            Set tables = gen.getQueryTables();
            for (Iterator iterator = tables.iterator(); iterator.hasNext();) {
                String tableName = (String) iterator.next();
                if (QD.interesting_orders.getIndexesForTable(tableName) == null) {
                    QD.interesting_orders.addIndex(new Index(tableName, new LinkedHashSet()));
                }
            }
            // get the list of tables and if there is no interesting order column on the table.

            // System.out.println("getInterestingOrders: used: " + QD.used.toString());
            // System.out.println("getInterestingOrders: interesting: " + QD.interesting_orders.toString());
        }
    }

    public void getInterestingOrders(PostgresGlobalInfo globalInfo){
        if (!query_descriptors.isEmpty()) return;

        for (int i = 0; i < sqlStatements.size(); i++) {
            ZQuery query = (ZQuery) sqlStatements.elementAt(i);
            QueryDesc QD = new QueryDesc();
            QD.queryString = query.toString();
            QD.parsed_query = query;
            //save the query info ...
            query_descriptors.add(QD);
            // System.out.println("query.toString() = " + query.toString());
            //C: this will hold the columns names from the query: from select, where clause,joins,  groupby, orderby
            ColumnsGatherer gen = new ColumnsGatherer(query, globalInfo); //

            //process interesting orders from the parsed representation
            //Set interesting_columns = gen.getInterestingOrders();
            //Set used_columns = gen.getUsedColumns();

            //C: will contain the tables used by the query
            QD.used = new PhysicalConfiguration(gen.associateColumnsWithTables(gen.getUsedColumns()));

            //C: the tables which columns are in the group by
            if (gen.getGroupByOrders() != null)
                QD.group_by = new PhysicalConfiguration(gen.getGroupByOrders());

           //C: if group by is null, then get the tables which have columns in order by clause
            if (QD.group_by == null)
                QD.group_by = new PhysicalConfiguration(gen.getOrderByOrders());

            //C: tables with have columns in join
            QD.interesting_orders = new PhysicalConfiguration(gen.associateColumnsWithTables(gen.getInterestingOrders()));

            //printout results for inspection
            if (QD.group_by != null) {
                // System.out.println("getInterestingOrders: groupby: " + QD.group_by.toString());
                for (Iterator iterator = QD.group_by.indexes(); iterator.hasNext();) {
                    Index index = (Index) iterator.next();
                    // QD.interesting_orders.addIndex(index);
                    if(QD.interesting_orders.getIndexedTableNames().contains(index.getTableName())) {
                        Index idx = QD.interesting_orders.getFirstIndexForTable(index.getTableName());
                        idx.getColumns().addAll(index.getColumns());
                    } else {
                        QD.interesting_orders.addIndex(index);
                    }
                }
            }

            Set tables = gen.getQueryTables();
            for (Iterator iterator = tables.iterator(); iterator.hasNext();) {
                String tableName = (String) iterator.next();
                if (QD.interesting_orders.getIndexesForTable(tableName) == null) {
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

        
        for (int i = 0; i < sqlStatements.size(); i++) {
            PhysicalConfiguration config = new PhysicalConfiguration();
            ZQuery query = (ZQuery) sqlStatements.elementAt(i);
            System.out.println("" + (i + 1) + " -- query.toString() = " + query.toString());
            ColumnsGatherer gen = new ColumnsGatherer(query);
            generateCandidatesFromCG(gen);
            candidates.addAll(coveringIndexes(gen.getUsedColumns()));
        }
    }

    public void generateCandidateIndexesPerQuery(PostgresGlobalInfo globalInfo) {
        PhysicalConfiguration phconfig = new PhysicalConfiguration();


        for (int i = 0; i < sqlStatements.size(); i++) {
            PhysicalConfiguration config = new PhysicalConfiguration();
            ZQuery query = (ZQuery) sqlStatements.elementAt(i);
            System.out.println("" + (i + 1) + " -- query.toString() = " + query.toString());
            ColumnsGatherer gen = new ColumnsGatherer(query, globalInfo);
            generateCandidatesFromCG(gen);
            candidates.addAll(coveringIndexes(gen.getUsedColumns()));
        }
    }

    public List coveringIndexes(Set usedColumns) {
        // this returns the covering index for
        Multimap config = associateColumnsWithTables(usedColumns);

        List covering = new ArrayList();
        for (final String tableName : (Set<String>) config.keySet()) {
            Collection columns = config.get(tableName);
            for (Iterator iterator = columns.iterator(); iterator.hasNext();) {
                String col = (String) iterator.next();

                LinkedHashSet newCover = new LinkedHashSet();
                newCover.add(col);
                newCover.addAll(columns);

                if (newCover.size() > 16) {
                    int i = 0;
                    for (Iterator iterator1 = newCover.iterator(); iterator1.hasNext();) {
                        String s = (String) iterator1.next();
                        if (i++ >= 16) {
                            iterator1.remove();
                        }
                    }
                }
                covering.add(new Index(tableName, newCover));
            }
        }

        return covering;
    }


    public void generateCandidateIndexes() {
        if(Config.generateAllCandidates()) {
            this.generateCandidateIndexesFull();
        } else { 
            this.generateCandidateIndexesPerQuery();
        }

        Set candidateSet = new HashSet();
        ArrayList uniqList = new ArrayList();
        for (Iterator<Index> iterator = candidates.iterator(); iterator.hasNext();) {
            Index index = iterator.next();
            if(!candidateSet.contains(index.getKey())) {
                uniqList.add(index);
                candidateSet.add(index.getKey());
            }
        }

        this.candidates = uniqList;
        System.out.println("Generated " + candidates.size() + " candidates");
    }

    public void generateCandidateIndexesFull() {
        PhysicalConfiguration config = new PhysicalConfiguration();
        for (int i = 0; i < sqlStatements.size(); i++) {
            ZQuery query = (ZQuery) sqlStatements.elementAt(i);
            System.out.println("query.toString() = " + query.toString());
            ColumnsGatherer gen = new ColumnsGatherer(query);
            Set columns = gen.getUsedColumns();
            Multimap<String, String> splitColumns = gen.associateColumnsWithTables(columns);

            for (final String tableName : splitColumns.keySet()) {
                LinkedHashSet set = new LinkedHashSet(splitColumns.get(tableName));
                config.addIndex(new Index(tableName, set));
            }
        }
        this.universe = config;
        generateCandidatesFromConfig(config);
        //indexMemory.clear();
    }

    public void generateUniverse() {
        PhysicalConfiguration config = new PhysicalConfiguration();
        for (int i = 0; i < sqlStatements.size(); i++) {
            ZQuery query = (ZQuery) sqlStatements.elementAt(i);
            ColumnsGatherer gen = new ColumnsGatherer(query);
            Set columns = gen.getUsedColumns(); //the columns used in the query, in select where.. they are stored in eqColumns
            Multimap<String, String> splitColumns = gen.associateColumnsWithTables(columns); //hashmap for table, columns

            for (final String tableName : splitColumns.keySet()) {
                LinkedHashSet set = new LinkedHashSet(splitColumns.get(tableName));
                config.addIndex(new Index(tableName, set)); //add them in config
            }
        }
        this.universe = config;
    }

    public void generateUniverse(PostgresGlobalInfo globalInfo) {
        PhysicalConfiguration config = new PhysicalConfiguration();
        for (int i = 0; i < sqlStatements.size(); i++) {
            ZQuery query = (ZQuery) sqlStatements.elementAt(i);
            ColumnsGatherer gen = new ColumnsGatherer(query, globalInfo);
            Set columns = gen.getUsedColumns(); //the columns used in the query, in select where.. they are stored in eqColumns
            Multimap<String, String> splitColumns = gen.associateColumnsWithTables(columns); //hashmap for table, columns

            for (final String tableName : splitColumns.keySet()) {
                LinkedHashSet set = new LinkedHashSet(splitColumns.get(tableName));
                config.addIndex(new Index(tableName, set)); //add them in config
            }
        }
        this.universe = config;
    }

    public void generateCandidatesFromCG(ColumnsGatherer cg) {
        Set columns = cg.getUsedColumns(); //the columns used in the query, in select where.. they are stored in eqColumns
        Multimap<String, String> splitColumns = cg.associateColumnsWithTables(columns); //hashmap for table, columns
        Multimap<String, String> splitInterestingOrders = ColumnsGatherer.associateColumnsWithTables(cg.getInterestingOrders());
        Multimap<String, String> splitIndexableColumns = ColumnsGatherer.associateColumnsWithTables(cg.getIndexableColumns());
        Multimap<String, String> splitWhereColumns = ColumnsGatherer.associateColumnsWithTables(cg.getWhereColumns());

        for (final String tableName : splitColumns.keySet()) {
            Set allCols = new HashSet(splitColumns.get(tableName));
            Set interestingOrders = new HashSet(splitInterestingOrders.get(tableName));
            Set indexSet = new HashSet(splitIndexableColumns.get(tableName));
            Set whereSet = new HashSet(splitWhereColumns.get(tableName));
            Set scanOnlySet = new HashSet(allCols);
            scanOnlySet.removeAll(interestingOrders);
            scanOnlySet.removeAll(whereSet);
            
            Set firstColumns = new HashSet(whereSet);
            firstColumns.addAll(interestingOrders);

            for (Iterator iterator = firstColumns.iterator(); iterator.hasNext();) {
                String firstCol = (String) iterator.next();
                Set remainingColumns = new HashSet(firstColumns);
                remainingColumns.remove(firstCol);
                buildIndexes(tableName, firstCol, (String[]) remainingColumns.toArray(new String[remainingColumns.size()]), scanOnlySet);
            }
        }
    }

    protected void generateCandidatesFromConfig(PhysicalConfiguration config) {
        for (final String tableName : config.getIndexedTableNames()) {
            Set fieldSet = new HashSet();
            for (Iterator<Index> iterator = config.getIndexesForTable(tableName).iterator(); iterator.hasNext();) {
                Index index = iterator.next();
                fieldSet.addAll(index.getColumns());
            }

            String fields[] = (String[]) fieldSet.toArray(new String[fieldSet.size()]);
            // get the columns from the where clause, and take their subsets.

            for (int j = 0; j < fields.length; j++) {
                String firstColumn = fields[j];
                String rest[] = new String[fields.length - 1];
                for (int k = 0, idx = 0; k < fields.length; k++) {
                    String field = fields[k];
                    if (!field.equals(firstColumn))
                        rest[idx++] = field;
                }

                buildIndexes(tableName, firstColumn, rest, new HashSet());
            }
        }
    }

    private void buildIndexes(String tableName, String firstColumn, String[] rest, Set scanOnly) {
        int length = Math.min(rest.length, getMaxIndexLength());

        // add the 0th instnace.
        LinkedHashSet set = new LinkedHashSet();
        set.add(firstColumn);
        set.addAll(scanOnly);
        candidates.add(new Index(tableName, set));

        for (int k = 0; k < length; k++) {
            CombGen generator = new CombGen(rest.length, k + 1);
            while (generator.hasMore()) {
                int indices[] = generator.getNext();
                LinkedHashSet columns = new LinkedHashSet();
                columns.add(firstColumn);
                for (int l = 0; l < indices.length; l++) {
                    int indice = indices[l];
                    columns.add(rest[indice]);
                }

                columns.addAll(scanOnly);

                Index newIdx = new Index(tableName, columns);
                candidates.add(newIdx);
            }
        }
    }

    private ArrayList generateCandidatesFromConfigPerQuery(Multimap<String, String> config) {
        final int[] count = new int[]{0};
        ArrayList result = new ArrayList();
        for (final String tableName : config.keySet()) {
            Collection list = config.get(tableName);
            String fields[] = (String[]) list.toArray(new String[list.size()]);

            if (fields.length == 1) {
            } else {
                for (int j = 0; j < list.size(); j++) {
                    String firstColumn = fields[j];

                    /*
                    Configuration index1 = new Configuration();
                    index1.addColumn(tableName, firstColumn);
                    result.add(index1);
                    */

                    String rest[] = new String[fields.length - 1];

                    for (int k = 0, idx = 0; k < fields.length; k++) {
                        String field = fields[k];
                        if (!field.equals(firstColumn))
                            rest[idx++] = field;
                    }

                    int length = Math.min(rest.length, 10);
                    for (int k = 0; k < length; k++) {
                        CombGen generator = new CombGen(rest.length, k + 1);
                        while (generator.hasMore()) {
                            int indices[] = generator.getNext();
                            LinkedHashSet columns = new LinkedHashSet();
                            columns.add(firstColumn);
                            for (int l = 0; l < indices.length; l++) {
                                int indice = indices[l];
                                columns.add(rest[indice]);
                            }

                            result.add(new Index(tableName, (columns)));
                            count[0]++;
                            if (count[0] % 1000 == 0) {
                                System.out.println("Generated " + count[0] + " indexes ");
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    public List generateCandidateMatViews() {
        Set candidates = new LinkedHashSet();
        for (int i = 0; i < query_descriptors.size(); i++) {
            QueryDesc desc = (QueryDesc) query_descriptors.get(i);
            ZQuery query = (ZQuery) Cloner.clone(desc.parsed_query);

            // in the where clause remove the subqueries
            Set columns = new HashSet();
            ZExp exp = query.getWhere();
            ZExp exp1 = _removeSubQueries(exp, columns);
            query.addWhere(exp1);

            // remove the order by clause, but keep the columns.
            // always first select the order by columns.
            LinkedHashSet selectColumns = new LinkedHashSet();
            LinkedHashSet selectColumnNames = new LinkedHashSet();
            Vector by = query.getOrderBy();
            if (by != null) {
                for (int j = 0; j < by.size(); j++) {
                    ZOrderBy orderBy = (ZOrderBy) by.elementAt(j);
                    if(selectColumnNames.add(orderBy.getExpression().toString())) {
                        selectColumns.add(orderBy.getExpression());
                    }
                }
            }

            for (Iterator iterator = columns.iterator(); iterator.hasNext();) {
                String col = (String) iterator.next();
                if(selectColumnNames.add(col)) {
                    selectColumns.add(ZqlUtils.getNewColumn(col));
                }
            }

            query.addOrderBy(null);

            Vector select = query.getSelect();
            Set groupBys = new HashSet(ZqlUtils.getGroupByColumns(query.getGroupBy()));
            Vector groupColumns = new Vector();
            ZExp removedOperand = null;

            for (int j = 0; j < select.size(); j++) {
                ZSelectItem item = (ZSelectItem) select.elementAt(j);
                ZExp sexp = item.getExpression();

                if (!(sexp instanceof ZConstant)) {
                    if (sexp instanceof ZExpression) {
                        ZExpression zsexp = (ZExpression) sexp;
                        if(zsexp.getOperator().equalsIgnoreCase("COUNT")) {
                            removedOperand = zsexp.getOperand(0);
                            continue;
                        }
                        List fields = ZqlUtils.getUsedColumns(sexp, false);
                        for (int k = 0; k < fields.size(); k++) {
                            String fieldName = (String) fields.get(k);
                            ZConstant newColumn = ZqlUtils.getNewColumn(fieldName);
                            if (!groupBys.contains(fieldName)) {
                                // add to the list.
                                groupColumns.add(newColumn);
                            }

                            if(selectColumnNames.add(newColumn.toString())) {
                                selectColumns.add(newColumn);
                            }
                        }
                    } else {
                        throw new UnsupportedOperationException(sexp.toString());
                    }
                } else {
                    if(selectColumnNames.add(sexp.toString())) {
                        selectColumns.add(sexp);
                    }
                }
            }

            // now convert the select columns to ZSelectItems
            StringBuffer buffer = new StringBuffer("select ");
            if (selectColumns.isEmpty()) {
                selectColumns.add(removedOperand);
            }
            for (Iterator iterator = selectColumns.iterator(); iterator.hasNext();) {
                ZExp zExp = (ZExp) iterator.next();
                buffer.append(zExp);
                if(iterator.hasNext()) buffer.append(", ");
            }
            buffer.append(" from dual");
            try {
                query.addSelect(ZqlUtils.parseSQL(buffer.toString()).getSelect());
            } catch (ParseException e) {
                System.err.println("buffer = " + buffer);
                e.printStackTrace();
                throw new AssertionError(e);
                // not possible.
            }

            if(!groupColumns.isEmpty()) {
                ZGroupBy groupBy;

                if(query.getGroupBy() == null) {
                    groupBy = new ZGroupBy(groupColumns);
                } else {
                    Vector v = query.getGroupBy().getGroupBy();
                    if (v != null) {
                        v = groupColumns;
                    } else {
                        v.addAll(groupColumns);
                    }
                    groupBy = new ZGroupBy(v);
                    groupBy.setHaving(query.getGroupBy().getHaving());
                }

                query.addGroupBy(groupBy);
            }

            // now lets build the materialized views.
            Vector froms = query.getFrom();
            List fromNames = new ArrayList(froms.size());
            for (int j = 0; j < froms.size(); j++) {
                ZFromItem item = (ZFromItem) froms.get(j);
                fromNames.add(item.getTable());
            }

            ZQuery filtered = _filterQueryForTables(query, new HashSet(fromNames));
            candidates.add(new MatView(filtered != null ? filtered : query));

            int maxSize = froms.size() - 1;
            for (int j = 1; j <= maxSize; j++) {
                CombGen cg = new CombGen(froms.size(), j);
                while (cg.hasMore()) {
                    Set tables = new HashSet();
                    int[] next = cg.getNext();
                    for (int k = 0; k < next.length; k++) {
                        int i1 = next[k];
                        tables.add(fromNames.get(i1));
                    }

                    ZQuery query1 = _filterQueryForTables(query, tables);
                    if (query1 != null) {
                        candidates.add(new MatView(query1));
                    }
                }
            }
        }

        materializedViews = new ArrayList(candidates);
        return materializedViews;
    }

    private ZQuery _filterQueryForTables(ZQuery query, Set tables) {
        Vector filteredSelects = filterSelects(query.getSelect(), tables);
        if (filteredSelects.size() == 0 || filteredSelects.size() >= 16) return null;
        Vector filteredFroms = filterFroms(query.getFrom(), tables);
        ZExp filteredWhere = filterExpression(query.getWhere(), tables);
        ZGroupBy groupBy = filterGroups(query.getGroupBy(), tables);

        groupBy = groupBy != null ? updateGroupBy(filteredSelects, groupBy) : null;

        ZQuery zQuery = new ZQuery();
        zQuery.addSelect(filteredSelects);
        zQuery.addFrom(filteredFroms);
        zQuery.addWhere(filteredWhere);
        zQuery.addGroupBy(groupBy);

        return zQuery;
    }

    private ZGroupBy updateGroupBy(Vector filteredSelects, ZGroupBy groupBy) {
        Set grpSet = new HashSet();
        Vector newGrpBy = new Vector();

        if (groupBy != null && groupBy.getGroupBy() != null) {
            for (int i = 0; i < groupBy.getGroupBy().size(); i++) {
                ZExp exp = (ZExp) groupBy.getGroupBy().elementAt(i);
                grpSet.add(exp.toString());
            }
            newGrpBy.addAll(groupBy.getGroupBy());
        }

        for (int i = 0; i < filteredSelects.size(); i++) {
            ZSelectItem item = (ZSelectItem) filteredSelects.elementAt(i);
            if (item.getAggregate() == null && !grpSet.contains(item.toString())) {
                newGrpBy.add(ZqlUtils.getUsedColumns(item.getExpression(), false));
            }
        }

        if (!newGrpBy.isEmpty()) {
            ZGroupBy newGroup = new ZGroupBy(newGrpBy);
            if (groupBy != null)
                newGroup.setHaving(groupBy.getHaving());

            return newGroup;
        } else {
            return groupBy;
        }
    }

    private ZGroupBy filterGroups(ZGroupBy groupBy, Set tables) {
        if (groupBy == null) {
            return null;
        }

        Vector groupBys = groupBy.getGroupBy();
        Vector newGroups = new Vector(groupBys.size());
        for (int i = 0; i < groupBys.size(); i++) {
            ZExp exp = (ZExp) groupBys.get(i);
            ZExp filtered = filterExpression(exp, tables);
            if (filtered != null) {
                newGroups.add(filtered);
            }
        }

        ZExp having = groupBy.getHaving() == null ? null : filterExpression(groupBy.getHaving(), tables);
        ZGroupBy by = null;
        if (!newGroups.isEmpty()) {
            by = new ZGroupBy(newGroups);
            by.setHaving(having);
        }
        return by;
    }

    private Vector filterFroms(Vector froms, Set tables) {
        Vector newFroms = new Vector(froms.size());
        for (int i = 0; i < froms.size(); i++) {
            ZFromItem item = (ZFromItem) froms.elementAt(i);
            if (tables.contains(item.getTable())) {
                newFroms.add(item);
            }
        }

        return newFroms;
    }

    private Vector filterSelects(Vector selects, Set tables) {
        Vector newSelects = new Vector(selects.size());
        for (int i = 0; i < selects.size(); i++) {
            ZSelectItem item = (ZSelectItem) selects.get(i);
            ZExp exp = item.getExpression();
            ZExp filtered = filterExpression(exp, tables);

            if (filtered != null) {
                ZSelectItem item1 = (ZSelectItem) Cloner.clone(item);
                item1.setExpression(filtered);
                newSelects.add(item1);
            }
        }

        return newSelects;
    }

    private ZExp filterExpression(ZExp exp, Set tables) {
        if(exp == null)
            return null;
        
        if (exp instanceof ZExpression) {
            ZExpression expression = (ZExpression) exp;
            String operator = expression.getOperator();
            Vector operands = expression.getOperands();
            if (operator.equalsIgnoreCase("AND")) {
                // then get the children and remove the un filtered ones.
                ZExpression newExp = new ZExpression(operator);
                for (int i = 0; i < operands.size(); i++) {
                    ZExp zExp = (ZExp) operands.elementAt(i);
                    zExp = filterExpression(zExp, tables);
                    if (zExp != null) {
                        newExp.addOperand(zExp);
                    }
                }

                if (newExp.getOperands() == null) {
                    return null;
                } else {
                    if (newExp.getOperands().size() == 1) {
                        return (ZExp) newExp.getOperands().get(0);
                    } else
                        return newExp;
                }
            } else {
                ZExpression newExp = new ZExpression(operator);
                for (Iterator iterator = operands.iterator(); iterator.hasNext();) {
                    ZExp zExp = (ZExp) iterator.next();
                    ZExp zExp1 = filterExpression(zExp, tables);
                    if (zExp1 == null) {
                        return null;
                    } else {
                        newExp.addOperand(zExp1);
                    }
                }

                return newExp;
            }
        } else if (exp instanceof ZConstant) {
            ZConstant constant = (ZConstant) exp;
            if (constant.getType() == ZConstant.COLUMNNAME) {
                if (constant.getValue().equals("*") ||
                        tables.contains(ColumnsGatherer.getTableName(constant.getValue().toUpperCase()))) {
                    return exp;
                } else {
                    return null;
                }
            } else {
                return exp;
            }
        } else if (exp instanceof ZQuery) {
            return null;
        } else {
            throw new UnsupportedOperationException(String.valueOf(exp));
        }
    }

    private ZExp _removeSubQueries(ZExp exp, Set removedColumns) {
        if (exp instanceof ZQuery) {
            return null;
        } else if (exp instanceof ZExpression) {
            ZExpression expression = (ZExpression) exp;
            String operator = expression.getOperator();
            Vector v = expression.getOperands();
            Vector newOperands = new Vector(v.size());
            boolean isAggr = ZqlUtils.AGGR_OPER.contains(operator.toUpperCase());

            for (ListIterator iterator = v.listIterator(); iterator.hasNext();) {
                ZExp zExp = (ZExp) iterator.next();
                ZExp zExp1 = _removeSubQueries(zExp, removedColumns);
                if (zExp1 == null) {
                    if(!(zExp instanceof ZQuery))
                        removedColumns.addAll(ZqlUtils.getUsedColumns(zExp, false));
                    
                    if (!isAggr) {
                        return null;
                    }
                    iterator.remove();
                } else
                    newOperands.add(zExp1);
            }

            if (isAggr && newOperands.size() == 1) {
                return (ZExp) newOperands.get(0);
            } else {
                ZExpression expression1 = new ZExpression(operator);
                expression1.setOperands(newOperands);
                return expression1;
            }
        } else if (exp instanceof ZConstant) {
            ZConstant constant = (ZConstant) exp;
            if(constant.getType() == ZConstant.STRING) {
                Matcher matcher = DATE_PATTERN.matcher(constant.getValue());
                if(matcher.matches()) {
                    return null;
                }
            }

            return exp;
        } else
            return exp;
    }

    public Multimap associateColumnsWithTables(Set columns) {
        return ColumnsGatherer.associateColumnsWithTables(columns);
    }

    public static void main(String[] args) throws ParseException, IOException {
        WorkloadProcessor proc = new WorkloadProcessor(args[0]);
        for (Iterator<QueryDesc> iterator = proc.query_descriptors.iterator(); iterator.hasNext();) {
            QueryDesc queryDesc = iterator.next();
            ColumnsGatherer gath = new ColumnsGatherer(queryDesc.parsed_query);
            System.out.println("gath.getUsedColumns() = " + gath.getUsedColumns());
            System.out.println("gath.getInterestingOrders() = " + gath.getInterestingOrders());
            System.out.println("gath.getIndexableColumns() = " + gath.getIndexableColumns());
            System.out.println("gath.getGroupByColumns() = " + gath.getGroupByColumns());
            System.out.println("gath.getOrderByColumns() = " + gath.getOrderByColumns());
        }

        proc.generateCandidateIndexes();
        System.out.println("proc.candidates.si = " + proc.candidates.size());
            // cg.getInterestingOrders();
    }

    public List getCandidateIndexes() {
        return candidates;
    }

    public List getCandidateClusteredIndexes(String tableName) throws IOException {
        Index usedFields = universe.getFirstIndexForTable(tableName);
        Set<String> allFields = (Set<String>) ColumnsGatherer.GetColumnMap().get(tableName.toUpperCase());

        List candidates = new ArrayList();
        for (String field : (Set<String>) usedFields.getColumns()) {
            LinkedHashSet<String> set = new LinkedHashSet<String>();
            set.add(field);
            List<String> rest = new ArrayList();
            for (String field1 : allFields) {
                if (!field.equals(field1)) {
                    rest.add(field1);
                }
            }

            Collections.sort(rest);
            set.addAll(rest);

            Index idx = new Index(tableName, set);
            candidates.add(idx);
        }

        return candidates;
    }

    public int getMaxIndexLength() {
        return 15;
    }
}
