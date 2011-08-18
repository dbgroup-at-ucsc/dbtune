/* ************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied  *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.DB2Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Strings;
import edu.ucsc.dbtune.util.ToStringBuilder;
import edu.ucsc.dbtune.spi.core.Function;
import edu.ucsc.dbtune.spi.core.Parameter;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.SQLCategory;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import java.util.Set;

import static edu.ucsc.dbtune.optimizer.DB2Optimizer.DB2Commands.*;
import static edu.ucsc.dbtune.spi.core.Functions.submit;
import static edu.ucsc.dbtune.spi.core.Functions.submitAll;
import static edu.ucsc.dbtune.spi.core.Functions.supplyValue;
import static edu.ucsc.dbtune.util.Instances.newTreeSet;

/**
 * Interface to the DB2 optimizer.
 *
 * @author Karl Schnaitter
 * @author Huascar Sanchez
 * @author Ivo Jimenez
 */
public class DB2Optimizer extends Optimizer
{
    private final Connection connection;
    private final String     databaseName;

    /**
     * Creates a DB2 optimizer with the given information.
     *
     * @param connection
     *     a live connection to DB2
     * @param databaseName
     *     the name of the database the connection is connected to
     */
    public DB2Optimizer(Connection connection, String databaseName){
        this.connection   = connection;
        this.databaseName = databaseName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedSQLStatement explain(SQLStatement sql, Configuration indexes) throws SQLException {
        optimizationCount++;
        Checks.checkSQLRelatedState(null != connection && !connection.isClosed(), "Connection is closed.");
        Checks.checkArgument(!Strings.isEmpty(sql.getSQL()), "Empty SQL statement");

        SQLCategory category     = null;
        double      updateCost   = 0.0;
        int         count        = 0;
        int         explainCount; // should be equal to 1
        double      totalCost;

        // a batch supplying of commands with no returned value.
        submitAll(
                // clear out the tables we'll be reading
                submit(clearExplainObject(), connection),
                submit(clearExplainStatement(), connection),
                submit(clearExplainOperator(), connection),
                // execute statment in explain mode = explain
                submit(explainModeExplain(), connection)
                );

        connection.createStatement().execute(sql.getSQL());
        submit(explainModeNo(), connection);
        category = supplyValue(fetchExplainStatementType(), connection);
        if(SQLCategory.DML.isSame(category)){
            supplyValue(fetchExplainObjectUpdatedTable(), connection);
            updateCost   = supplyValue(fetchExplainOpUpdateCost(), connection);
        }

        for(Index idx : indexes) {
            idx.getId();
            count++;
        }

        CostLevel costLevel;

        costLevel = supplyValue(fetchExplainStatementTotals(), connection);

        try {
            explainCount = costLevel.getCount();
            totalCost    = costLevel.getTotalCost();
        } catch (Throwable e) {
            throw new SQLException(e);
        }

        Checks.checkSQLRelatedState(
                explainCount == 1,
                "Error: Unexpected number of statements: "
                        + explainCount
                        + " (expected 1)"
        );

        submit(fetchExplainObjectCandidates(), connection, new IndexBitSet());
        sql.setSQLCategory(getStatementType(connection));

        double[] updateCosts  = new double[count];
        Arrays.fill(updateCosts, updateCost);

        return new PreparedSQLStatement(sql, totalCost, indexes);
    }

    private SQLCategory getStatementType(Connection connection) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(
                "SELECT TRIM(STATEMENT_TYPE) AS TYPE "
                + "FROM explain_statement "
                + "WHERE explain_level = 'P'"
                );

        ResultSet rs = ps.executeQuery();
        try {
            if(!rs.next()){
                throw new SQLException("could not derive stmt type: no rows");
            }

            String category = rs.getString(1);
            return SQLCategory.from(category);
        } finally {
            rs.close();
            connection.commit();
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration recommendIndexes(SQLStatement sql) throws SQLException {
        Checks.checkSQLRelatedState(null != connection && !connection.isClosed(), "Connection is closed.");
        List<Index> indexList;

        submitAll(
                submit(clearAdviseIndex(), connection),
                submit(explainModeRecommendIndexes(), connection)
                );
        try {
            Statement stmt = connection.createStatement();
            stmt.execute(sql.getSQL());
            stmt.close();
        } catch (SQLException ignore) {
        }

        indexList = supplyValue(readAdviseOnAllIndexes(), connection, databaseName);
        submit(clearAdviseIndex(), connection);

        return new Configuration(indexList);
    }

    public static class DB2Commands {

        private DB2Commands(){}

        public static Function<Integer, SQLException> clearExplainObject(){
            return makeIntegerCallback(
                    "clearExplainObject", 
                    "DELETE FROM explain_object"
                    );
        }

        public static Function<Integer, SQLException> clearExplainStatement(){
            return makeIntegerCallback(
                    "clearExplainStatement", 
                    "DELETE FROM explain_statement"
                    );
        }

        public static Function<Integer, SQLException> clearExplainPredicate(){
            return makeIntegerCallback(
                    "clearExplainPredicate", 
                    "DELETE FROM explain_predicate"
                    );
        }

        public static Function<Integer, SQLException> clearExplainOperator(){
            return makeIntegerCallback(
                    "clearExplainOperator", 
                    "DELETE FROM explain_operator"
                    );
        }

        public static Function<Integer, SQLException> clearAdviseIndex(){
            return makeIntegerCallback(
                    "clearAdviseIndex", 
                    "DELETE FROM advise_index"
                    );
        }

        public static Function<Integer, SQLException> enableAdviseIndexRows(){
            return EnableAdviseIndexRows.INSTANCE;
        }

        public static Function<Void, SQLException> explainModeRecommendIndexes(){
            return makeCommand(
                    "explainModeRecommendIndexes", 
                    "SET CURRENT EXPLAIN MODE = RECOMMEND INDEXES"
                    );
        }

        public static Function<Void, SQLException> explainModeEvaluateIndexes(){
            return makeCommand(
                    "explainModeEvaluateIndexes", 
                    "SET CURRENT EXPLAIN MODE = EVALUATE INDEXES"
                    );
        }

        public static Function<Void, SQLException> explainModeNo(){
            return makeCommand(
                    "explainModeNo", 
                    "SET CURRENT EXPLAIN MODE = NO"
                    );
        }

        public static Function<Void, SQLException> explainModeExplain(){
            return makeCommand(
                    "explainModeExplain", 
                    "SET CURRENT EXPLAIN MODE = EXPLAIN"
                    );
        }

        public static Function<DB2Optimizer.CostLevel, SQLException> fetchExplainStatementTotals(){
            return FetchExplainStatementTotals.INSTANCE;
        }

        public static Function<SQLCategory, SQLException> fetchExplainStatementType(){
            return FetchExplainStatementTypeStatement.INSTANCE;
        }

        public static Function<Void, SQLException> fetchExplainObjectCandidates(){
            return FetchExplainObjectCandidates.INSTANCE;
        }


        public static Function<Double, SQLException> fetchExplainOpUpdateCost(){
            return FetchExplainOpUpdateCost.INSTANCE;
        }


        public static Function<String, SQLException> fetchExplainPredicateString(){
            return FetchExplainPredicateString.INSTANCE;
        }

        public static Function<Table, SQLException> fetchExplainObjectUpdatedTable(){
            return FetchExplainObjectUpdatedTable.INSTANCE;
        }

        /**
         * @return a function that handles isolationLevelReadCommitted query.
         */
        public static Function<Void, SQLException> isolationLevelReadCommitted(){
            return makeCommand(
                    "isolationLevelReadCommitted", 
                    "SET ISOLATION READ COMMITTED"
                    );
        }


        public static Function<Integer, SQLException> loadAdviseIndex(){
            return LoadAdviseIndex.INSTANCE;
        }

        private static Function<Void, SQLException> makeCommand(final String type, final String value){
            return DefaultCommand.makeNewCommand(type, value);
        }


        private static Function<Integer, SQLException> makeIntegerCallback(String type, String value){
            return IntegerCallback.makeIntegerCallback(type, value);
        }

        public static Function<List<Index>, SQLException> readAdviseOnAllIndexes(){
            return ReadAdviseOnAllIndexes.INSTANCE;
        }

        public static Function<DB2Index, SQLException> readAdviseOnOneIndex(){
            return ReadAdviseOnOneIndex.INSTANCE;
        }

        // enum singleton pattern
        private static enum EnableAdviseIndexRows implements Function<Integer, SQLException> {
            INSTANCE;
            @Override
                public Integer apply(Parameter input) throws SQLException {
                    final Connection connection =
                        input.getParameterValue(Connection.class);
                    final IndexBitSet configuration  = input.getParameterValue(IndexBitSet.class);

                    final Statement statement = connection.createStatement();

                    final String sql = configuration.cardinality() == 0
                        ? "UPDATE advise_index SET use_index = 'N'"
                        : makeAlternateSQL(configuration);
                    final int count = statement.executeUpdate(sql);
                    connection.commit();
                    return count;
                }

            private String makeAlternateSQL(IndexBitSet config){
                final StringBuilder builder = new StringBuilder();
                builder.append("UPDATE advise_index SET use_index = CASE WHEN iid IN (");
                boolean first = true;
                for (int i = config.nextSetBit(0); i >= 0; i = config.nextSetBit(i+1)) {
                    if (!first)
                        builder.append(", ");
                    builder.append(i);
                    first = false;
                }

                builder.append(") THEN 'Y' ELSE 'N' END");
                return builder.toString();
            }

            @Override
                public String toString() {
                    return "enableAdviseIndexRows";
                }
        }

        private static enum FetchExplainOpUpdateCost implements Function<Double, SQLException> {
            INSTANCE;

            private static final StringBuilder QUERY = new StringBuilder();
            static{          
                QUERY.append("SELECT TOTAL_COST ")
                    .append("FROM EXPLAIN_OPERATOR ")
                    .append("WHERE OPERATOR_ID = 2 OR OPERATOR_ID = 3 ")
                    .append("ORDER BY OPERATOR_ID");        
            }

            private PreparedStatement ps;

            @Override
                public Double apply(Parameter input) throws SQLException {
                    final Connection connection = input.getParameterValue(Connection.class);
                    if(ps == null){
                        ps = connection.prepareStatement(QUERY.toString());
                    }

                    final ResultSet rs = ps.executeQuery();
                    try {
                        if (!rs.next()){
                            throw new SQLException("Could not get update cost: no rows");
                        }

                        final double updateOpCost = rs.getDouble(1);
                        if (!rs.next()){
                            throw new SQLException("Could not get update cost: only one row");
                        }

                        final double childOpCost = rs.getDouble(1);
                        if (rs.next()){
                            throw new SQLException("Could not get update cost: too many rows");
                        }

                        return updateOpCost - childOpCost;                 
                    } finally {
                        rs.close();
                        connection.commit();
                    }                          
                }
        }

        private static enum FetchExplainPredicateString implements Function<String, SQLException> {
            INSTANCE;
            private static final StringBuilder QUERY = new StringBuilder();
            static {
                QUERY.append("SELECT p.predicate_text ")
                    .append("FROM explain_predicate p, explain_operator o ")
                    .append("WHERE o.operator_id=p.operator_id AND o.operator_type='IXSCAN'");
            }

            private PreparedStatement ps;
            private final Set<String> tempPredicateSet = newTreeSet();

            @Override
                public String apply(Parameter input) throws SQLException {
                    Checks.checkAssertion(tempPredicateSet.size() == 0, "tempPredicateSet was not cleared");
                    final Connection connection = input.getParameterValue(Connection.class);
                    if(ps == null){
                        ps = connection.prepareStatement(QUERY.toString());
                    }


                    // put predicates into tempPredicateSet in order to eliminate duplicates
                    // we can't do DISTINCT in the SQL because of DB2 limitations on data types
                    // I haven't found an easy in-database workaround 
                    final ResultSet rs = ps.executeQuery();
                    try {
                        while(rs.next()){
                            tempPredicateSet.add(rs.getString(1));
                        }
                    } finally {
                        rs.close();
                        connection.commit();
                    }

                    final StringBuilder returnValue = new StringBuilder(tempPredicateSet.size() * 10); // cheating the JVM to gain a little bit of performance.
                    for(String predicate : tempPredicateSet){
                        if(returnValue.length() > 0){
                            returnValue.append(' ');
                        }

                        returnValue.append(predicate);
                    }

                    tempPredicateSet.clear();

                    return returnValue.toString();
                }
        }

        // enum singleton pattern.
        private static enum FetchExplainObjectUpdatedTable implements Function<Table, SQLException> 
        {
            INSTANCE;
            private static final StringBuilder QUERY = new StringBuilder();
            static {
                QUERY.append("SELECT trim(object_schema), trim(object_name) ")
                    .append("FROM EXPLAIN_OBJECT ")
                    .append("WHERE OBJECT_TYPE = 'TA'");
            }

            private PreparedStatement ps;

            @Override
                public Table apply(Parameter input) throws SQLException {
                    final Connection connection = input.getParameterValue(Connection.class);
                    if(ps == null){
                        ps = connection.prepareStatement(QUERY.toString());
                    }

                    final ResultSet rs = ps.executeQuery();
                    try {
                        if(!rs.next()){
                            throw new SQLException("Could not get updated table: no rows");                    
                        }

                        final String schemaName = rs.getString(1);
                        final String tableName  = rs.getString(2);
                        if(rs.next()){
                            throw new SQLException("Could not get updated table: too many rows");
                        }

                        final String dbName = input.getParameterValue(String.class);
                        return new Table(dbName, schemaName, tableName);
                    } finally {
                        rs.close();
                        connection.commit();
                    }
                }
        }


        // enum singleton pattern
        private static class DefaultCommand implements Function<Void, SQLException> {
            private String type;
            private String value;
            DefaultCommand(String type, String value){
                this.type  = type;
                this.value = value;
            }

            static DefaultCommand makeNewCommand(String type, String value){
                return new DefaultCommand(type, value);
            }


            @Override
                public Void apply(Parameter input) throws SQLException {
                    final Connection        connection  = input.getParameterValue(Connection.class);
                    final PreparedStatement ps          = connection.prepareStatement(value);
                    if(ps.execute()){
                        throw new SQLException("Command returned unexpected result set");
                    }
                    return null;
                }

            @Override
                public String toString() {
                    return type;
                }
        }

        // enum singleton pattern
        private static enum FetchExplainStatementTotals implements Function<DB2Optimizer.CostLevel, 
                SQLException> {
            INSTANCE;
            private static final StringBuilder QUERY = new StringBuilder();
            static {
                QUERY.append("SELECT TOTAL_COST ")
                    .append("FROM EXPLAIN_OPERATOR ")
                    .append("WHERE OPERATOR_ID = 2 OR OPERATOR_ID = 3 ")
                    .append("ORDER BY OPERATOR_ID");
            }
            private PreparedStatement ps;
            @Override
                public DB2Optimizer.CostLevel apply(Parameter input) throws SQLException {
                    final Connection        connection = input.getParameterValue(Connection.class);
                    if(ps == null){
                        ps = connection.prepareStatement(QUERY.toString());
                    }

                    final ResultSet rs = ps.executeQuery();
                    double totalCost    = 0.0;
                    int    count        = 0;
                    try {
                        rs.next();
                        totalCost   = rs.getDouble(1);
                        count       = rs.getInt(2);
                    } finally {
                        rs.close();
                        connection.commit();
                    }

                    return DB2Optimizer.CostLevel.valueOf(totalCost, count);
                }


            @Override
                public String toString() {
                    return "FetchExplainStatementTotals";
                }
        }

        private static enum FetchExplainObjectCandidates implements Function<Void, SQLException> {
            INSTANCE;
            private static final StringBuilder QUERY = new StringBuilder();
            static{
                QUERY.append("SELECT DISTINCT CAST(SUBSTRING(object_name FROM ")
                    .append((DB2Index.DB2IndexMetadata.INDEX_NAME_BASE.length() + 1))
                    .append(" USING CODEUNITS16) AS INT) ")
                    .append("FROM EXPLAIN_OBJECT ")
                    .append("WHERE OBJECT_NAME LIKE '")
                    .append(DB2Index.DB2IndexMetadata.INDEX_NAME_BASE)
                    .append("_%'");        
            }

            private PreparedStatement ps;

            @Override
                public Void apply(Parameter input) throws SQLException {
                    final Connection c  = input.getParameterValue(Connection.class);
                    final IndexBitSet b  = input.getParameterValue(IndexBitSet.class);
                    if(ps == null){
                        ps = c.prepareStatement(QUERY.toString());
                    }

                    final ResultSet         rs = ps.executeQuery();

                    try {
                        while(rs.next()){
                            b.set(rs.getInt(1));
                        }
                    } finally {
                        rs.close();
                        c.commit();
                    }
                    return null; // don't return anything.
                }
        }

        // enum singleton pattern
        private static enum FetchExplainStatementTypeStatement implements Function<SQLCategory, 
                SQLException> {
            INSTANCE;

            @Override
                public SQLCategory apply(Parameter input) throws SQLException {
                    final Connection connection = input.getParameterValue(Connection.class);
                    final PreparedStatement ps = connection.prepareStatement(
                            "SELECT TRIM(STATEMENT_TYPE) AS TYPE "
                            + "FROM explain_statement "
                            + "WHERE explain_level = 'P'"
                            );

                    final ResultSet rs = ps.executeQuery();
                    try {
                        if(!rs.next()){
                            throw new SQLException("could not derive stmt type: no rows");
                        }

                        final String category = rs.getString(1);
                        ensureCategoryExist(category);
                        ensureOneOneRecord(rs);
                        return SQLCategory.from(category);
                    } finally {
                        rs.close();
                        connection.commit();
                    }
                }

            private static void ensureCategoryExist(String category) throws SQLException {
                if (category.length() == 0) {
                    throw new SQLException("could not derive stmt type: empty type");
                }
            }

            private static void ensureOneOneRecord(ResultSet rs) throws SQLException {
                if (rs.next()) {
                    throw new SQLException("could not derive stmt type: too many rows");
                }
            }


            @Override
                public String toString() {
                    return "fetchExplainStatementType";
                }
        }

        private static class IntegerCallback implements Function<Integer, SQLException> {
            private String type;
            private String value;
            IntegerCallback(String type, String value){
                this.type = type;
                this.value = value;
            }

            static IntegerCallback makeIntegerCallback(String type, String value){
                return new IntegerCallback(type, value);
            }

            @Override
                public Integer apply(Parameter input) throws SQLException {
                    final Connection        connection  = input.getParameterValue(Connection.class);
                    final PreparedStatement ps          = connection.prepareStatement(value);
                    final int               count       = ps.executeUpdate();
                    connection.commit();
                    return count;
                }

            @Override
                public String toString() {
                    return type;
                }
        }

        private static enum LoadAdviseIndex implements Function<Integer, SQLException> {
            INSTANCE;

            private Statement statement;
            LoadAdviseIndex(){
                statement = null;
            }

            @Override
                public Integer apply(Parameter input) throws SQLException {
                    @SuppressWarnings({"unchecked"})
                        final Iterator<DB2Index> config = input.getParameterValue(Iterator.class);
                    final boolean            enable = input.getParameterValue(Boolean.class);
                    final Connection         conn   = input.getParameterValue(Connection.class);
                    if(!config.hasNext()) return 0;
                    if(statement == null){
                        statement = conn.createStatement();
                    }

                    final StringBuilder masterquery     = new StringBuilder();
                    final StringBuilder cache           = new StringBuilder();
                    if(cache.length() == 0){
                        masterquery.append("INSERT INTO advise_index(");
                        Strings.implode(masterquery, DB2Index.DB2IndexMetadata.AdviseIndexColumn.values(), ", ");
                        masterquery.append(") VALUES ");
                        cache.append(masterquery.toString());
                    } else {
                        masterquery.append(cache.toString());
                    }

                    while(config.hasNext()){
                        final DB2Index idx = config.next();
                        idx.getMeta().adviseIndexRowText(masterquery, enable);
                        if (config.hasNext()){
                            masterquery.append(", ");
                        }

                    }

                    final int count = statement.executeUpdate(masterquery.toString());
                    conn.commit();
                    return count;
                }
        }


        // enum singleton pattern
        private static enum ReadAdviseOnAllIndexes implements Function<List<Index>, SQLException> {
            INSTANCE;

            private static final StringBuilder QUERY_ALL = new StringBuilder();
            static {
                QUERY_ALL.append("SELECT ");
                Strings.implode(QUERY_ALL, DB2Index.DB2IndexMetadata.AdviseIndexColumn.values(), ", ");
                QUERY_ALL.append(" FROM advise_index WHERE exists = 'N'");
            }

            //private PreparedStatement psAll; // use all indexes in table

            @Override
            public List<Index> apply(Parameter input) throws SQLException {
                throw new SQLException("not implemented yet"); // will fix in issue #64
                /*
                final Connection        conn  = input.getParameterValue(Connection.class);
                if(psAll == null){
                    psAll = conn.prepareStatement(QUERY_ALL.toString());
                }

                final ResultSet rs = psAll.executeQuery();
                final AtomicInteger id = new AtomicInteger(0);
                final String dbName = input.getParameterValue(String.class);
                final List<Index> candidateSet = Instances.newList();
                try {
                    while(rs.next()){
                        id.incrementAndGet();
                        //candidateSet.add(new 
                        //DB2Index(input.getParameterValue(Connection.class), rs, dbName, 
                        //id.get(), -1));
                    }
                } catch(Exception ex) {
                    throw new SQLException(ex);
                } finally {
                    rs.close();
                    conn.commit();
                }
                return candidateSet;
                */
            }


            @Override
                public String toString() {
                    return "ReadAdviseIndex";
                }
        }

        // enum singleton pattern
        private static enum ReadAdviseOnOneIndex implements Function<DB2Index, SQLException> {
            INSTANCE;

            private static final StringBuilder QUERY_ONE = new StringBuilder();
            static {
                QUERY_ONE.append("SELECT ");
                Strings.implode(QUERY_ONE, DB2Index.DB2IndexMetadata.AdviseIndexColumn.values(), ", ");
                QUERY_ONE.append(" FROM advise_index WHERE exists = 'N'");
                QUERY_ONE.append(" AND name = ?");
            }

            private PreparedStatement psOne; // use one index in table

            @Override
            public DB2Index apply(Parameter input) throws SQLException {
                /*
                final Connection conn = input.getParameterValue(Connection.class)
                    ;
                if(psOne == null){
                    psOne = conn.prepareStatement(QUERY_ONE.toString());
                }
                final String indexName = input.getParameterValue(String.class);
                psOne.setString(1, indexName);

                final ResultSet rs          = psOne.executeQuery();
                final Integer   id          = input.getParameterValue(Integer.class);
                final Double    megabytes   = input.getParameterValue(Double.class);

                try {
                    final boolean haveResult = rs.next();
                    Checks.checkAssertion(haveResult, "did not find index " + indexName + " in ADVISE_INDEX");
                    return new DB2Index(defaultDatabaseConnection, rs, dbName, id, megabytes);
                } catch(Exception ex) {
                    throw new SQLException(ex);
                } finally {
                    rs.close();
                    conn.commit();
                }
                */
                throw new SQLException("not implemented yet"); // will fix in issue #64
            }
        }
    }
    public static class CostLevel {
        private final double    totalCost;
        private final int       count;
        private final boolean   initialized;

        public CostLevel(double totalCost, int count, boolean initialized) {
            this.totalCost   = totalCost;
            this.count       = count;
            this.initialized = initialized;
        }

        /**
         * create an immutable {@code CostOfLevel} instance.
         * @param totalCost
         *      total cost of level.
         * @param count
         *      # of levels
         * @return
         *      a new immutable cost level object.
         */
        public static CostLevel valueOf(double totalCost, int count) {
            return new CostLevel(totalCost, count, (!(totalCost == 0.0 || count == 0)));
        }

        void ensureInitialization(){
            if(!isInitialized()){
                throw new IllegalStateException();
            }
        }

        public double getTotalCost() {
            ensureInitialization();
            return totalCost;
        }

        public int getCount() {
            ensureInitialization();
            return count;
        }

        public boolean isInitialized(){
            return initialized;
        }

        @Override
            public String toString() {
                if(!isInitialized()) return "CostLevel(...)";
                return new ToStringBuilder<CostLevel>(this)
                    .add("total cost", getTotalCost())
                    .add("count", getCount())
                    .toString();
            }
    }
}
