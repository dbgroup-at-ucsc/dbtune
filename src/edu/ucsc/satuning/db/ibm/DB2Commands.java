/*
 * ****************************************************************************
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
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */
package edu.ucsc.satuning.db.ibm;

import edu.ucsc.satuning.db.AbstractDatabaseConnectionManager;
import edu.ucsc.satuning.db.CostOfLevel;
import edu.ucsc.satuning.db.DefaultDatabaseConnection;
import edu.ucsc.satuning.spi.Command;
import edu.ucsc.satuning.spi.Parameter;
import edu.ucsc.satuning.util.BitSet;
import edu.ucsc.satuning.util.DBUtilities;
import edu.ucsc.satuning.util.Debug;
import edu.ucsc.satuning.util.Objects;
import edu.ucsc.satuning.util.Util;
import edu.ucsc.satuning.workload.SQLStatement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static edu.ucsc.satuning.util.DBUtilities.implode;
import static edu.ucsc.satuning.util.Util.newTreeSet;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class DB2Commands {
    private DB2Commands(){}

    public static Command<Integer, SQLException> clearExplainObject(){
        return makeIntegerCallback(
                "clearExplainObject", 
                "DELETE FROM explain_object"
        );
    }

    public static Command<Integer, SQLException> clearExplainStatement(){
        return makeIntegerCallback(
                "clearExplainStatement", 
                "DELETE FROM explain_statement"
        );
    }

    public static Command<Integer, SQLException> clearExplainPredicate(){
        return makeIntegerCallback(
                "clearExplainPredicate", 
                "DELETE FROM explain_predicate"
        );
    }

    public static Command<Integer, SQLException> clearExplainOperator(){
        return makeIntegerCallback(
                "clearExplainOperator", 
                "DELETE FROM explain_operator"
        );
    }

    public static Command<Integer, SQLException> clearAdviseIndex(){
        return makeIntegerCallback(
                "clearAdviseIndex", 
                "DELETE FROM advise_index"
        );
    }
    
    public static Command<Integer, SQLException> enableAdviseIndexRows(){
        return EnableAdviseIndexRows.INSTANCE;
    }

    public static Command<Void, SQLException> explainModeRecommendIndexes(){
        return makeCommand(
                "explainModeRecommendIndexes", 
                "SET CURRENT EXPLAIN MODE = RECOMMEND INDEXES"
        );
    }

    public static Command<Void, SQLException> explainModeEvaluateIndexes(){
        return makeCommand(
                "explainModeEvaluateIndexes", 
                "SET CURRENT EXPLAIN MODE = EVALUATE INDEXES"
        );
    }

    public static Command<Void, SQLException> explainModeNo(){
        return makeCommand(
                "explainModeNo", 
                "SET CURRENT EXPLAIN MODE = NO"
        );
    }

    public static Command<Void, SQLException> explainModeExplain(){
        return makeCommand(
                "explainModeExplain", 
                "SET CURRENT EXPLAIN MODE = EXPLAIN"
        );
    }

    public static Command<CostOfLevel, SQLException> fetchExplainStatementTotals(){
        return FetchExplainStatementTotals.INSTANCE;
    }

    public static Command<SQLStatement.SQLCategory, SQLException> fetchExplainStatementType(){
        return FetchExplainStatementTypeStatement.INSTANCE;
    }

    public static Command<Void, SQLException> fetchExplainObjectCandidates(){
        return FetchExplainObjectCandidates.INSTANCE;
    }


    public static Command<Double, SQLException> fetchExplainOpUpdateCost(){
        return FetchExplainOpUpdateCost.INSTANCE;
    }
    

    public static Command<String, SQLException> fetchExplainPredicateString(){
        return FetchExplainPredicateString.INSTANCE;
    }
    
    public static Command<QualifiedName, SQLException>  fetchExplainObjectUpdatedTable(){
        return FetchExplainObjectUpdatedTable.INSTANCE;
    }

    /**
     * @return a function that handles isolationLevelReadCommitted query.
     */
    public static Command<Void, SQLException> isolationLevelReadCommitted(){
        return makeCommand(
                "isolationLevelReadCommitted", 
                "SET ISOLATION READ COMMITTED"
        );
    }


    public static Command<Integer, SQLException> loadAdviseIndex(){
        return LoadAdviseIndex.INSTANCE;
    }

    private static Command<Void, SQLException> makeCommand(final String type, final String value){
        return DefaultCommand.makeNewCommand(type, value);
    }


    private static Command<Integer, SQLException> makeIntegerCallback(String type, String value){
        return IntegerCallback.makeIntegerCallback(type, value);
    }

    public static Command<List<DB2IndexMetadata>, SQLException> readAdviseOnAllIndexes(){
        return ReadAdviseOnAllIndexes.INSTANCE;
    }

    public static Command<DB2IndexMetadata, SQLException> readAdviseOnOneIndex(){
        return ReadAdviseOnOneIndex.INSTANCE;
    }
    
    // enum singleton pattern
    private enum EnableAdviseIndexRows implements Command<Integer, SQLException> {
        INSTANCE;
        @Override
        public Integer apply(Parameter input) throws SQLException {
            final Connection connection     = input.getParameterValue(DefaultDatabaseConnection.class).getJdbcConnection();
            final BitSet     configuration  = input.getParameterValue(BitSet.class);

            final Statement statement = connection.createStatement();

            final String sql = configuration.cardinality() == 0
                    ? "UPDATE advise_index SET use_index = 'N'"
                    : makeAlternateSQL(configuration);
            final int count = statement.executeUpdate(sql);
            connection.commit();
            return count;
        }

        private String makeAlternateSQL(BitSet config){
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
    
     private enum FetchExplainOpUpdateCost implements Command<Double, SQLException> {
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
             final Connection connection = input.getParameterValue(DefaultDatabaseConnection.class).getJdbcConnection();
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
                 
                Debug.println("updateCost = " + (updateOpCost - childOpCost));
                return updateOpCost - childOpCost;                 
             } finally {
                 rs.close();
                 connection.commit();
             }                          
         }
     }
    
    private enum FetchExplainPredicateString implements Command<String, SQLException> {
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
            Debug.assertion(tempPredicateSet.size() == 0, "tempPredicateSet was not cleared");
            final Connection connection = input.getParameterValue(DefaultDatabaseConnection.class).getJdbcConnection();
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
    private enum FetchExplainObjectUpdatedTable implements Command<QualifiedName, SQLException> {
        INSTANCE;
        private static final StringBuilder QUERY = new StringBuilder();
        static {
            QUERY.append("SELECT trim(object_schema), trim(object_name) ")
                 .append("FROM EXPLAIN_OBJECT ")
                 .append("WHERE OBJECT_TYPE = 'TA'");
        }
        
        private PreparedStatement ps;

        @Override
        public QualifiedName apply(Parameter input) throws SQLException {
            final Connection connection = input.getParameterValue(DefaultDatabaseConnection.class).getJdbcConnection();
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
                return new QualifiedName(dbName, schemaName, tableName);
            } finally {
                rs.close();
                connection.commit();
            }
        }
    }


    // enum singleton pattern
    private static class DefaultCommand implements Command<Void, SQLException> {
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
            final Connection        connection  = input.getParameterValue(DefaultDatabaseConnection.class).getJdbcConnection();
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
    private enum FetchExplainStatementTotals implements Command<CostOfLevel, SQLException> {
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
        public CostOfLevel apply(Parameter input) throws SQLException {
            final Connection        connection = input.getParameterValue(DefaultDatabaseConnection.class).getJdbcConnection();
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

            return CostOfLevel.valueOf(totalCost, count);
        }


        @Override
        public String toString() {
            return "FetchExplainStatementTotals";
        }
    }

    private enum FetchExplainObjectCandidates implements Command<Void, SQLException> {
        INSTANCE;
        private static final StringBuilder QUERY = new StringBuilder();
        static{
          QUERY.append("SELECT DISTINCT CAST(SUBSTRING(object_name FROM ")
               .append((DB2IndexMetadata.INDEX_NAME_BASE.length() + 1))
               .append(" USING CODEUNITS16) AS INT) ")
               .append("FROM EXPLAIN_OBJECT ")
               .append("WHERE OBJECT_NAME LIKE '")
               .append(DB2IndexMetadata.INDEX_NAME_BASE)
               .append("_%'");        
        }
        
        private PreparedStatement ps;

        @Override
        public Void apply(Parameter input) throws SQLException {
            final Connection c  = input.getParameterValue(DefaultDatabaseConnection.class).getJdbcConnection();
            final BitSet     b  = input.getParameterValue(BitSet.class);
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
    private enum FetchExplainStatementTypeStatement implements Command<SQLStatement.SQLCategory, SQLException> {
        INSTANCE;

        @Override
        public SQLStatement.SQLCategory apply(Parameter input) throws SQLException {
            final Connection connection = input.getParameterValue(DefaultDatabaseConnection.class).getJdbcConnection();
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
                return SQLStatement.SQLCategory.from(category);
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

    private static class IntegerCallback implements Command<Integer, SQLException> {
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
            final Connection        connection  = input.getParameterValue(DefaultDatabaseConnection.class).getJdbcConnection();
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

    private enum LoadAdviseIndex implements Command<Integer, SQLException>{
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
            final Connection         conn   = input.getParameterValue(DefaultDatabaseConnection.class).getJdbcConnection();
            if(!config.hasNext()) return 0;
            if(statement == null){
                statement = conn.createStatement();
            }

            final StringBuilder masterquery     = new StringBuilder();
            final StringBuilder cache           = new StringBuilder();
            if(cache.length() == 0){
                masterquery.append("INSERT INTO advise_index(");
                implode(masterquery, DB2IndexMetadata.AdviseIndexColumn.values(), ", ");
                masterquery.append(") VALUES ");
                cache.append(masterquery.toString());
            } else {
                masterquery.append(cache.toString());
            }

            while(config.hasNext()){
                final DB2Index idx = config.next();
                idx.meta.adviseIndexRowText(masterquery, enable);
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
    private enum ReadAdviseOnAllIndexes implements Command<List<DB2IndexMetadata>, SQLException> {
        INSTANCE;

        private static final StringBuilder QUERY_ALL = new StringBuilder();
        static {
            QUERY_ALL.append("SELECT ");
            DBUtilities.implode(QUERY_ALL, DB2IndexMetadata.AdviseIndexColumn.values(), ", ");
            QUERY_ALL.append(" FROM advise_index WHERE exists = 'N'");
        }

        private PreparedStatement psAll; // use all indexes in table

        @Override
        public List<DB2IndexMetadata> apply(Parameter input) throws SQLException {
            final Connection        conn  = input.getParameterValue(DefaultDatabaseConnection.class).getJdbcConnection();
            if(psAll == null){
                psAll = conn.prepareStatement(QUERY_ALL.toString());
            }

            final ResultSet rs = psAll.executeQuery();
            final AtomicInteger id = new AtomicInteger(0);
            final String dbName = input.getParameterValue(String.class);
            @SuppressWarnings({"unchecked"})
            final List<DB2IndexMetadata> candidateSet = Util.newList();
            try {
               while(rs.next()){
                   id.incrementAndGet();
                   candidateSet.add(DB2IndexMetadata.consFromAdviseIndex(rs, dbName, id.get(), -1));
               }
            } finally {
                rs.close();
                conn.commit();
            }
            return candidateSet;
        }


        @Override
        public String toString() {
            return "ReadAdviseIndex";
        }
    }

    // enum singleton pattern
    private enum ReadAdviseOnOneIndex implements Command<DB2IndexMetadata, SQLException> {
        INSTANCE;

        private static final StringBuilder QUERY_ONE = new StringBuilder();
        static {
            QUERY_ONE.append("SELECT ");
            DBUtilities.implode(QUERY_ONE, DB2IndexMetadata.AdviseIndexColumn.values(), ", ");
            QUERY_ONE.append(" FROM advise_index WHERE exists = 'N'");
            QUERY_ONE.append(" AND name = ?");
        }

        private PreparedStatement psOne; // use one index in table

        @Override
        public DB2IndexMetadata apply(Parameter input) throws SQLException {
            final DefaultDatabaseConnection<DB2Index> defaultDatabaseConnection = Objects.as(
                    input.getParameterValue(DefaultDatabaseConnection.class)
            );
            final String dbName = Objects.cast(
                    defaultDatabaseConnection.getConnectionManager(),
                    AbstractDatabaseConnectionManager.class
            ).getDatabase();

            final Connection  conn   = defaultDatabaseConnection.getJdbcConnection();
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
                Debug.assertion(haveResult, "did not find index " + indexName + " in ADVISE_INDEX");
                return DB2IndexMetadata.consFromAdviseIndex(rs, dbName, id, megabytes);
            } finally {
                rs.close();
                conn.commit();
            }            
        }


        @Override
        public String toString() {
            return "ReadAdviseIndex";
        }
    }
}
