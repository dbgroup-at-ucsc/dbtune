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

package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.core.metadata.*;
import edu.ucsc.dbtune.core.metadata.PGReifiedTypes.ReifiedPGIndexList;
import edu.ucsc.dbtune.core.optimizers.WhatIfOptimizationBuilder;
import edu.ucsc.dbtune.util.DefaultBitSet;
import edu.ucsc.dbtune.util.*;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static edu.ucsc.dbtune.core.metadata.DB2Commands.*;
import static edu.ucsc.dbtune.core.metadata.PGCommands.explainIndexes;
import static edu.ucsc.dbtune.core.metadata.PGCommands.explainIndexesCost;
import static edu.ucsc.dbtune.spi.core.Commands.*;
import static edu.ucsc.dbtune.util.DBUtilities.trimSqlStatement;
import static edu.ucsc.dbtune.util.Instances.newAtomicInteger;
import static edu.ucsc.dbtune.util.Instances.newList;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Platform {
    private static final Map<String, DatabaseIndexExtractorFactory<?>> AVAILABLE_EXTRACTORS;
    private static final Map<String, DatabaseWhatIfOptimizerFactory<?>> AVAILABLE_OPTIMIZERS;
    static {
        Map<String, DatabaseIndexExtractorFactory<?>> driverToExtractor =
                new HashMap<String, DatabaseIndexExtractorFactory<?>>(){
                    {
                        put("com.ibm.db2.jcc.DB2Driver", new DB2IndexExtractorFactory());
                        put("org.postgresql.Driver", new PGIndexExtractorFactory());
                    }
                };
        Map<String, DatabaseWhatIfOptimizerFactory<?>> driverToOptimizer =
                new HashMap<String, DatabaseWhatIfOptimizerFactory<?>>(){
                    {
                        put("com.ibm.db2.jcc.DB2Driver", new DB2WhatIfOptimizerFactory());
                        put("org.postgresql.Driver", new PGWhatIfOptimizerFactory());
                    }
                };

        AVAILABLE_EXTRACTORS = Collections.unmodifiableMap(driverToExtractor);
        AVAILABLE_OPTIMIZERS = Collections.unmodifiableMap(driverToOptimizer);
    }

    /**
     * utility class.
     */
    private Platform(){}

    /**
     * finds the appropriate {@link edu.ucsc.dbtune.core.DatabaseIndexExtractorFactory} strategy for a given driver. This method
     * will throw a {@link NullPointerException} if strategy is not found. We rather deal with problem
     * right away (i.e., throwing a NullPointerException) rather than waiting for some side affect along the execution line.
     *
     * @param driver
     *      dbms-driver's fully qualified name.
     * @param <I>
     *      type of {@link DBIndex}.
     * @return
     *      a found {@link edu.ucsc.dbtune.core.DatabaseIndexExtractorFactory} pre-instantiated instance.
     * @throws NullPointerException
     *      if strategy is not found.
     */
    public static <I extends DBIndex<I>> DatabaseIndexExtractorFactory<I> findIndexExtractorFactory(String driver){
        return  Objects.as(PreConditions.checkNotNull(AVAILABLE_EXTRACTORS.get(driver)));
    }

    /**
     * finds the appropriate {@link edu.ucsc.dbtune.core.DatabaseWhatIfOptimizerFactory} strategy for a given driver. This method
     * will throw a {@link NullPointerException} if strategy is not found. We rather deal with problem
     * right away (i.e., throwing a NullPointerException) rather than waiting for some side affect along the execution line.
     *
     * @param driver
     *      dbms-driver's fully qualified name.
     * @param <I>
     *      type of {@link DBIndex}.
     * @return
     *      a found {@link edu.ucsc.dbtune.core.DatabaseWhatIfOptimizerFactory} pre-instantiated instance.
     * @throws NullPointerException
     *      if strategy is not found.
     */
    public static <I extends DBIndex<I>> DatabaseWhatIfOptimizerFactory<I> findWhatIfOptimizerFactory(String driver){
        return Objects.as(PreConditions.checkNotNull(AVAILABLE_OPTIMIZERS.get(driver)));
    }

    private static class DB2IndexExtractorFactory implements DatabaseIndexExtractorFactory<DB2Index> {
        @Override
        public DatabaseIndexExtractor<DB2Index> makeIndexExtractor(DatabaseConnection<DB2Index> connection) {
            return new DB2DatabaseIndexExtractor("/home/karlsch/sqllib/bin/db2advis", PreConditions.checkNotNull(connection));
        }
    }

    private static class PGIndexExtractorFactory implements DatabaseIndexExtractorFactory<PGIndex> {
        @Override
        public DatabaseIndexExtractor<PGIndex> makeIndexExtractor(DatabaseConnection<PGIndex> connection) {
            return new PGDatabaseIndexExtractor(PreConditions.checkNotNull(connection));
        }
    }

    private static class DB2WhatIfOptimizerFactory implements DatabaseWhatIfOptimizerFactory<DB2Index> {
        @Override
        public DatabaseWhatIfOptimizer<DB2Index> makeWhatIfOptimizer(DatabaseConnection<DB2Index> connection) {
            return new DB2DatabaseWhatIfOptimizer(PreConditions.checkNotNull(connection));
        }
    }

    private static class PGWhatIfOptimizerFactory implements DatabaseWhatIfOptimizerFactory<PGIndex> {
        @Override
        public DatabaseWhatIfOptimizer<PGIndex> makeWhatIfOptimizer(DatabaseConnection<PGIndex> connection) {
            return new PGDatabaseWhatIfOptimizer(PreConditions.checkNotNull(connection));
        }
    }

    static class PGDatabaseWhatIfOptimizer extends AbstractDatabaseWhatIfOptimizer<PGIndex> {
        private final DatabaseConnection<PGIndex> connection;
        private final AtomicInteger                                     whatifCount;
        private DefaultBitSet cachedBitSet = new DefaultBitSet();
        private List<PGIndex>            cachedCandidateSet = new ArrayList<PGIndex>();


        PGDatabaseWhatIfOptimizer(DatabaseConnection<PGIndex> connection){
            super();
            this.connection = connection;
            whatifCount     = newAtomicInteger();
        }

        @Override
        public void fixCandidates(Iterable<PGIndex> candidateSet) throws SQLException {
            if(!isEnabled()) throw new SQLException("IndexExtractor is Disabled.");
            cachedCandidateSet.clear();
            cachedBitSet.clear();
            for (PGIndex idx : candidateSet) {
                cachedCandidateSet.add(idx);
                cachedBitSet.set(idx.internalId());
            }
        }

        @Override
        public ExplainInfo<PGIndex> explainInfo(String sql) throws SQLException {
            if(!isEnabled()) throw new SQLException("IndexExtractor is Disabled.");
            incrementWhatIfCounter();
            final ReifiedPGIndexList indexSet = new ReifiedPGIndexList();
            for(PGIndex each : cachedCandidateSet){
                indexSet.add(each);
            }
            final Double[] maintCost = new Double[cachedBitSet.length()];
            return supplyValue(
                    explainIndexes(),
                    connection,
                    indexSet,
                    cachedBitSet,
                    sql,
                    cachedBitSet.cardinality(),
                    maintCost
            );
        }

        private void incrementWhatIfCounter(){
            Objects.<AbstractDatabaseWhatIfOptimizer<PGIndex>>as(
                    connection.getWhatIfOptimizer()
            ).incrementWhatIfCount();
        }


        @Override
        public Iterable<PGIndex> getCandidateSet() {
            return cachedCandidateSet;
        }

        @Override
        protected void incrementWhatIfCount() {
            whatifCount.incrementAndGet();
        }

        @Override
        protected Double runWhatIfTrial(WhatIfOptimizationBuilder<PGIndex> pgIndexWhatIfOptimizationBuilder) throws SQLException {
            incrementWhatIfCount();
            Debug.print(".");
            if (whatifCount.get() % 75 == 0) {
                Debug.println();
            }

            final WhatIfOptimizationBuilderImpl<DB2Index> whatIfImpl = Objects.as(
                    pgIndexWhatIfOptimizationBuilder
            );

            if(whatIfImpl.withProfiledIndex()){
                throw new UnsupportedOperationException(
                        "whatifOptimize cannot give used columns in PGConnection"
                );
            }

            final String             sql             = whatIfImpl.getSQL();
            final DefaultBitSet usedSet         = whatIfImpl.getUsedSet();
            final DefaultBitSet configuration   = whatIfImpl.getConfiguration();
            final ReifiedPGIndexList indexSet        = getCandidateIndexSet();

            return supplyValue(
                    explainIndexesCost(usedSet),
                    connection,
                    indexSet,
                    configuration,
                    sql,
                    configuration.cardinality(),
                    new Double[indexSet.size()]
            );
        }

        private ReifiedPGIndexList getCandidateIndexSet(){
            final ReifiedPGIndexList indexSet = new ReifiedPGIndexList();
            for(PGIndex each : cachedCandidateSet){
                indexSet.add(each);
            }

            return indexSet;
        }



        @Override
        public int getWhatIfCount() {
            return whatifCount.get();
        }
    }

    /**
     * A DB2-specific optimizer.
     */
    static class DB2DatabaseWhatIfOptimizer extends AbstractDatabaseWhatIfOptimizer<DB2Index> {
        private final DatabaseConnection<DB2Index> connection;
        private final AtomicInteger                                      whatifCount;
        private       Iterable<DB2Index>            cachedCandidateSet = new LinkedList<DB2Index>();


        DB2DatabaseWhatIfOptimizer(DatabaseConnection<DB2Index> connection){
            super();
            this.connection = connection;
            whatifCount     = newAtomicInteger();
        }

        @Override
        public Iterable<DB2Index> getCandidateSet() {
            return cachedCandidateSet;
        }

        @Override
        protected void incrementWhatIfCount() {
            whatifCount.incrementAndGet();
        }

        @Override
        public ExplainInfo<DB2Index> explainInfo(String sql) throws SQLException {
            PreConditions.checkSQLRelatedState(
                    isEnabled(),
                    "We cannot use this extractor; its owner connection has been closed."
            );
            SQLStatement.SQLCategory category     = null;
            DB2QualifiedName updatedTable = null;
            double                   updateCost   = 0.0;
            final JdbcDatabaseConnection<DB2Index> c = Objects.as(connection);

            try {
                // a batch supplying of commands with no returned value.
                submitAll(
                        // clear out the tables we'll be reading
                        submit(clearExplainObject(), c),
                        submit(clearExplainStatement(), c),
                        submit(clearExplainOperator(), c),
                        // execute statment in explain mode = explain
                        submit(explainModeExplain(), c)
                );

                c.execute(sql);
                submit(explainModeNo(), c);
                category = supplyValue(fetchExplainStatementType(), c);
                if(category == SQLStatement.SQLCategory.DML){
                    updatedTable = supplyValue(fetchExplainObjectUpdatedTable(), c);
                    updateCost   = supplyValue(fetchExplainOpUpdateCost(), c);
                }
            } catch (RuntimeException e){
                c.rollback();
            }

            try {
                c.rollback();
            } catch (SQLException s){
                Debug.logError("Could not rollback transaction", s);
            }

            return new DB2ExplainInfo(category, updatedTable, updateCost);
        }

        @Override
        public void fixCandidates(Iterable<DB2Index> candidateSet) throws SQLException {
            PreConditions.checkSQLRelatedState(
                    isEnabled(),
                    "We cannot use this extractor; its owner connection has been closed."
            );
            final JdbcDatabaseConnection<DB2Index> c = Objects.as(connection);
            cachedCandidateSet = candidateSet;

            submitAll(
                    submit(clearAdviseIndex(), c),
                    submit(loadAdviseIndex(), candidateSet.iterator(), false, c)
            );
        }

        @Override
        protected Double runWhatIfTrial(
                WhatIfOptimizationBuilder<DB2Index> db2IndexWhatIfOptimizationBuilder
        ) throws SQLException {

            if (whatifCount.incrementAndGet() % 80 == 0) {
                fixAnyExistingCandidates();
                System.err.println();
            }

            final WhatIfOptimizationBuilderImpl<DB2Index> whatIfImpl = Objects.as(
                    db2IndexWhatIfOptimizationBuilder
            );

            final JdbcDatabaseConnection<DB2Index> c =  Objects.as(connection);

            if(!whatIfImpl.withProfiledIndex()){
               return runWhatIfOptimizationWithoutProfiledIndex(whatIfImpl, c);
            }  else {
               return runWhatIfOptimizationWithProfiledIndex(whatIfImpl, c);
            }

        }

        private void fixAnyExistingCandidates() throws SQLException {
            fixCandidates(cachedCandidateSet);
        }

        @SuppressWarnings({"UnusedAssignment"})
        private static Double runWhatIfOptimizationWithProfiledIndex(
                WhatIfOptimizationBuilderImpl<DB2Index> whatIfImpl,
                JdbcDatabaseConnection<DB2Index> activeConnection
        ) throws SQLException {
            int explainCount; // should be equal to 1
            double totalCost;

            final String        sql             = whatIfImpl.getSQL();
            final DB2Index      profiledIndex   = whatIfImpl.getProfiledIndex();
            final DefaultBitSet usedColumns     = whatIfImpl.getUsedColumns();
            final DefaultBitSet configuration   = whatIfImpl.getConfiguration();
            submitAll(
                    // clear explain tables that we will end up reading
                    submit(clearExplainObject(), activeConnection),
                    submit(clearExplainStatement(), activeConnection),
                    submit(clearExplainPredicate(), activeConnection),
                    // enable indexes and set explain mode
                    submit(enableAdviseIndexRows(), activeConnection, configuration),
                    submit(explainModeEvaluateIndexes(), activeConnection)
            );

            try {
                activeConnection.execute(sql);
                System.err.print('.');
            } catch (SQLException e){
                System.err.print('.');
                Debug.logError(e.getLocalizedMessage());
            }

            // reset explain mode (indexes are left enabled...)
            submit(explainModeNo(), activeConnection);
            // post-process the explain tables
            // first get workload cost
            CostLevel result = supplyValue(fetchExplainStatementTotals(), activeConnection);
            try {
                explainCount = result.getCount();
                totalCost    = result.getTotalCost();
            } catch (Exception e){
                result = result.close();
                throw new SQLException(e);
            }
            result = result.close();
            if (explainCount != 1){
                throw new SQLException(
                        "unexpected number of statements: "
                                + explainCount
                                + " (expected 1)"
                );
            }

            if(profiledIndex != null){
                final String preds = supplyValue(fetchExplainPredicateString(), activeConnection);
                for (int i = 0; i < profiledIndex.columnCount(); i++) {
                    if (preds.contains(profiledIndex.getColumn(i).getName())){
                        usedColumns.set(i);
                    }
                }
            }

            activeConnection.commit();
            return totalCost;
        }


        @SuppressWarnings({"UnusedAssignment"})
        private static Double runWhatIfOptimizationWithoutProfiledIndex(
                WhatIfOptimizationBuilderImpl<DB2Index> whatIfImpl,
                JdbcDatabaseConnection<DB2Index> activeConnection
        ) throws SQLException {

            int explainCount; // should be equal to 1
            double totalCost;
            final String sql     = whatIfImpl.getSQL();
            final DefaultBitSet config  = whatIfImpl.getConfiguration();
            final DefaultBitSet usedSet = whatIfImpl.getUsedSet();

            // silently supply a command and an input parameter so that it could
            // get executed in the background (since no return to the caller is needed)
            submitAll(
                    // clear explain tables that we will end up reading
                    submit(clearExplainObject(), activeConnection),
                    submit(clearExplainStatement(), activeConnection),
                    // enable indexes and set explain mode
                    submit(enableAdviseIndexRows(), activeConnection, config),
                    submit(explainModeEvaluateIndexes(), activeConnection)
            );

            // evaluate the query
            try {
                activeConnection.execute(sql);
                //throw new Error("returned from execute() in what-if mode");
                System.err.print('.');
            } catch (SQLException e) {
                System.err.print('.');
                // expected in explain mode
            }

            // reset explain mode (indexes are left enabled...)
            submit(explainModeNo(), activeConnection);


            // post-process the explain tables
            // first get workload cost
            CostLevel costLevel = supplyValue(
                    fetchExplainStatementTotals(),
                    activeConnection
            );
            try {
                explainCount = costLevel.getCount();
                totalCost    = costLevel.getTotalCost();
            } catch (Throwable e) {
               costLevel = costLevel.close();
                throw new SQLException(e);
            }
            costLevel = costLevel.close();
            if (explainCount != 1){
                throw new SQLException("unexpected number of statements: " + explainCount + " (expected 1)");
            }


            // now get used indexes, using the input BitSet
            usedSet.clear();
            submit(fetchExplainObjectCandidates(), activeConnection, usedSet);
            activeConnection.commit();

            return totalCost;
        }

        @Override
        public int getWhatIfCount() {
            return whatifCount.get();
        }
    }


    /**
     *  A DB2-specific Database Index Extractor.
     */
    static class DB2DatabaseIndexExtractor extends AbstractDatabaseIndexExtractor<DB2Index> {
        private final String                        db2AdvisorPath;
        private final DatabaseConnection<DB2Index>  connection;

        DB2DatabaseIndexExtractor(String db2AdvisorPath, DatabaseConnection<DB2Index> connection){
            super();
            this.db2AdvisorPath = db2AdvisorPath;
            this.connection     = connection;
        }


        @SuppressWarnings({"RedundantTypeArguments"})
        @Override
        public void adjust(DatabaseConnection<DB2Index> connection) {
            final JdbcDatabaseConnection<DB2Index> c = Objects.as(connection);
            submit(
                    isolationLevelReadCommitted(),
                    c
            );
        }


        @Override
        public Iterable<DB2Index> recommendIndexes(String sql) throws SQLException {
            PreConditions.checkSQLRelatedState(
                    isEnabled(),
                    "We cannot use this extractor; its owner connection has been closed."
            );
            final JdbcDatabaseConnection<DB2Index> c = Objects.as(connection);
            List<DB2IndexMetadata> suppliedMetaList;

            try {

                submitAll(
                        submit(clearAdviseIndex(), c),
                        submit(explainModeRecommendIndexes(), c)
                );
                try {c.execute(sql);} catch (SQLException ignore){}

                // there is an unchecked warning here...
                final String databaseName = c.getConnectionManager().getDatabaseName();

                suppliedMetaList = supplyValue(readAdviseOnAllIndexes(), c, databaseName);
                submit(clearAdviseIndex(), c);
            } catch (Exception e){
                try{
                    c.rollback();
                } catch(SQLException debugged) {
                    Debug.logError("Could not rollback transaction", debugged);
                }

                Debug.logError("Could not recommend indexes for statement " + sql,  e);
                throw new SQLException(e);
            }

            final List<DB2Index> indexList = newList();
            for(DB2IndexMetadata each : suppliedMetaList){
                final double creationCost = each.creationCost(connection.getWhatIfOptimizer());
                final DB2Index index = new DB2Index(each, creationCost);
                indexList.add(index);
            }
            return indexList;
        }

        @Override
        public Iterable<DB2Index> recommendIndexes(File workloadFile) throws SQLException, IOException {
            PreConditions.checkSQLRelatedState(
                    isEnabled(),
                    "We cannot use this extractor; its owner connection has been closed."
            );
            final JdbcDatabaseConnection<DB2Index> c = Objects.as(connection);
            try {
                final Advisor.FileInfo advisorFile = Advisor.createAdvisorFile(
                        c,
                        db2AdvisorPath,
                        -1,
                        workloadFile
                );

                return advisorFile.getCandidates(c);
            } catch (AdvisorException a){
                throw new SQLException(a);
            }
        }
    }

    /**
     *  A PG-specific Database Index Extractor.
     */
    static class PGDatabaseIndexExtractor extends AbstractDatabaseIndexExtractor<PGIndex> {
        private final DatabaseConnection<PGIndex> connection;


        PGDatabaseIndexExtractor(DatabaseConnection<PGIndex> connection){
            super();
            this.connection = connection;
        }


        @Override
        public void adjust(DatabaseConnection<PGIndex> pgIndexDatabaseConnection) {
            //does nothing
        }

        @Override
        public Iterable<PGIndex> recommendIndexes(String sql) throws SQLException {
            if(!isEnabled()) throw new SQLException("IndexExtractor is Disabled.");
            return supplyValue(PGCommands.recommendIndexes(), connection, sql);
        }

        @Override
        public Iterable<PGIndex> recommendIndexes(File workloadFile) throws SQLException, IOException {
            if(!isEnabled()) throw new SQLException("IndexExtractor is Disabled.");
            final List<PGIndex> candidateSet = new ArrayList<PGIndex>();
            for (String line : Files.getLines(workloadFile)) {
                final String sql = trimSqlStatement(line);
                candidateSet.addAll((Collection<? extends PGIndex>) recommendIndexes(sql));
            }
            return candidateSet;
        }
    }
}
