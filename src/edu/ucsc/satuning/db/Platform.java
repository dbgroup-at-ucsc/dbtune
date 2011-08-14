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
package edu.ucsc.satuning.db;

import edu.ucsc.satuning.db.ibm.Advisor;
import edu.ucsc.satuning.db.ibm.AdvisorException;
import edu.ucsc.satuning.db.ibm.DB2ExplainInfo;
import edu.ucsc.satuning.db.ibm.DB2Index;
import edu.ucsc.satuning.db.ibm.DB2IndexMetadata;
import edu.ucsc.satuning.db.ibm.QualifiedName;
import edu.ucsc.satuning.db.pg.PGCommands;
import edu.ucsc.satuning.db.pg.PGIndex;
import edu.ucsc.satuning.db.pg.PGReifiedTypes.ReifiedPGIndexList;
import edu.ucsc.satuning.util.BitSet;
import edu.ucsc.satuning.util.Debug;
import edu.ucsc.satuning.util.Files;
import edu.ucsc.satuning.util.Objects;
import edu.ucsc.satuning.util.PreConditions;
import edu.ucsc.satuning.workload.SQLStatement;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static edu.ucsc.satuning.db.ibm.DB2Commands.clearAdviseIndex;
import static edu.ucsc.satuning.db.ibm.DB2Commands.clearExplainObject;
import static edu.ucsc.satuning.db.ibm.DB2Commands.clearExplainOperator;
import static edu.ucsc.satuning.db.ibm.DB2Commands.clearExplainPredicate;
import static edu.ucsc.satuning.db.ibm.DB2Commands.clearExplainStatement;
import static edu.ucsc.satuning.db.ibm.DB2Commands.enableAdviseIndexRows;
import static edu.ucsc.satuning.db.ibm.DB2Commands.explainModeEvaluateIndexes;
import static edu.ucsc.satuning.db.ibm.DB2Commands.explainModeExplain;
import static edu.ucsc.satuning.db.ibm.DB2Commands.explainModeNo;
import static edu.ucsc.satuning.db.ibm.DB2Commands.explainModeRecommendIndexes;
import static edu.ucsc.satuning.db.ibm.DB2Commands.fetchExplainObjectCandidates;
import static edu.ucsc.satuning.db.ibm.DB2Commands.fetchExplainObjectUpdatedTable;
import static edu.ucsc.satuning.db.ibm.DB2Commands.fetchExplainOpUpdateCost;
import static edu.ucsc.satuning.db.ibm.DB2Commands.fetchExplainPredicateString;
import static edu.ucsc.satuning.db.ibm.DB2Commands.fetchExplainStatementTotals;
import static edu.ucsc.satuning.db.ibm.DB2Commands.fetchExplainStatementType;
import static edu.ucsc.satuning.db.ibm.DB2Commands.isolationLevelReadCommitted;
import static edu.ucsc.satuning.db.ibm.DB2Commands.loadAdviseIndex;
import static edu.ucsc.satuning.db.ibm.DB2Commands.readAdviseOnAllIndexes;
import static edu.ucsc.satuning.db.pg.PGCommands.explainIndexes;
import static edu.ucsc.satuning.db.pg.PGCommands.explainIndexesCost;
import static edu.ucsc.satuning.spi.Commands.submit;
import static edu.ucsc.satuning.spi.Commands.submitAll;
import static edu.ucsc.satuning.spi.Commands.supplyValue;
import static edu.ucsc.satuning.util.DBUtilities.trimSqlStatement;
import static edu.ucsc.satuning.util.Util.newAtomicInteger;
import static edu.ucsc.satuning.util.Util.newAtomicReference;
import static edu.ucsc.satuning.util.Util.newList;

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
     * finds the appropriate {@link DatabaseIndexExtractorFactory} strategy for a given driver. This method
     * will throw a {@link NullPointerException} if strategy is not found. We rather deal with problem
     * right away (i.e., throwing a NullPointerException) rather than waiting for some side affect along the execution line.
     *
     * @param driver
     *      dbms-driver's fully qualified name.
     * @param <I>
     *      type of {@link DBIndex}.
     * @return
     *      a found {@link DatabaseIndexExtractorFactory} pre-instantiated instance.
     * @throws NullPointerException
     *      if strategy is not found.
     */
    public static <I extends DBIndex<I>> DatabaseIndexExtractorFactory<I> findIndexExtractorFactory(String driver){
        return  Objects.as(PreConditions.checkNotNull(AVAILABLE_EXTRACTORS.get(driver)));
    }

    /**
     * finds the appropriate {@link DatabaseWhatIfOptimizerFactory} strategy for a given driver. This method
     * will throw a {@link NullPointerException} if strategy is not found. We rather deal with problem
     * right away (i.e., throwing a NullPointerException) rather than waiting for some side affect along the execution line.
     *
     * @param driver
     *      dbms-driver's fully qualified name.
     * @param <I>
     *      type of {@link DBIndex}.
     * @return
     *      a found {@link DatabaseWhatIfOptimizerFactory} pre-instantiated instance.
     * @throws NullPointerException
     *      if strategy is not found.
     */
    public static <I extends DBIndex<I>> DatabaseWhatIfOptimizerFactory<I> findWhatIfOptimizerFactory(String driver){
        return Objects.as(PreConditions.checkNotNull(AVAILABLE_OPTIMIZERS.get(driver)));
    }

    private static class DB2IndexExtractorFactory implements DatabaseIndexExtractorFactory<DB2Index>{
        @Override
        public DatabaseIndexExtractor<DB2Index> makeIndexExtractor(DatabaseConnection<DB2Index> connection) {
            return new DB2DatabaseIndexExtractor("/home/karlsch/sqllib/bin/db2advis", PreConditions.checkNotNull(connection));
        }
    }

    private static class PGIndexExtractorFactory implements DatabaseIndexExtractorFactory<PGIndex>{
        @Override
        public DatabaseIndexExtractor<PGIndex> makeIndexExtractor(DatabaseConnection<PGIndex> connection) {
            return new PGDatabaseIndexExtractor(PreConditions.checkNotNull(connection));
        }
    }

    private static class DB2WhatIfOptimizerFactory implements DatabaseWhatIfOptimizerFactory<DB2Index>{
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
        private final DatabaseConnection<PGIndex>       connection;

        private final AtomicReference<DatabaseIndexExtractor<PGIndex>>  extractor;
        private final AtomicInteger                                     whatifCount;


        PGDatabaseWhatIfOptimizer(DatabaseConnection<PGIndex> connection){
            super();
            this.connection = connection;
            whatifCount     = newAtomicInteger();
            extractor       = newAtomicReference();

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
            final BitSet             usedSet         = whatIfImpl.getUsedSet();
            final BitSet             configuration   = whatIfImpl.getConfiguration();
            final ReifiedPGIndexList indexSet        = getCandidateIndexSet();

            return supplyValue(
                    explainIndexesCost(usedSet),
                    connection,
                    indexSet,
                    configuration,
                    sql,
                    configuration.cardinality(),
                    new Double[]{0.0}
            );
        }

        private ReifiedPGIndexList getCandidateIndexSet(){
            if(extractor.get() == null){
                extractor.set(PreConditions.checkNotNull(connection.getIndexExtractor()));
            }

            final AbstractDatabaseIndexExtractor<PGIndex> aExtractor = Objects.as(extractor.get());
            final Iterable<PGIndex> candidateSet = aExtractor.getCandidateSet();
            final ReifiedPGIndexList indexSet = new ReifiedPGIndexList();
            for(PGIndex each : candidateSet){
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
        private final DatabaseConnection<DB2Index>                       connection;
        private final AtomicReference<DatabaseIndexExtractor<DB2Index>>  extractor;
        private final AtomicInteger                                      whatifCount;

        DB2DatabaseWhatIfOptimizer(DatabaseConnection<DB2Index> connection){
            super();
            this.connection = connection;
            whatifCount     = newAtomicInteger();
            extractor       = newAtomicReference();
        }

        @Override
        protected void incrementWhatIfCount() {
            whatifCount.incrementAndGet();
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

            final DefaultDatabaseConnection<DB2Index> c =  Objects.as(connection);

            if(!whatIfImpl.withProfiledIndex()){
               return runWhatIfOptimizationWithoutProfiledIndex(whatIfImpl, c);
            }  else {
               return runWhatIfOptimizationWithProfiledIndex(whatIfImpl, c);
            }

        }

        private void fixAnyExistingCandidates() throws SQLException {
            if(extractor.get() == null){
                extractor.set(connection.getIndexExtractor());
            }
            final DatabaseIndexExtractor<DB2Index> e = extractor.get();
            final Iterable<DB2Index> candidateSet = Objects.<AbstractDatabaseIndexExtractor<DB2Index>>as(e).getCandidateSet();
            e.fixCandidates(candidateSet);
        }

        @SuppressWarnings({"UnusedAssignment"})
        private static Double runWhatIfOptimizationWithProfiledIndex(
                WhatIfOptimizationBuilderImpl<DB2Index> whatIfImpl,
                DefaultDatabaseConnection<DB2Index> activeConnection
        ) throws SQLException {
            int explainCount; // should be equal to 1
            double totalCost;

            final String        sql             = whatIfImpl.getSQL();
            final DB2Index      profiledIndex   = whatIfImpl.getProfiledIndex();
            final BitSet        usedColumns     = whatIfImpl.getUsedColumns();
            final BitSet        configuration   = whatIfImpl.getConfiguration();
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
            CostOfLevel result = supplyValue(fetchExplainStatementTotals(), activeConnection);
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
                DefaultDatabaseConnection<DB2Index> activeConnection
        ) throws SQLException {

            int explainCount; // should be equal to 1
            double totalCost;
            final String sql     = whatIfImpl.getSQL();
            final BitSet config  = whatIfImpl.getConfiguration();
            final BitSet usedSet = whatIfImpl.getUsedSet();

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
            CostOfLevel costLevel = supplyValue(
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
        private       Iterable<DB2Index>            cachedCandidateSet = new LinkedList<DB2Index>();
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
            final DefaultDatabaseConnection<DB2Index> c = Objects.as(connection);
            submit(
                    isolationLevelReadCommitted(),
                    c
            );
        }

        @Override
        public ExplainInfo<DB2Index> explainInfo(String sql) throws SQLException {
            PreConditions.checkSQLRelatedState(
                    isEnabled(),
                    "We cannot use this extractor; its owner connection has been closed."
            );
            SQLStatement.SQLCategory category     = null;
            QualifiedName updatedTable = null;
            double                   updateCost   = 0.0;
            final DefaultDatabaseConnection<DB2Index> c = Objects.as(connection);

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
            final DefaultDatabaseConnection<DB2Index> c = Objects.as(connection);
            cachedCandidateSet = candidateSet;

            submitAll(
                    submit(clearAdviseIndex(), c),
                    submit(loadAdviseIndex(), candidateSet.iterator(), false, c)
            );
        }

        @Override
        public Iterable<DB2Index> recommendIndexes(String sql) throws SQLException {
            PreConditions.checkSQLRelatedState(
                    isEnabled(),
                    "We cannot use this extractor; its owner connection has been closed."
            );
            final DefaultDatabaseConnection<DB2Index> c = Objects.as(connection);
            List<DB2IndexMetadata> suppliedMetaList;

            try {

                submitAll(
                        submit(clearAdviseIndex(), c),
                        submit(explainModeRecommendIndexes(), c)
                );
                try {c.execute(sql);} catch (SQLException ignore){}

                // there is an unchecked warning here...
                final String databaseName = Objects.<AbstractDatabaseConnectionManager<DB2Index>>as(
                        c.getConnectionManager()
                ).getDatabase();

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
                final double creationCost = each.creationCost(this, connection.getWhatIfOptimizer());
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
            final DefaultDatabaseConnection<DB2Index> c = Objects.as(connection);
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

        @Override
        public Iterable<DB2Index> getCandidateSet() {
            return cachedCandidateSet;
        }

        @Override
        public BitSet getCachedBitSet() {
            return new BitSet();
        }
    }

    /**
     *  A PG-specific Database Index Extractor.
     */
    static class PGDatabaseIndexExtractor extends AbstractDatabaseIndexExtractor<PGIndex>{
        private final DatabaseConnection<PGIndex> connection;
        private BitSet cachedBitSet = new BitSet();
        private List<PGIndex>            cachedCandidateSet = new ArrayList<PGIndex>();

        PGDatabaseIndexExtractor(DatabaseConnection<PGIndex> connection){
            super();
            this.connection = connection;
        }


        @Override
        public void adjust(DatabaseConnection<PGIndex> pgIndexDatabaseConnection) {
            //does nothing
        }

        @Override
        public ExplainInfo<PGIndex> explainInfo(String sql) throws SQLException {
            if(!isEnabled()) throw new SQLException("IndexExtractor is Disabled.");
            incrementWhatIfCounter();
            final ReifiedPGIndexList indexSet = new ReifiedPGIndexList();
            indexSet.addAll(Objects.<Collection<? extends PGIndex>>as(Arrays.asList(cachedCandidateSet)));
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
        public void fixCandidates(Iterable<PGIndex> candidateSet) throws SQLException {
            if(!isEnabled()) throw new SQLException("IndexExtractor is Disabled.");
            cachedCandidateSet.clear();
            getCachedBitSet().clear();
            for (PGIndex idx : candidateSet) {
                cachedCandidateSet.add(idx);
                getCachedBitSet().set(idx.internalId());
            }
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

        @Override
        public Iterable<PGIndex> getCandidateSet() {
            return cachedCandidateSet;
        }

        @Override
        public BitSet getCachedBitSet() {
            return cachedBitSet;
        }
    }
}
