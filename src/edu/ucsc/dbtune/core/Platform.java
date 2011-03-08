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

import edu.ucsc.dbtune.core.metadata.DB2Index;
import edu.ucsc.dbtune.core.metadata.DB2IndexMetadata;
import edu.ucsc.dbtune.core.metadata.PGCommands;
import edu.ucsc.dbtune.core.metadata.PGIndex;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.Debug;
import edu.ucsc.dbtune.util.Files;
import edu.ucsc.dbtune.util.Iterables;
import edu.ucsc.dbtune.util.Objects;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static edu.ucsc.dbtune.core.metadata.DB2Commands.clearAdviseIndex;
import static edu.ucsc.dbtune.core.metadata.DB2Commands.explainModeRecommendIndexes;
import static edu.ucsc.dbtune.core.metadata.DB2Commands.isolationLevelReadCommitted;
import static edu.ucsc.dbtune.core.metadata.DB2Commands.readAdviseOnAllIndexes;
import static edu.ucsc.dbtune.spi.core.Functions.submit;
import static edu.ucsc.dbtune.spi.core.Functions.submitAll;
import static edu.ucsc.dbtune.spi.core.Functions.supplyValue;
import static edu.ucsc.dbtune.util.DBUtilities.trimSqlStatement;
import static edu.ucsc.dbtune.util.Instances.newList;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Platform {
    private static final Map<String, IndexExtractorFactory>  AVAILABLE_EXTRACTORS;
    private static final Map<String, WhatIfOptimizerFactory> AVAILABLE_OPTIMIZERS;
    static {
        Map<String, IndexExtractorFactory> driverToExtractor =
                new HashMap<String, IndexExtractorFactory>(){
                    {
                        put("com.ibm.db2.jcc.DB2Driver", new DB2IndexExtractorFactory());
                        put("org.postgresql.Driver", new PGIndexExtractorFactory());
                    }
                };
        Map<String, WhatIfOptimizerFactory> driverToOptimizer =
                new HashMap<String, WhatIfOptimizerFactory>(){
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
     * finds the appropriate {@link IndexExtractorFactory} strategy for a given driver. This method
     * will throw a {@link NullPointerException} if strategy is not found. We rather deal with problem
     * right away (i.e., throwing a NullPointerException) rather than waiting for some side affect along the execution line.
     *
     * @param driver
     *      dbms-driver's fully qualified name.
     * @return
     *      a found {@link IndexExtractorFactory} pre-instantiated instance.
     * @throws NullPointerException
     *      if strategy is not found.
     */
    public static IndexExtractorFactory findIndexExtractorFactory(String driver){
        return  Objects.as(Checks.checkNotNull(AVAILABLE_EXTRACTORS.get(driver)));
    }

    /**
     * finds the appropriate {@link WhatIfOptimizerFactory} strategy for a given driver. This method
     * will throw a {@link NullPointerException} if strategy is not found. We rather deal with problem
     * right away (i.e., throwing a NullPointerException) rather than waiting for some side affect along the execution line.
     *
     * @param driver
     *      dbms-driver's fully qualified name.
     * @return
     *      a found {@link WhatIfOptimizerFactory} pre-instantiated instance.
     * @throws NullPointerException
     *      if strategy is not found.
     */
    public static WhatIfOptimizerFactory findWhatIfOptimizerFactory(String driver){
        return Objects.as(Checks.checkNotNull(AVAILABLE_OPTIMIZERS.get(driver)));
    }

    private static class DB2IndexExtractorFactory implements IndexExtractorFactory {
        @Override
        public IndexExtractor newIndexExtractor(String advisorFolder, DatabaseConnection connection) {
            return new DB2DatabaseIndexExtractor(advisorFolder, Checks.checkNotNull(connection));
        }
    }

    private static class PGIndexExtractorFactory implements IndexExtractorFactory {
        @Override
        public IndexExtractor newIndexExtractor(String advisorFolder, DatabaseConnection connection) {
            return new PGDatabaseIndexExtractor(Checks.checkNotNull(connection));
        }
    }

    private static class DB2WhatIfOptimizerFactory implements WhatIfOptimizerFactory {
        @Override
        public WhatIfOptimizer newWhatIfOptimizer(DatabaseConnection connection) {
            return new DB2WhatIfOptimizer(Checks.checkNotNull(connection));
        }

        @Override
        public IBGWhatIfOptimizer newIBGWhatIfOptimizer(DatabaseConnection connection) {
            return new DB2IBGWhatIfOptimizer(Checks.checkNotNull(connection));
        }
    }

    private static class PGWhatIfOptimizerFactory implements WhatIfOptimizerFactory {
        @Override
        public WhatIfOptimizer newWhatIfOptimizer(DatabaseConnection connection) {
            return new PostgresWhatIfOptimizer(Checks.checkNotNull(connection));
        }

        @Override
        public IBGWhatIfOptimizer newIBGWhatIfOptimizer(DatabaseConnection connection) {
            return new PostgresIBGWhatIfOptimizer(Checks.checkNotNull(connection));
        }
    }


    /**
     *  A DB2-specific Database Index Extractor.
     */
    static class DB2DatabaseIndexExtractor extends AbstractIndexExtractor {
        private final String                        db2AdvisorPath;
        private final DatabaseConnection            connection;

        DB2DatabaseIndexExtractor(String db2AdvisorPath, DatabaseConnection connection){
            super();
            this.db2AdvisorPath = db2AdvisorPath;
            this.connection     = connection;
        }


        @SuppressWarnings({"RedundantTypeArguments"})
        @Override
        public void adjust(DatabaseConnection connection) {
            submit(
                    isolationLevelReadCommitted(),
                    connection
            );
        }


        @Override
        public Iterable<DBIndex> recommendIndexes(String sql) throws SQLException {
            Checks.checkSQLRelatedState(null != connection && !connection.isClosed(), "Connection is closed.");
            final JdbcConnection c = Objects.as(connection);
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

            final List<DBIndex> indexList = newList();
            for(DB2IndexMetadata each : suppliedMetaList){
                final double creationCost = each.creationCost(connection.getIBGWhatIfOptimizer());
                final DBIndex index = new DB2Index(each, creationCost);
                indexList.add(index);
            }
            return indexList;
        }

        @Override
        public Iterable<DBIndex> recommendIndexes(File workloadFile) throws SQLException, IOException {
            Checks.checkSQLRelatedState(null != connection && !connection.isClosed(), "Connection is closed.");
            final JdbcConnection c = Objects.as(connection);
            try {
                final Advisor.FileInfo advisorFile = Advisor.createAdvisorFile(
                        c,
                        db2AdvisorPath,
                        -1,
                        workloadFile
                );

                return Iterables.<DBIndex>asIterable(advisorFile.getCandidates(c));
            } catch (AdvisorException a){
                throw new SQLException(a);
            }
        }
    }

    /**
     *  A PG-specific Database Index Extractor.
     */
    static class PGDatabaseIndexExtractor extends AbstractIndexExtractor {
        private final DatabaseConnection connection;


        PGDatabaseIndexExtractor(DatabaseConnection connection){
            super();
            this.connection = connection;
        }


        @Override
        public void adjust(DatabaseConnection pgIndexDatabaseConnection) {
            //does nothing
        }

        @Override
        public Iterable<DBIndex> recommendIndexes(String sql) throws SQLException {
            Checks.checkSQLRelatedState(null != connection && !connection.isClosed(), "Connection is closed.");
            Iterable<PGIndex> suppliedIterable = supplyValue(PGCommands.recommendIndexes(), connection, sql);
            return Iterables.<DBIndex>asIterable(suppliedIterable);
        }

        @Override
        public Iterable<DBIndex> recommendIndexes(File workloadFile) throws SQLException, IOException {
            if(!isEnabled()) throw new SQLException("IndexExtractor is Disabled.");
            final List<DBIndex> candidateSet = new ArrayList<DBIndex>();
            for (String line : Files.getLines(workloadFile)) {
                final String sql = trimSqlStatement(line);
                final Iterable<? extends DBIndex> recommended = recommendIndexes(sql);
                candidateSet.addAll(Iterables.asCollection(recommended));
            }
            return candidateSet;
        }
    }
}
