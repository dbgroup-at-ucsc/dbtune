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

package edu.ucsc.dbtune;

import edu.ucsc.dbtune.advisor.AbstractCandidateIndexExtractor;
import edu.ucsc.dbtune.advisor.DB2AdvisorCaller;
import edu.ucsc.dbtune.advisor.CandidateIndexExtractor;
import edu.ucsc.dbtune.advisor.CandidateIndexExtractorFactory;
import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.connectivity.JdbcConnection;
import edu.ucsc.dbtune.connectivity.PGCommands;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.PGIndex;
import edu.ucsc.dbtune.optimizer.DB2IBGWhatIfOptimizer;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.IBGWhatIfOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.OptimizerFactory;
import edu.ucsc.dbtune.optimizer.PGOptimizer;
import edu.ucsc.dbtune.optimizer.PostgresIBGWhatIfOptimizer;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.Iterables;
import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static edu.ucsc.dbtune.connectivity.DB2Commands.clearAdviseIndex;
import static edu.ucsc.dbtune.connectivity.DB2Commands.explainModeRecommendIndexes;
import static edu.ucsc.dbtune.connectivity.DB2Commands.isolationLevelReadCommitted;
import static edu.ucsc.dbtune.connectivity.DB2Commands.readAdviseOnAllIndexes;
import static edu.ucsc.dbtune.spi.core.Functions.submit;
import static edu.ucsc.dbtune.spi.core.Functions.submitAll;
import static edu.ucsc.dbtune.spi.core.Functions.supplyValue;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Platform {
    private static final Map<String, CandidateIndexExtractorFactory>  AVAILABLE_EXTRACTORS;
    private static final Map<String, OptimizerFactory>       AVAILABLE_OPTIMIZERS;
    static {
        Map<String, CandidateIndexExtractorFactory> driverToExtractor =
                new HashMap<String, CandidateIndexExtractorFactory>(){
                    {
                        put("com.ibm.db2.jcc.DB2Driver", new DB2CandidateIndexExtractorFactory());
                        put("org.postgresql.Driver", new PGCandidateIndexExtractorFactory());
                    }
                };

        Map<String, OptimizerFactory> driverToOptimizer =
                new HashMap<String, OptimizerFactory>(){
                    {
                        put("com.ibm.db2.jcc.DB2Driver", new DB2OptimizerFactory());
                        put("org.postgresql.Driver", new PGOptimizerFactory());
                    }
                };

        AVAILABLE_EXTRACTORS        = Collections.unmodifiableMap(driverToExtractor);
        AVAILABLE_OPTIMIZERS        = Collections.unmodifiableMap(driverToOptimizer);
    }

    /**
     * utility class.
     */
    private Platform(){}

    /**
     * finds the appropriate {@link CandidateIndexExtractorFactory} strategy for a given driver. This method
     * will throw a {@link NullPointerException} if strategy is not found. We rather deal with problem
     * right away (i.e., throwing a NullPointerException) rather than waiting for some side affect along the execution line.
     *
     * @param driver
     *      dbms-driver's fully qualified name.
     * @return
     *      a found {@link CandidateIndexExtractorFactory} pre-instantiated instance.
     * @throws NullPointerException
     *      if strategy is not found.
     */
    public static CandidateIndexExtractorFactory findIndexExtractorFactory(String driver){
        return  Objects.as(Checks.checkNotNull(AVAILABLE_EXTRACTORS.get(driver)));
    }

    /**
     * finds the appropriate {@link OptimizerFactory} strategy for a given driver. This method
     * will throw a {@link NullPointerException} if strategy is not found. We rather deal with problem
     * right away (i.e., throwing a NullPointerException) rather than waiting for some side affect along the execution line.
     *
     * @param driver
     *      dbms-driver's fully qualified name.
     * @return
     *      a found {@link OptimizerFactory} pre-instantiated instance.
     * @throws NullPointerException
     *      if strategy is not found.
     */
    public static OptimizerFactory findOptimizerFactory(String driver){
        return Objects.as(Checks.checkNotNull(AVAILABLE_OPTIMIZERS.get(driver)));
    }

    private static class DB2CandidateIndexExtractorFactory implements CandidateIndexExtractorFactory {
        @Override
        public CandidateIndexExtractor newCandidateIndexExtractor(String advisorFolder, DatabaseConnection connection) {
            return new DB2CandidateIndexExtractor(advisorFolder, Checks.checkNotNull(connection));
        }
    }

    private static class PGCandidateIndexExtractorFactory implements CandidateIndexExtractorFactory {
        @Override
        public CandidateIndexExtractor newCandidateIndexExtractor(String advisorFolder, DatabaseConnection connection) {
            return new PGCandidateIndexExtractor(Checks.checkNotNull(connection));
        }
    }

    private static class DB2OptimizerFactory implements OptimizerFactory {
        @Override
        public IBGWhatIfOptimizer newIBGWhatIfOptimizer(DatabaseConnection connection) {
            return new DB2IBGWhatIfOptimizer(Checks.checkNotNull(connection));
        }

        @Override
        public Optimizer newOptimizer(DatabaseConnection connection) {
            try {
                return new DB2Optimizer(connection.getJdbcConnection());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class PGOptimizerFactory implements OptimizerFactory {
        @Override
        public IBGWhatIfOptimizer newIBGWhatIfOptimizer(DatabaseConnection connection) {
            return new PostgresIBGWhatIfOptimizer(Checks.checkNotNull(connection));
        }
        @Override
        public Optimizer newOptimizer(DatabaseConnection connection) {
            try {
                return new PGOptimizer(connection);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     *  A DB2-specific Database Index Extractor.
     */
    static class DB2CandidateIndexExtractor extends AbstractCandidateIndexExtractor {
        private final String                        db2AdvisorPath;
        private final DatabaseConnection            connection;

        DB2CandidateIndexExtractor(String db2AdvisorPath, DatabaseConnection connection){
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
        public Iterable<Index> recommendIndexes(SQLStatement sql) throws SQLException {
            Checks.checkSQLRelatedState(null != connection && !connection.isClosed(), "Connection is closed.");
            final JdbcConnection c = Objects.as(connection);
            List<Index> indexList;

            try {

                submitAll(
                        submit(clearAdviseIndex(), c),
                        submit(explainModeRecommendIndexes(), c)
                );
                try {c.execute(sql.getSQL());} catch (SQLException ignore){}

                // there is an unchecked warning here...
                final String databaseName = c.getConnectionManager().getDatabaseName();

                indexList = supplyValue(readAdviseOnAllIndexes(), c, databaseName);
                submit(clearAdviseIndex(), c);
            } catch (Exception e){
                try{
                    c.rollback();
                } catch(SQLException debugged) {
                    Console.streaming().error("Could not rollback transaction", debugged);
                }

                Console.streaming().error("Could not recommend indexes for statement " + sql, e);
                throw new SQLException(e);
            }

            return indexList;
        }
    }

    /**
     *  A PG-specific Database Index Extractor.
     */
    static class PGCandidateIndexExtractor extends AbstractCandidateIndexExtractor {
        private final DatabaseConnection connection;


        PGCandidateIndexExtractor(DatabaseConnection connection){
            super();
            this.connection = connection;
        }


        @Override
        public void adjust(DatabaseConnection pgIndexDatabaseConnection) {
            //does nothing
        }

        @Override
        public Iterable<Index> recommendIndexes(SQLStatement sql) throws SQLException {
            Checks.checkSQLRelatedState(null != connection && !connection.isClosed(), "Connection is closed.");
            Iterable<PGIndex> suppliedIterable = supplyValue(PGCommands.recommendIndexes(), connection, sql);
            return Iterables.<Index>asIterable(suppliedIterable);
        }
    }
}
