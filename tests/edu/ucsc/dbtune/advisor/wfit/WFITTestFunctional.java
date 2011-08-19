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
package edu.ucsc.dbtune.advisor.wfit;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.wfit.WFIT;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.IBGPreparedSQLStatement;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.spi.EnvironmentProperties.SCHEMA;

import static edu.ucsc.dbtune.util.SQLScriptExecuter.execute;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

/**
 * Functional test for the WFIT use case.
 *
 * Some of the documentation contained in this class refers to sections of Karl Schnaitter's 
 * Doctoral Thesis "On-line Index Selection for Physical Database Tuning". Specifically to chapter 
 * 6.
 *
 * @see <a href="http://proquest.umi.com/pqdlink?did=2171968721&Fmt=7&clientId=1565&RQT=309&VName=PQD">
 *     "On-line Index Selection for Physical Database Tuning"</a>
 * @author Ivo Jimenez
 */
public class WFITTestFunctional
{
    public static DatabaseSystem db;
    public static Environment    en;

    @BeforeClass
    public static void setUp() throws Exception
    {
        Properties cfg;
        String     ddl;

        cfg = new Properties(Environment.getInstance().getAll());

        cfg.setProperty(SCHEMA,"one_table");

        en  = new Environment(cfg);
        db  = DatabaseSystem.newDatabaseSystem(en);
        ddl = en.getScriptAtWorkloadsFolder("one_table/create.sql");

        {
            // DatabaseSystem reads the catalog as part of its creation, so we need to wipe anything 
            // in the movies schema and reload the data. Then create a DB again to get a fresh 
            // catalog
            //execute(db.getConnection(), ddl);
            db.getConnection().close();

            db = null;
            db = DatabaseSystem.newDatabaseSystem(en);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception{
        db.getConnection().close();
    }

    /**
     * The test has to check first just one query and one index. We need to make sure that the index 
     * gets recommended and dropped consistently to what we expect.
     */
    @Test
    public void testWFIT() throws Exception
    {
        IBGPreparedSQLStatement qinfo;
        Configuration pool;
        WFIT wfit;
        Workload workload;
        String   workloadFile;

        FileReader  fileReader;
        Configuration configuration;
        int         maxNumIndexes;
        int         maxNumStates;
        int         windowSize;
        int         partIterations;
        int         q;
        int         whatIfCount;

        workloadFile   = en.getScriptAtWorkloadsFolder("one_table/workload.sql");
        maxNumIndexes  = en.getMaxNumIndexes();
        maxNumStates   = en.getMaxNumStates();
        windowSize     = en.getIndexStatisticsWindow();
        partIterations = en.getNumPartitionIterations();
        pool           = getCandidates(workloadFile);
        fileReader     = new FileReader(workloadFile);
        workload       = new Workload(fileReader);
        q              = 0;
        whatIfCount    = 0;

        wfit = new WFIT(db.getOptimizer(), pool, maxNumStates, maxNumIndexes, windowSize, partIterations);

        for (SQLStatement sql : workload) {
            wfit.process(sql);

            assertThat(wfit.getPartitions().subsetCount(), is(1));

            configuration = wfit.getRecommendation();

            qinfo = wfit.getStatement(q);

            //System.out.println("------\nq" + q + "\n" + qinfo);
            //System.out.println("\n" + qinfo.getConfiguration());
            //System.out.println("\n" + configuration);

            assertThat(qinfo.getConfiguration().size(), is(1));

            if(q == 0) {
                assertThat(qinfo.getOptimizationCount()-whatIfCount, is(11));
            } else {
                assertThat(qinfo.getOptimizationCount()-whatIfCount, is(9));
            }

            if(q < 5) {
                assertThat(configuration.size(), is(0));
                assertThat(configuration.isEmpty(), is(true));
            } else if(q == 5) {
                //assertThat(configuration.size(), is(1));
                //assertThat(configuration.isEmpty(), is(false));
            } else if(q == 6) {
                assertThat(configuration.size(), is(0));
                assertThat(configuration.isEmpty(), is(true));
            } else {
                throw new SQLException("Workload should have 7 statements");
            }

            whatIfCount = qinfo.getOptimizationCount();

            q++;
        }
    }

    private static Configuration getCandidates(String workloadFilename)
        throws SQLException, IOException
    {
        Configuration   pool;
        Iterable<Index> candidateSet;
        Workload        wl;
        
        wl   = new Workload(new FileReader(workloadFilename));
        pool = new Configuration("conf");

        for(SQLStatement sql : wl) {
            candidateSet = db.getOptimizer().recommendIndexes(sql);

            for (Index index : candidateSet) {
                pool.add(index);
            }
        }

        return pool;
    }
}
