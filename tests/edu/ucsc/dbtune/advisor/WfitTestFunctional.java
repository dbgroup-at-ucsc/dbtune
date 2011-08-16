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
package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.IBGPreparedSQLStatement;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.connectivity.JdbcConnectionManager.makeDatabaseConnectionManager;
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
public class WfitTestFunctional
{
    public final static DatabaseConnection connection;
    public final static Environment        env;

    static {
        try {       
            env        = Environment.getInstance();
            connection = makeDatabaseConnectionManager(env.getAll()).connect();
        } catch(SQLException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    @BeforeClass
    public static void setUp() throws Exception
    {
        File   outputdir   = new File(env.getOutputFoldername() + "/one_table");
        String ddlfilename = env.getScriptAtWorkloadsFolder("one_table/create.sql");

        outputdir.mkdirs();
        //SQLScriptExecuter.execute(connection.getJdbcConnection(), ddlfilename);
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

        workloadFile   = env.getScriptAtWorkloadsFolder("one_table/workload.sql");
        maxNumIndexes  = env.getMaxNumIndexes();
        maxNumStates   = env.getMaxNumStates();
        windowSize     = env.getIndexStatisticsWindow();
        partIterations = env.getNumPartitionIterations();
        pool           = getCandidates(connection, workloadFile);
        fileReader     = new FileReader(workloadFile);
        workload       = new Workload(fileReader);
        q              = 0;
        whatIfCount    = 0;

        wfit = new WFIT(connection, pool, maxNumStates, maxNumIndexes, windowSize, partIterations);

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
                assertThat(qinfo.getOptimizationCount()-whatIfCount, is(5));
            } else {
                assertThat(qinfo.getOptimizationCount()-whatIfCount, is(3));
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

    private static Configuration getCandidates(DatabaseConnection con, String workloadFilename)
        throws SQLException, IOException
    {
        Configuration   pool;
        Iterable<Index> candidateSet;
        Workload        wl;
        
        wl   = new Workload(new FileReader(workloadFilename));
        pool = new Configuration("conf");

        for(SQLStatement sql : wl) {
            candidateSet = con.getOptimizer().recommendIndexes(sql.getSQL());

            for (Index index : candidateSet) {
                pool.add(index);
            }
        }

        return pool;
    }
}
