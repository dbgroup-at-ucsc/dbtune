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
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.DatabaseSystem.newConnection;
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
public class WFITFunctionalTest
{
    public static DatabaseSystem db;
    public static Environment    en;

    @BeforeClass
    public static void setUp() throws Exception
    {
        Connection con;
        String     ddl;

        en  = Environment.getInstance();
        ddl = en.getScriptAtWorkloadsFolder("one_table/create.sql");
        con = newConnection(en);

        //execute(con, ddl);
        con.close();

        db = newDatabaseSystem(en);
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
        PreparedSQLStatement qinfo;
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

        workloadFile   = en.getScriptAtWorkloadsFolder("one_table/workload.sql");
        maxNumIndexes  = en.getMaxNumIndexes();
        maxNumStates   = en.getMaxNumStates();
        windowSize     = en.getIndexStatisticsWindow();
        partIterations = en.getNumPartitionIterations();
        pool           = getCandidates(workloadFile);
        fileReader     = new FileReader(workloadFile);
        workload       = new Workload(fileReader);
        q              = 0;

        wfit = new WFIT(db.getOptimizer(), pool, maxNumStates, maxNumIndexes, windowSize, partIterations);

        for (SQLStatement sql : workload) {
            wfit.process(sql);

            //assertThat(wfit.getPartitions().subsetCount(), is(1));

            configuration = wfit.getRecommendation();

            qinfo = wfit.getStatement(q);

            //System.out.println("------\nq" + q + "\n" + qinfo);
            //System.out.println("\n" + qinfo.getConfiguration());
            //System.out.println("\n" + qinfo.getOptimizationCount());
            //System.out.println("\n" + configuration);

            // Alkis: I am not sure what this check does. An SQL statement processed by WFIT
            // should not have a configuration associated with it.
            // assertThat(qinfo.getConfiguration().size(), is(1));

            // Alkis: Same here -- the optimization count can be 1
            // only if there the single index is not useful for the query
            // assertThat(qinfo.getOptimizationCount(), is(1));

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

            for (Index index : candidateSet)
                if (!pool.containsContent(index))
                    pool.add(index);
        }

        return pool;
    }
}
