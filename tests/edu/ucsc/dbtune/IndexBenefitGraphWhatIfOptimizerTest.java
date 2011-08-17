/****************************************************************************
 * Copyright 2010 Huascar A. Sanchez                                        *
 *                                                                          *
 * Licensed under the Apache License, Version 2.0 (the "License");          *
 * you may not use this file except in compliance with the License.         *
 * You may obtain a copy of the License at                                  *
 *                                                                          *
 *     http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                          *
 * Unless required by applicable law or agreed to in writing, software      *
 * distributed under the License is distributed on an "AS IS" BASIS,        *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 * See the License for the specific language governing permissions and      *
 * limitations under the License.                                           *
 ****************************************************************************/
package edu.ucsc.dbtune;
/*
import edu.ucsc.dbtune.connectivity.ConnectionManager;

import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;

import java.util.ArrayList;
import java.util.List;
*/

import org.junit.Before;
import org.junit.Test;

/*
import static edu.ucsc.dbtune.metadata.Index.NON_UNIQUE;
import static edu.ucsc.dbtune.metadata.Index.SECONDARY;
import static edu.ucsc.dbtune.metadata.Index.UNCLUSTERED;
*/

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class IndexBenefitGraphWhatIfOptimizerTest {
    //private ConnectionManager connectionManager;
    @Before
    public void setUp() throws Exception {
         //connectionManager = DBTuneInstances.newPGDatabaseConnectionManager();
    }

    @Test
    public void testWhatIfOptimizerForRandQuery() throws Exception {
        //final DatabaseConnection connect = connectionManager.connect();
        //final IBGOptimizer whatIf  = new IBGOptimizer(connect.getOptimizer());
        // Mocking for empty configuration is not returning a ResultSet correctly
        // final double cost = whatIf.explain(basicQuery(), getIndexes()).getCost();
        //assertTrue(Double.compare(cost, 1.0) == 0);
    }
/*
    private List<Index> getIndexes() throws Exception
    {
        List<Index> list = new ArrayList<Index>();

        Table t = new Table(1);
        Column c;
        Index i;

        c = new Column(1);
        t.add(c);
        i = new Index( "index1", t, SECONDARY,UNCLUSTERED, NON_UNIQUE);
        i.setId(1);
        i.add(c);
        t.add(i);
        list.add(i);
        c = new Column(2);
        t.add(c);
        i = new Index( "index2", t, SECONDARY,UNCLUSTERED, NON_UNIQUE);
        i.setId(2);
        i.add(c);
        t.add(i);
        list.add(i);
        c = new Column(3);
        t.add(c);
        i = new Index( "index3", t, SECONDARY,UNCLUSTERED, NON_UNIQUE);
        i.setId(3);
        i.add(c);
        t.add(i);
        list.add(i);

        return list;
    }

    private static String basicQuery(){
        return "SELECT R.salary, R.timeOffCount FROM R WHERE R.fullname = 'Bruce Wayne';";
    }*/
}
