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
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune;

import org.junit.Test;
/*
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
*/

//@RunWith(PowerMockRunner.class)
/**
 * @author Ivo Jimenez
 */
//@PrepareForTest({DatabaseSystem.class})
public class DatabaseSystemTest
{
    /**
     * Checks if a system is constructed correctly. All dependencies are mocked
     */
    @Test
    public void testConstruction() throws Exception
    {
        /*
         * XXX: this tests depends on having Connection mocks for:
         *        - metadata.extraction.GenericJDBCExtractor
         *        - metadata.extraction.PGExtractor
         *        - metadata.extraction.MySQLExctractor
         *        - metadata.extraction.DB2Exctractor
         *        - metadata.extraction.IBGOptimizer
         *        - metadata.extraction.PGOptimizer
         *        - metadata.extraction.DB2Optimizer
         *        - metadata.extraction.MySQLOptimizer
         *        - metadata.extraction.InumOptimizer
         *
         *      with mocks for all the above, we can easily use them in the DatabaseSystem 
         *      construction
        mockStatic(DriverManager.class);

        Connection c = Mockito.mock(Connection.class);

        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(c);

        DatabaseSystem db = new DatabaseSystem();

        assertThat(db.getConnection(), is(c));

        */
    }
}
