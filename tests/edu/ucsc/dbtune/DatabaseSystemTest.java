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

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.optimizer.Optimizer;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.DriverManager;

import static edu.ucsc.dbtune.DbTuneMocks.makeOptimizerMock;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
/**
 * @author Ivo Jimenez
 */
@PrepareForTest({DatabaseSystem.class})
public class DatabaseSystemTest
{
    /**
     * Checks if a system is constructed correctly.
     */
    @Test
    public void testConstructor() throws Exception
    {
        mockStatic(DriverManager.class);

        Connection     con = mock(Connection.class);
        Catalog        cat = mock(Catalog.class);
        Optimizer      opt = makeOptimizerMock();
        DatabaseSystem db  = new DatabaseSystem(con,cat,opt);

        assertThat(db.getConnection(), is(con));
        assertThat(db.getCatalog(), is(cat));
        assertThat(db.getOptimizer(), is(opt));
    }

    /**
     * Checks that the static (factory) methods work
     */
    @Test
    public void testFactory() throws Exception
    {
        mockStatic(DriverManager.class);

        Connection     con = mock(Connection.class);
        Catalog        cat = mock(Catalog.class);
        Optimizer      opt = makeOptimizerMock();

        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(con);

        DatabaseSystem db  = new DatabaseSystem(con,cat,opt);

        assertThat(db.getConnection(), is(con));
        assertThat(db.getCatalog(), is(cat));
        assertThat(db.getOptimizer(), is(opt));
    }
}
