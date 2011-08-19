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

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

/**
 * @author Ivo Jimenez
 */
public class DatabaseSystemTestFunctional
{
    /**
     * Checks if a system is constructed correctly. This test depends on the contents of the default 
     * configuration file read by {@link Environment}
     *
     * @see Environment
     */
    @Test
    public void testDBConnection() throws Exception
    {
        DatabaseSystem db = DatabaseSystem.newDatabaseSystem();

        assertThat(db.getConnection() != null, is(true));
        assertThat(db.getOptimizer() != null, is(true));
        assertThat(db.getCatalog() != null, is(true));

        db.getConnection().close();
    }
}
