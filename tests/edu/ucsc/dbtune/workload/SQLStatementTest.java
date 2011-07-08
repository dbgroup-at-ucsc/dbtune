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
package edu.ucsc.dbtune.workload;

import edu.ucsc.dbtune.core.metadata.SQLCategory;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for the {@link SQLStatement} class.
 *
 * @author Ivo Jimenez
 */
public class SQLStatementTest
{
    @BeforeClass
    public static void setUp() throws Exception
    {
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
    }

    /** checks the constructor */
    @Test
    public void testConstructor() throws Exception
    {
        SQLStatement sql = new SQLStatement(SQLCategory.QUERY, "select * from tbl");

        assertThat(sql.getSQL(),is("select * from tbl"));
    }
}
