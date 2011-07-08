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

import java.io.StringReader;

import org.junit.Test;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for the {@link SQLStatement} class.
 *
 * @author Ivo Jimenez
 */
public class WorkloadTest
{
    /**
     * checks that a character stream is parsed correctly and a set of {@link SQLStatement} objects 
     * is obtained properly 
     */
    @Test
    public void testConstructor() throws Exception
    {
        String sqls =
            "select * from a\n" +
            "update tbl set a = 0 where something = 'foo';\n" +
            "-- comment that shouldn't be take into accountupdate tbl set a = 0 where something = 'foo';\n" +
            "      -- this one also    \n" +
            "   update tbl set a = 0 where something = 'foo'   \n" +
            "select * from a  \n" +
            "    with ss as (select ca_county,d_qoy from tpch)\n" +
            "with ss as (select ca_county,d_qoy from tpch); \n";

        Workload wl = new Workload(new StringReader(sqls));

        assertThat(wl.get(0).getSQL(),is("select * from a"));
        assertThat(wl.get(1).getSQL(),is("update tbl set a = 0 where something = 'foo'"));
        assertThat(wl.get(2).getSQL(),is("update tbl set a = 0 where something = 'foo'"));
        assertThat(wl.get(3).getSQL(),is("select * from a"));
        assertThat(wl.get(4).getSQL(),is("with ss as (select ca_county,d_qoy from tpch)"));
        assertThat(wl.get(5).getSQL(),is("with ss as (select ca_county,d_qoy from tpch)"));

        assertThat(wl.get(0).getSQLCategory(), is(SQLCategory.QUERY));
        assertThat(wl.get(1).getSQLCategory(), is(SQLCategory.DML));
        assertThat(wl.get(2).getSQLCategory(), is(SQLCategory.DML));
        assertThat(wl.get(3).getSQLCategory(), is(SQLCategory.QUERY));
        assertThat(wl.get(4).getSQLCategory(), is(SQLCategory.QUERY));
        assertThat(wl.get(5).getSQLCategory(), is(SQLCategory.QUERY));
    }
}
