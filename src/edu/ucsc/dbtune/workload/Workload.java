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

import edu.ucsc.dbtune.metadata.SQLCategory;

import java.io.Reader;
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.Iterable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a workload.
 *
 * @author Ivo Jimenez
 */
public class Workload implements Iterable<SQLStatement>
{
    private List<SQLStatement> sqls;

    /**
     * Creates a workload containing the set of SQL statements provided by the {@code 
     * workloadStream} object. It's assumed that there's one statement per line and only one-line 
     * comments (line beginning with string {@code "--"}).
     *
     * @param workloadStream
     *     stream that provides the set of SQL statements. One statement per line is assumed; 
     *     single-line comments only.
     */
    public Workload(Reader workloadStream) throws IOException {
        BufferedReader reader;
        String         line;
        String         lineLow;
        SQLCategory    category;

        sqls   = new ArrayList<SQLStatement>();
        reader = new BufferedReader(workloadStream);

        while((line = reader.readLine()) != null) {

            line    = line.trim();
            lineLow = line.toLowerCase();

            if(lineLow.startsWith("--")) {
                continue;
            } else if(lineLow.startsWith("select") || lineLow.startsWith("with")) {
                category = SQLCategory.QUERY;
            } else {
                category = SQLCategory.DML;
            }

            if(line.endsWith(";")) {
                sqls.add(new SQLStatement(line.substring(0, line.length()-1),category));
            } else {
                sqls.add(new SQLStatement(line,category));
            }
        }
    }

    /**
     * Returns the statement at the given position (zero-indexing).
     *
     * @param i
     *     index of the SQL statement retrieved
     */
    public SQLStatement get(int i) {
        return sqls.get(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<SQLStatement> iterator() {
        return sqls.iterator();
    }

    // SQLStatement get(String sql) // return the statement corresponding to the given sql
    //                              // string. This would require string matching stuff, or
    //                              // even having to do some query processing (parse, rewrite
    //                              // views, query flattening, etc) in order to compare queries
}
