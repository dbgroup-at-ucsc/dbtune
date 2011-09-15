/* **************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                         *
 *                                                                              *
 *   Licensed under the Apache License, Version 2.0 (the "License");            *
 *   you may not use this file except in compliance with the License.           *
 *   You may obtain a copy of the License at                                    *
 *                                                                              *
 *       http://www.apache.org/licenses/LICENSE-2.0                             *
 *                                                                              *
 *   Unless required by applicable law or agreed to in writing, software        *
 *   distributed under the License is distributed on an "AS IS" BASIS,          *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 *   See the License for the specific language governing permissions and        *
 *   limitations under the License.                                             *
 * **************************************************************************** */
package edu.ucsc.dbtune.workload;

import java.sql.SQLException;

/**
 * Represents a SQL statement. Each {@code SQLStatement} object is tied to a {@code String} object 
 * that contains the actual literal contents of the SQL statement.
 *
 * @author Ivo Jimenez
 */
public class SQLStatement
{
    /** category of statement */
    private SQLCategory category;

    /** literal contents of the statement */
    private String sql;

    /**
     * Constructs a {@code SQLStatement}. The constructor tries to infer the category of the 
     * statement using the {@link SQLCategory#from} method.
     *
     * @param sql
     *      a sql statement.
     * @throws SQLException
     *     if no category can't be extracted from the given string.
     * @see SQLCategory#from
     */
    public SQLStatement(String sql) throws SQLException {
        this(sql, SQLCategory.from(sql));
    }

    /**
     * Constructs a {@code SQLStatement} given its category and the literal contents.
     *
     * @param category
     *      the corresponding {@link SQLCategory} representing the category of statement.
     * @param sql
     *      a sql statement.
     */
    public SQLStatement(String sql, SQLCategory category) {
        this.category = category;
        this.sql      = sql;
    }

    /**
     * Returns the category of statement.
     *
     * @return
     *     a sql category.
     * @see SQLCategory
     */
    public SQLCategory getSQLCategory() {
        return category;
    }

    /**
     * Returns the actual SQL statement.
     *
     * @return
     *     a string containing the SQL statement that was optimized.
     */
    public String getSQL() {
        return sql;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[ category=" + category +
               " text=\"" + sql + "\"]";
    }
}
