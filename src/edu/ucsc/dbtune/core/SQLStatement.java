/*
 * ****************************************************************************
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
 *  ****************************************************************************
 */

package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.util.ToStringBuilder;

import java.util.Arrays;
import java.util.List;

/**
 * represents a {@code SQL statement}.
 */
public class SQLStatement {
	private final SQLCategory type;
	private final String sql;

    /**
     * construct a {@code SQLStatement} using a {@code SQLCategory} and the
     * actual {@code query statement}.
     * @param category
     *      a {@link SQLCategory} type.
     * @param query
     *      a sql query.
     */
	public SQLStatement(SQLCategory category, String query) {
		type    = category;
		sql     = query;
	}

    public SQLCategory getSQLCategory() {
        return type;
    }

    public String getSQL() {
        return sql;
    }

    @Override
    public String toString() {
        return new ToStringBuilder<SQLStatement>(this)
               .add("type", getSQLCategory())
               .add("sql", getSQL())
               .toString();
    }

    /**
     * enum representing possible SQL categories.
     */
	public enum SQLCategory { 
        QUERY("S"),
        DML("I","U","D", "M", "UC", "DC"),
        OTHER();

        private final List<String> code;
        SQLCategory(String... code){
            this.code = Arrays.asList(code);
        }

        boolean contains(String code){
            return code.contains(code);
        }

        public static SQLCategory from(String code){
            for(SQLCategory each : values()){
                if(each.contains(code)){
                    return each;
                }
            }
            return OTHER;
        }

        @Override
        public String toString() {
            return code.toString();
        }
    }
}
