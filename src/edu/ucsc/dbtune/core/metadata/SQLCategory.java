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
package edu.ucsc.dbtune.core.metadata;

import java.util.Arrays;
import java.util.List;

/**
 * enum representing possible SQL categories.
 */
public enum SQLCategory {
    /**
     * A DML statement, specifically {@code SELECT} statements
     */
    QUERY("S"),
    /**
     * The rest of the DML statements (all but {@code SELECT})
     */
    DML("I", "U", "D", "M", "UC", "DC"),
    /**
     * Any other type
     */
    OTHER();

    /** codes corresponding to the category */
    private final List<String> code;

    /**
     * Creates a new category with the given code-names.
     *
     * @param code
     *     one or more strings containing the codes of the category
     */
    SQLCategory(String... code){
        this.code = Arrays.asList(code);
    }

    /**
     * Compares another category against this one.
     *
     * @param that
     *     other statement to compare against this
     * @return
     *     {@code true} if the given category is the same as {@code this} one; {@code false} 
     *     otherwise.
     */
    public boolean isSame(SQLCategory that){
        return this == that;
    }

    /**
     * Checks whether or not the given code is part of this category.
     *
     * @param code
     *     the code that is checked against the list of codes contained in {@code this}.
     * @return
     *     {@code true} if the category contains the given {@code code}; {@code false} otherwise.
     */
    boolean contains(String code){
        return code.contains(code);
    }

    /**
     * Returns the category that contains the given code.
     *
     * @param code
     *     code used to look for a category
     * @return
     *     the category whose one of its codes matches the given {@code code}; {@code OTHER} if no 
     *     category is found.
     */
    public static SQLCategory from(String code){
        for(SQLCategory category : values()){
            if(category.contains(code)){
                return category;
            }
        }
        return OTHER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return code.toString();
    }
}
