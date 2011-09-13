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

import java.util.Arrays;
import java.util.List;

/**
 * Enum representing possible SQL categories.
 *
 * @author Karl Schnaitter
 * @author Huascar Sanchez
 * @author Ivo Jimenez
 */
public enum SQLCategory
{
    SELECT("S"),
    INSERT("I"),
    UPDATE("U"),
    DELETE("D"),
    /**
     * Convenience element that represents all DML statements but {@link SELECT}
     */
    NOT_SELECT("I", "U", "D", "M", "UC", "DC"),
    /**
     * Any other type
     */
    UNKNOWN("O");

    /** codes corresponding to the category */
    private final List<String> code;

    /**
     * Creates a new category with the given code-names.
     *
     * @param code
     *     one or more strings containing the codes of the category
     */
    SQLCategory(String... code)
    {
        this.code = Arrays.asList(code);
    }

    /**
     * Checks whether or not the given category is part of another. For aggregated categories (eg. 
     * NOT_SELECT), a category defines broader categories.
     *
     * @param category
     *     the category that is checked against this one.
     * @return
     *     {@code true} if the category contains the given {@code category}; {@code false} 
     *     otherwise.
     */
    boolean contains(SQLCategory category)
    {
        for(String id : category.code) {
            if(contains(id)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether or not the given code is part of this category.
     *
     * @param code
     *     the code that is checked against the list of codes contained in {@code this}.
     * @return
     *     {@code true} if the category contains the given {@code code}; {@code false} otherwise.
     */
    boolean contains(String code)
    {
        return this.code.contains(code);
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
    public static SQLCategory from(String code)
    {
        for(SQLCategory category : values()){
            if(category.contains(code)){
                return category;
            }
        }
        return UNKNOWN;
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
    public boolean isSame(SQLCategory that)
    {
        return this == that || this.contains(that) || that.contains(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return code.toString();
    }
}
