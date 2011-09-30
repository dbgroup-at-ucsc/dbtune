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
package edu.ucsc.dbtune.metadata;

import java.sql.SQLException;
import java.util.Iterator;

import edu.ucsc.dbtune.util.Objects;

/**
 * Metadata for a table.
 *
 * @author Ivo Jimenez
 */
public class Table extends DatabaseObject implements Iterable<Column>
{
    protected int type;

    public static final int REGULAR = 1;
    public static final int MDC     = 2;

    /**
     * Constructor
     *
     * @param name
     *     name of the table
     * @param schema
     *     schema where the new table will be contained
     * @throws SQLException
     *     if a table with the given name is already contained in the schema
     */
    public Table(Schema schema, String name) throws SQLException
    {
        super(schema, name);

        this.type = REGULAR;
    }

    /**
     * Copy Constructor
     *
     * @param other
     *     object being copied
     */
    protected Table(Table other)
    {
        super(other);

        type = other.type;
    }

    /**
     * Returns the schema that contains this table. Convenience method that accomplishes what {@link 
     * #getContainer} does but without requiring the user to cast. In other words, the following is 
     * true {@code getSchema() == (Schema)getContainer()}.
     *
     * @return
     *     the schema that contains this object
     */
    public Schema getSchema()
    {
        return (Schema) container;
    }

    /**
     * Finds a column whose name matches the given argument. Convenience method that accomplishes 
     * what {@link #find} does but without making requiring the user to cast. In other words, the 
     * following is true {@code findColumn("name") == (Column)find("name")}.
     *
     * @param name
     *     name of the object that is searched for in <code>this</code> table.
     * @return
     *     the column that has the given name; null if not found
     */
    public Column findColumn(String name)
    {
        return (Column) find(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseObject newContainee(String name) throws SQLException
    {
        return new Column(this,name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Column> iterator()
    {
        return Objects.<Iterator<Column>>as(containees.iterator());
    }
    public Iterable<Column> columns()
    {
        return Objects.<Iterable<Column>>as(containees);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(DatabaseObject dbo)
    {
        return dbo instanceof Column;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equalsContent(Object other)
    {
        throw new RuntimeException("not implemented yet");
    }
}
