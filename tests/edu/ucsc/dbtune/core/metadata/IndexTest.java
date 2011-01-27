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
 * ****************************************************************************
 */

package edu.ucsc.dbtune.core.metadata;

import java.util.List;
import java.util.ArrayList;

import org.junit.Test;
import org.junit.Before;

import static edu.ucsc.dbtune.core.metadata.Index.PRIMARY;
import static edu.ucsc.dbtune.core.metadata.Index.CLUSTERED;
import static edu.ucsc.dbtune.core.metadata.Index.UNIQUE;
import static edu.ucsc.dbtune.core.metadata.SQLTypes.INTEGER;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Test for the Index class
 */
public class IndexTest
{ 
    List<Column> columns;
    Table table;

    @Before
    public void runBeforeEveryTest() throws Exception
    {
        Column column;

        columns = new ArrayList<Column>();
        table   = new Table( "test_table" );

        for( int i = 0; i < 10; i++ ) 
        {
            column = new Column( "col_" + i, INTEGER );

            table.add( column );
            columns.add( column );
        }
    }

    @Test
    public void testConstructors() throws Exception
    {
        Index index;

        index = new Index( table, PRIMARY, CLUSTERED, UNIQUE );

        assertEquals( table, index.getTable() );

        index = new Index( columns, PRIMARY, CLUSTERED, UNIQUE );

        assertEquals( table, index.getTable() );
        assertEquals( table.getColumns().size(), index.size() );
    }

    @Test
    public void testEquality() throws Exception
    {
        Index index1;
        Index index2;
        Index index3;

        index1 = new Index( columns, PRIMARY, CLUSTERED, UNIQUE );
        index2 = new Index( columns, PRIMARY, CLUSTERED, UNIQUE );
        index3 = new Index( table, PRIMARY, CLUSTERED, UNIQUE );

        for( int i = 0; i < table.getColumns().size(); i++ )
        {
            if( i % 2 == 0 )
            {
                index3.add( table.getColumns().get(i) );
            }
        }

        assertFalse( index1 == index2 );
        assertFalse( index1.equals( index3 ) );
        assertTrue( index1.equals( index2 ) );
        assertTrue( index2.equals( index1 ) );
    }

    @Test
    public void testHashing() throws Exception
    {
        Index index1;
        Index index2;
        Index index3;

        index1 = new Index( columns, PRIMARY, CLUSTERED, UNIQUE );
        index2 = new Index( columns, PRIMARY, CLUSTERED, UNIQUE );
        index3 = new Index( table, PRIMARY, CLUSTERED, UNIQUE );

        for( int i = 0; i < table.getColumns().size(); i++ )
        {
            if( i % 2 == 0 )
            {
                index3.add( table.getColumns().get(i) );
            }
        }

        assertFalse( index1.hashCode() == index3.hashCode() );
        assertEquals( index1.hashCode(), index2.hashCode() );

        index1.setClustered( false );

        assertFalse( index1.hashCode() == index2.hashCode() );

        index2.setClustered( false );

        assertEquals( index1.hashCode(), index2.hashCode() );

        index1.setId( 10 );
        index2.setId( 11 );

        assertFalse( index1.hashCode() == index2.hashCode() );

        index2.setId( 10 );

        assertEquals( index1.hashCode(), index2.hashCode() );
    }
}
