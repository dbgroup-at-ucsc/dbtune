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
package edu.ucsc.dbtune.metadata.extraction;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.spi.Environment;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import static edu.ucsc.dbtune.util.SQLScriptExecuter.execute;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.SCHEMA;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

/**
 * Test for the generic metadata extraction class.
 * <p>
 * The test assumes that a database has been configured in the <code>build.properties</code> file.  
 * Depending on the value for the {@link EnvironmentProperties#URL} and {@link 
 * EnvironmentProperties#JDBC_DRIVER} property, the test instantiates and calls the appropriate 
 * {@link } implementation.
 * <p>
 * Also, the file {@code movies/create.sql} in folder {@link 
 * EnvironmentProperties#WORKLOADS_FOLDERNAME} is expected to be present and contain the 
 * DBMS-dependant SQL statements that create and load the <i>Movies</i> database.
 * <p>
 * The test fails entirely if the above pre-conditions aren't met.
 *
 * @author Ivo Jimenez
 * @see EnvironmentProperties
 * @see DatabaseSystem
 */
public class GenericJDBCExtractorFunctionalTest
{
    private static DatabaseSystem db;
    private static Environment    en;
    private static Catalog        catalog;
    private static Schema         schema;

    /**
     * Executes the SQL script that should contain the 'movies' database, then extracts the metadata 
     * for this database using the appropriate {@link MetadataExtractor} implementor.
     */
    @BeforeClass
    public static void setUp() throws Exception
    {
        Properties cfg;
        String     ddl;

        cfg = new Properties(Environment.getInstance().getAll());

        cfg.setProperty(SCHEMA,"movies");

        en  = new Environment(cfg);
        db  = DatabaseSystem.newDatabaseSystem(en);
        ddl = en.getScriptAtWorkloadsFolder("movies/create.sql");

        {
            // DatabaseSystem reads the catalog as part of its creation, so we need to wipe anything 
            // in the movies schema and reload the data. Then create a DB again to get a fresh 
            // catalog
            //execute(db.getConnection(), ddl);
            db.getConnection().close();
            db = null;
            db = DatabaseSystem.newDatabaseSystem(en);
        }

        catalog = db.getCatalog();
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        db.getConnection().close();
    }

    /**
     * Tests that the schema we created exists.
     */
    @Test
    public void testCatalogExists() throws Exception
    {
        assertThat(catalog.getSchemas().size(),is(1));
    }
}
