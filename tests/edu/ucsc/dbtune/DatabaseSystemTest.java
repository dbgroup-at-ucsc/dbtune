package edu.ucsc.dbtune;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.extraction.DB2Extractor;
import edu.ucsc.dbtune.metadata.extraction.MetadataExtractor;
import edu.ucsc.dbtune.metadata.extraction.MySQLExtractor;
import edu.ucsc.dbtune.metadata.extraction.PGExtractor;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;
import edu.ucsc.dbtune.optimizer.MySQLOptimizer;
import edu.ucsc.dbtune.optimizer.PGOptimizer;
import edu.ucsc.dbtune.util.Environment;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static edu.ucsc.dbtune.DBTuneMocks.makeOptimizerMock;
import static edu.ucsc.dbtune.DBTuneInstances.configureDB2;
import static edu.ucsc.dbtune.DBTuneInstances.configureMySQL;
import static edu.ucsc.dbtune.DBTuneInstances.configurePG;
import static edu.ucsc.dbtune.DBTuneInstances.configureDBMSOptimizer;
import static edu.ucsc.dbtune.DBTuneInstances.configureIBGOptimizer;
import static edu.ucsc.dbtune.DBTuneInstances.configureINUMOptimizer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
/**
 * @author Ivo Jimenez
 */
@PrepareForTest({DatabaseSystem.class})
public class DatabaseSystemTest
{
    /**
     * Checks if a system is constructed correctly.
     */
    @Test
    public void testConstructor() throws Exception
    {
        Connection        con = mock(Connection.class);
        Catalog           cat = mock(Catalog.class);
        MetadataExtractor ext = mock(MetadataExtractor.class);
        Optimizer         opt = makeOptimizerMock();

        when(ext.extract(con)).thenReturn(cat);

        DatabaseSystem db  = new DatabaseSystem(con,ext,opt);

        assertThat(db.getConnection(), is(con));
        assertThat(db.getCatalog(), is(cat));
        assertThat(db.getOptimizer(), is(opt));
    }

    /**
     * Checks the static (factory) methods
     */
    @Test
    public void testFactories() throws Exception
    {
        Environment env;

        Connection con = mock(Connection.class);

        mockStatic(DriverManager.class);

        when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(con);

        // check DB2
        env = configureDBMSOptimizer(configureDB2());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env,con) instanceof DB2Optimizer, is(true));
        assertThat(DatabaseSystem.newExtractor(env) instanceof DB2Extractor, is(true));

        // check MySQL
        env = configureDBMSOptimizer(configureMySQL());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env,con) instanceof MySQLOptimizer, is(true));
        assertThat(DatabaseSystem.newExtractor(env) instanceof MySQLExtractor, is(true));

        // check PostgreSQL
        env = configureDBMSOptimizer(configurePG());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env,con) instanceof PGOptimizer, is(true));
        assertThat(DatabaseSystem.newExtractor(env) instanceof PGExtractor, is(true));
        
        // check IBG
        env = configureIBGOptimizer(configureDB2());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env,con) instanceof IBGOptimizer, is(true));
        assertThat(DatabaseSystem.newExtractor(env) instanceof DB2Extractor, is(true));

        env = configureIBGOptimizer(configureMySQL());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env,con) instanceof IBGOptimizer, is(true));
        assertThat(DatabaseSystem.newExtractor(env) instanceof MySQLExtractor, is(true));

        env = configureIBGOptimizer(configurePG());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env,con) instanceof IBGOptimizer, is(true));
        assertThat(DatabaseSystem.newExtractor(env) instanceof PGExtractor, is(true));

        // check INUM
        env = configureINUMOptimizer(configureDB2());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        try {
            Optimizer opt = DatabaseSystem.newOptimizer(env,con);
            fail("Optimizer " + opt + " shouldn't be returned");
        } catch (SQLException e) {
            // nice;
        }
        assertThat(DatabaseSystem.newExtractor(env) instanceof DB2Extractor, is(true));

        env = configureINUMOptimizer(configureMySQL());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        try {
            Optimizer opt = DatabaseSystem.newOptimizer(env,con);
            fail("Optimizer " + opt + " shouldn't be returned");
        } catch (SQLException e) {
            // nice;
        }
        assertThat(DatabaseSystem.newExtractor(env) instanceof MySQLExtractor, is(true));

        env = configureINUMOptimizer(configurePG());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        try {
            Optimizer opt = DatabaseSystem.newOptimizer(env,con);
            fail("Optimizer " + opt + " shouldn't be returned");
        } catch (SQLException e) {
            // nice;
        }
        assertThat(DatabaseSystem.newExtractor(env) instanceof PGExtractor, is(true));
    }

    /**
     * Checks if a system is constructed correctly when built through the factory method
     */
    @Test
    public void testFactoryConstruction() throws Exception
    {
        Connection        con = mock(Connection.class);
        Catalog           cat = mock(Catalog.class);
        MetadataExtractor ext = mock(MetadataExtractor.class);
        Optimizer         opt = makeOptimizerMock();
        Environment       env = configurePG();

        when(ext.extract(con)).thenReturn(cat);

        mockStatic(DatabaseSystem.class);
        when(DatabaseSystem.newConnection(env)).thenReturn(con);
        when(DatabaseSystem.newExtractor(env)).thenReturn(ext);
        when(DatabaseSystem.newOptimizer(env,con)).thenReturn(opt);

        DatabaseSystem db = DatabaseSystem.Factory.newDatabaseSystem(env);

        verify(opt).setCatalog(cat);

        assertThat(db.getConnection(), is(con));
        assertThat(db.getCatalog(), is(cat));
        assertThat(db.getOptimizer(), is(opt));
    }
}
