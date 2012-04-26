package edu.ucsc.dbtune;

import java.sql.Connection;
import java.sql.DriverManager;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.extraction.DB2Extractor;
import edu.ucsc.dbtune.metadata.extraction.MetadataExtractor;
import edu.ucsc.dbtune.metadata.extraction.MySQLExtractor;
import edu.ucsc.dbtune.metadata.extraction.PGExtractor;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;
import edu.ucsc.dbtune.optimizer.MySQLOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.PGOptimizer;
import edu.ucsc.dbtune.util.Environment;

import org.junit.Test;
import org.junit.runner.RunWith;
 
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static edu.ucsc.dbtune.DBTuneInstances.configureDB2;
import static edu.ucsc.dbtune.DBTuneInstances.configureDBMSOptimizer;
import static edu.ucsc.dbtune.DBTuneInstances.configureIBGOptimizer;
import static edu.ucsc.dbtune.DBTuneInstances.configureINUMOptimizer;
import static edu.ucsc.dbtune.DBTuneInstances.configureMySQL;
import static edu.ucsc.dbtune.DBTuneInstances.configurePG;
import static edu.ucsc.dbtune.util.OptimizerUtils.getBaseOptimizer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import static org.junit.Assert.assertThat;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

/**
 * @author Ivo Jimenez
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DatabaseSystem.class })
public class DatabaseSystemTest
{
    /**
     * Checks if a system is constructed correctly.
     *
     * @throws Exception
     *      if fails
     */
    @Test
    public void testConstructor() throws Exception
    {
        Connection        con = mock(Connection.class);
        Catalog           cat = mock(Catalog.class);
        MetadataExtractor ext = mock(MetadataExtractor.class);
        Optimizer         opt = mock(Optimizer.class);

        when(ext.extract(con)).thenReturn(cat);

        DatabaseSystem db  = new DatabaseSystem(con, ext, opt);

        assertThat(db.getConnection(), is(con));
        assertThat(db.getCatalog(), is(cat));
        assertThat(db.getOptimizer(), is(opt));
    }

    /**
     * Checks the static (factory) methods.
     * @throws Exception
     *      if fails
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
        assertThat(DatabaseSystem.newOptimizer(env, con) instanceof DB2Optimizer, is(true));
        assertThat(DatabaseSystem.newExtractor(env) instanceof DB2Extractor, is(true));
        assertThat(DatabaseSystem.newOptimizer(env, con).getDelegate(), is(nullValue()));

        // check MySQL
        env = configureDBMSOptimizer(configureMySQL());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env, con) instanceof MySQLOptimizer, is(true));
        assertThat(DatabaseSystem.newExtractor(env) instanceof MySQLExtractor, is(true));
        assertThat(DatabaseSystem.newOptimizer(env, con).getDelegate(), is(nullValue()));

        // check PostgreSQL
        env = configureDBMSOptimizer(configurePG());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env, con) instanceof PGOptimizer, is(true));
        assertThat(DatabaseSystem.newExtractor(env) instanceof PGExtractor, is(true));
        assertThat(DatabaseSystem.newOptimizer(env, con).getDelegate(), is(nullValue()));
        
        // check IBG
        env = configureIBGOptimizer(configureDB2());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env, con) instanceof IBGOptimizer, is(true));
        assertThat(DatabaseSystem.newExtractor(env) instanceof DB2Extractor, is(true));
        assertThat(
                DatabaseSystem.newOptimizer(env, con).getDelegate() instanceof DB2Optimizer, 
                is(true));

        env = configureIBGOptimizer(configureMySQL());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env, con) instanceof IBGOptimizer, is(true));
        assertThat(DatabaseSystem.newExtractor(env) instanceof MySQLExtractor, is(true));
        assertThat(
                DatabaseSystem.newOptimizer(env, con).getDelegate() instanceof MySQLOptimizer, 
                is(true));

        env = configureIBGOptimizer(configurePG());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env, con) instanceof IBGOptimizer, is(true));
        assertThat(DatabaseSystem.newExtractor(env) instanceof PGExtractor, is(true));
        assertThat(
                DatabaseSystem.newOptimizer(env, con).getDelegate() instanceof PGOptimizer, 
                is(true));

        // check INUM
        env = configureINUMOptimizer(configureDB2());
        assertThat(DatabaseSystem.newOptimizer(env, con), is(notNullValue()));
        assertThat(DatabaseSystem.newExtractor(env) instanceof DB2Extractor, is(true));
        assertThat(
                DatabaseSystem.newOptimizer(env, con).getDelegate() instanceof DB2Optimizer, 
                is(true));

        env = configureINUMOptimizer(configureMySQL());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env, con), is(notNullValue()));
        assertThat(DatabaseSystem.newExtractor(env) instanceof MySQLExtractor, is(true));
        assertThat(
                DatabaseSystem.newOptimizer(env, con).getDelegate() instanceof MySQLOptimizer, 
                is(true));

        env = configureINUMOptimizer(configurePG());

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env, con), is(notNullValue()));
        assertThat(DatabaseSystem.newExtractor(env) instanceof PGExtractor, is(true));
        assertThat(
                DatabaseSystem.newOptimizer(env, con).getDelegate() instanceof PGOptimizer, 
                is(true));

        // check INUM on IBG
        env = configureINUMOptimizer(configureIBGOptimizer(configureDB2()));
        assertThat(DatabaseSystem.newOptimizer(env, con), is(notNullValue()));
        assertThat(DatabaseSystem.newExtractor(env) instanceof DB2Extractor, is(true));
        assertThat(
                DatabaseSystem.newOptimizer(env, con).getDelegate() instanceof IBGOptimizer, 
                is(true));
        assertThat(
                getBaseOptimizer(DatabaseSystem.newOptimizer(env, con)) instanceof DB2Optimizer, 
                is(true));

        env = configureINUMOptimizer(configureIBGOptimizer(configureMySQL()));

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env, con), is(notNullValue()));
        assertThat(DatabaseSystem.newExtractor(env) instanceof MySQLExtractor, is(true));
        assertThat(
                DatabaseSystem.newOptimizer(env, con).getDelegate() instanceof IBGOptimizer, 
                is(true));
        assertThat(
                getBaseOptimizer(DatabaseSystem.newOptimizer(env, con)) instanceof MySQLOptimizer, 
                is(true));

        env = configureINUMOptimizer(configureIBGOptimizer(configurePG()));

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env, con), is(notNullValue()));
        assertThat(DatabaseSystem.newExtractor(env) instanceof PGExtractor, is(true));
        assertThat(
                DatabaseSystem.newOptimizer(env, con).getDelegate() instanceof IBGOptimizer, 
                is(true));
        assertThat(
                getBaseOptimizer(DatabaseSystem.newOptimizer(env, con)) instanceof PGOptimizer, 
                is(true));


        // check IBG on INUM
        env = configureIBGOptimizer(configureINUMOptimizer(configureDB2()));
        assertThat(DatabaseSystem.newOptimizer(env, con), is(notNullValue()));
        assertThat(DatabaseSystem.newExtractor(env) instanceof DB2Extractor, is(true));
        assertThat(
                !(DatabaseSystem.newOptimizer(env, con).getDelegate() instanceof IBGOptimizer), 
                is(true));
        assertThat(
                getBaseOptimizer(DatabaseSystem.newOptimizer(env, con)) instanceof DB2Optimizer, 
                is(true));

        env = configureIBGOptimizer(configureINUMOptimizer(configureMySQL()));

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env, con), is(notNullValue()));
        assertThat(DatabaseSystem.newExtractor(env) instanceof MySQLExtractor, is(true));
        assertThat(
                !(DatabaseSystem.newOptimizer(env, con).getDelegate() instanceof IBGOptimizer), 
                is(true));
        assertThat(
                getBaseOptimizer(DatabaseSystem.newOptimizer(env, con)) instanceof MySQLOptimizer, 
                is(true));

        env = configureIBGOptimizer(configureINUMOptimizer(configurePG()));

        assertThat(DatabaseSystem.newConnection(env), is(con));
        assertThat(DatabaseSystem.newOptimizer(env, con), is(notNullValue()));
        assertThat(DatabaseSystem.newExtractor(env) instanceof PGExtractor, is(true));
        assertThat(
                !(DatabaseSystem.newOptimizer(env, con).getDelegate() instanceof IBGOptimizer), 
                is(true));
        assertThat(
                getBaseOptimizer(DatabaseSystem.newOptimizer(env, con)) instanceof PGOptimizer, 
                is(true));

    }

    /**
     * Checks if a system is constructed correctly when built through the factory method.
     *
     * @throws Exception
     *      if fails
     */
    @Test
    public void testFactoryConstruction() throws Exception
    {
        Connection        con = mock(Connection.class);
        Catalog           cat = mock(Catalog.class);
        MetadataExtractor ext = mock(MetadataExtractor.class);
        Optimizer         opt = mock(Optimizer.class);
        Environment       env = configurePG();

        when(ext.extract(con)).thenReturn(cat);

        mockStatic(DatabaseSystem.class);
        when(DatabaseSystem.newConnection(env)).thenReturn(con);
        when(DatabaseSystem.newExtractor(env)).thenReturn(ext);
        when(DatabaseSystem.newOptimizer(env, con)).thenReturn(opt);

        DatabaseSystem db = DatabaseSystem.Factory.newDatabaseSystem(env);

        verify(opt).setCatalog(cat);

        assertThat(db.getConnection(), is(con));
        assertThat(db.getCatalog(), is(cat));
        assertThat(db.getOptimizer(), is(opt));
    }
}
