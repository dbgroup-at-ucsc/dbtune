package edu.ucsc.dbtune.inum;

import com.google.common.collect.Sets;

import java.sql.Connection;
import java.util.Properties;
import java.util.Set;

import edu.ucsc.dbtune.DBTuneInstances;
import edu.ucsc.dbtune.SharedFixtures;
import edu.ucsc.dbtune.inum.InumInterestingOrdersExtractor.ColumnInformation;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.SQLTypes;

import org.junit.Test;
import org.mockito.Mockito;

import static edu.ucsc.dbtune.metadata.SQLTypes.INT;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests the {@link InterestingOrdersExtractor} interface.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InterestingOrdersExtractorTest
{
  private static final String SAMPLE_QUERY = "SELECT column_1, column_2\n"
      + "FROM schema_1.table_1\n"
      + "WHERE column_1 = 'IBM'\n"
      + "ORDER BY column_1 DESC, column_2 ASC;";

  @Test public void testExtractInterestingOrders() throws Exception
 {
    final Catalog                    catalog        = DBTuneInstances.configureCatalog();
    final ColumnPropertyLookup       columnProperty = configureProperty();
    final InterestingOrdersExtractor extractor      = new InumInterestingOrdersExtractor(catalog, columnProperty);
    final Set<Index> ios = extractor.extractInterestingOrders(SAMPLE_QUERY);
    assertThat(ios.isEmpty(), is(false));
  }

  private static ColumnPropertyLookup configureProperty() throws Exception
  {
    final ColumnPropertyLookup prop       = Mockito.mock(ColumnPropertyLookup.class);
    final Connection  connection = SharedFixtures.configureConnection();
    Mockito.when(prop.getDatabaseConnection()).thenReturn(connection);
    Mockito.doNothing().when(prop).refresh();
    final Set<ColumnInformation> info = populateColumnInformationSet();
    Mockito.when(prop.getColumnInformation(Mockito.anyInt())).thenReturn(info);
    Mockito.when(prop.getProperty(Mockito.eq("column_1".toUpperCase()))).thenReturn("schema_1.table_1".toUpperCase());
    Mockito.when(prop.getProperty(Mockito.eq("column_2".toUpperCase()))).thenReturn("schema_1.table_1".toUpperCase());
    Mockito.when(prop.getProperty(Mockito.eq("column_3".toUpperCase()))).thenReturn("schema_1.table_1".toUpperCase());
    Mockito.when(prop.getProperties()).thenReturn(populateProperties("schema_1.table_1", info));
    Mockito.when(prop.getColumnDataType(Mockito.eq("schema_1.table_1"), Mockito.eq("column_1".toUpperCase()))).thenReturn(INT);
    Mockito.when(prop.getColumnDataType(Mockito.eq("schema_1.table_1"), Mockito.eq("column_2".toUpperCase()))).thenReturn(INT);
    return prop;
  }

  private static Set<ColumnInformation> populateColumnInformationSet()
  {
    return Sets.newHashSet(
        singleColumnInformation("column_1", 1, INT),
        singleColumnInformation("column_2", 2, INT)
    );
  }

  private static Properties populateProperties(String tableName, Set<ColumnInformation> columnInformationSet)
  {
    final Properties properties = new Properties();
    for (ColumnInformation each : columnInformationSet){
      properties.setProperty(each.columnName.toUpperCase(), tableName);
    }
    return properties;
  }

  private static ColumnInformation singleColumnInformation(String colName, int attnum, int columnType)
  {
    final ColumnInformation info = new ColumnInformation();
    info.columnName = colName;
    info.attnum     = attnum;

    final String tip      = SQLTypes.codeToName(columnType);
    final String nullable = "f";

    info.isNullable = nullable.compareTo("f") == 0;
    info.atttypid   = columnType;
    info.columnType = tip;
    return info;
  }
}
