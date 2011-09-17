package edu.ucsc.dbtune.inum;

import com.google.common.collect.Sets;
import edu.ucsc.dbtune.SharedFixtures;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.inum.InumInterestingOrdersExtractor.ColumnInformation;
import java.sql.Connection;
import java.util.Properties;
import java.util.Set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests the {@link InterestingOrdersExtractor} interface.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InterestingOrdersExtractorTest {
  private static final String SAMPLE_QUERY = "SELECT supplier_city, supplier_state\n"
      + "FROM suppliers\n"
      + "WHERE supplier_name = 'IBM'\n"
      + "ORDER BY supplier_city DESC, supplier_state ASC;";

  @Test public void testExtractInterestingOrders() throws Exception {
    final ColumnPropertyLookup columnProperty = configureProperty();
    final InterestingOrdersExtractor extractor = new InumInterestingOrdersExtractor(columnProperty);
    final Configuration ios = extractor.extractInterestingOrders(SAMPLE_QUERY);
    assertThat(ios.toList().isEmpty(), is(false));
  }

  private static ColumnPropertyLookup configureProperty() throws Exception {
    final ColumnPropertyLookup prop       = Mockito.mock(ColumnPropertyLookup.class);
    final Connection  connection = SharedFixtures.configureConnection();
    Mockito.when(prop.getDatabaseConnection()).thenReturn(connection);
    Mockito.doNothing().when(prop).refresh();
    final Set<ColumnInformation> info = populateColumnInformationSet();
    Mockito.when(prop.getColumnInformation(Mockito.anyInt())).thenReturn(info);
    Mockito.when(prop.getProperty(Mockito.eq("supplier_city".toUpperCase()))).thenReturn("suppliers".toUpperCase());
    Mockito.when(prop.getProperty(Mockito.eq("supplier_state".toUpperCase()))).thenReturn("suppliers".toUpperCase());
    Mockito.when(prop.getProperty(Mockito.eq("supplier_name".toUpperCase()))).thenReturn("suppliers".toUpperCase());
    Mockito.when(prop.getProperties()).thenReturn(populateProperties("suppliers", info));
    Mockito.when(prop.getColumnDataType(Mockito.eq("suppliers"), Mockito.eq("supplier_city".toUpperCase()))).thenReturn(12);
    Mockito.when(prop.getColumnDataType(Mockito.eq("suppliers"), Mockito.eq("supplier_state".toUpperCase()))).thenReturn(12);
    return prop;
  }

  private static Set<ColumnInformation> populateColumnInformationSet(){
    return Sets.newHashSet(
        singleColumnInformation("supplier_city", 1, 12),
        singleColumnInformation("supplier_state", 2, 12)
    );
  }

  private static Properties populateProperties(String tableName, Set<ColumnInformation> columnInformationSet) {
    final Properties properties = new Properties();
    for(ColumnInformation each : columnInformationSet){
      properties.setProperty(each.columnName.toUpperCase(), tableName);
    }
    return properties;
  }

  private static ColumnInformation singleColumnInformation(String colName, int attnum, int columnType){
    final ColumnInformation info = new ColumnInformation();
    info.columnName = colName;
    info.attnum     = attnum;

    final String tip      = InumInterestingOrdersExtractor.getColumnType(columnType);
    final String nullable = "f";

    info.isNullable = nullable.compareTo("f") == 0;
    info.atttypid   = columnType;
    info.columnType = tip;
    return info;
  }
}
