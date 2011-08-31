package edu.ucsc.dbtune.inum;

import com.google.common.collect.Sets;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
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
    final ColumnProperty columnProperty = configureProperty();
    final InterestingOrdersExtractor extractor = new InumInterestingOrdersExtractor(columnProperty);
    final Set<DBIndex> ios = extractor.extractInterestingOrders(SAMPLE_QUERY);
    assertThat(ios.isEmpty(), is(false));
  }

  private static ColumnProperty configureProperty(){
    final ColumnProperty      prop       = Mockito.mock(ColumnProperty.class);
    final DatabaseConnection  connection = configureDatabaseConnection();
    Mockito.when(prop.getDatabaseConnection()).thenReturn(connection);
    Mockito.doNothing().when(prop).refresh();
    final Set<ColumnInformation> info = populateColumnInformationSet();
    Mockito.when(prop.getColumnInformation(Mockito.anyInt())).thenReturn(info);
    Mockito.when(prop.getProperty(Mockito.eq("supplier_city".toUpperCase()))).thenReturn("suppliers".toUpperCase());
    Mockito.when(prop.getProperty(Mockito.eq("supplier_state".toUpperCase()))).thenReturn("suppliers".toUpperCase());
    Mockito.when(prop.getProperty(Mockito.eq("supplier_name".toUpperCase()))).thenReturn("suppliers".toUpperCase());
    Mockito.when(prop.getProperties()).thenReturn(populateProperties("suppliers", info));
    return prop;
  }

  private static Set<ColumnInformation> populateColumnInformationSet(){
    return Sets.newHashSet(
        singleColumnInformation("supplier_city", 1, 1043),
        singleColumnInformation("supplier_state", 2, 1043)
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

  private static DatabaseConnection configureDatabaseConnection(){
    final DatabaseConnection mockConnection = Mockito.mock(DatabaseConnection.class);
    Connection jdbcConnection = Mockito.mock(Connection.class);
    Mockito.when(mockConnection.getJdbcConnection()).thenReturn(jdbcConnection);
    return mockConnection;
  }
}
