package edu.ucsc.dbtune.inum;

import com.google.caliper.internal.guava.collect.Maps;
import edu.ucsc.dbtune.SharedFixtures;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import java.util.Map;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class QueryRecordTest {
  @Test public void testQueryRecord_Same() throws Exception {
    final Configuration first = SharedFixtures.configureConfiguration();
    final Configuration second = SharedFixtures.configureConfiguration();
    final String sql = "Select * from tests";
    final QueryRecord one = new QueryRecord(sql, first);
    final QueryRecord two = new QueryRecord(sql, second);

    assertThat(one, equalTo(two));
  }

  @Test public void testQueryRecord_Different() throws Exception {
    final Configuration first = SharedFixtures.configureConfiguration();
    final Configuration second = SharedFixtures
        .configureConfiguration(new Table(new Schema(new Catalog("testb"), "testz"), "testing"), 1,
            2);
    final String sql = "Select * from tests";
    final QueryRecord one = new QueryRecord(sql, first);
    final QueryRecord two = new QueryRecord(sql, second);

    assertThat(one, not(equalTo(two)));
  }

  @Test public void testQueryRecord_RetrievalFromMap_SameSQL_DiffConfig() throws Exception {
    final Configuration first = SharedFixtures.configureConfiguration();
    final Configuration second = SharedFixtures
        .configureConfiguration(new Table(new Schema(new Catalog("testb"), "testz"), "testing"), 1,
            2);
    final String sql = "Select * from tests";
    final QueryRecord one = new QueryRecord(sql, first);
    final QueryRecord two = new QueryRecord(sql, second);

    final Map<QueryRecord, String> dumbMap = Maps.newHashMap();
    dumbMap.put(one, "lalalalala");
    dumbMap.put(two, "lololololo");

    assertThat("lalalalala", equalTo(dumbMap.get(one)));
    assertThat("lololololo", equalTo(dumbMap.get(two)));
  }

  @Test public void testQueryRecord_RetrievalFromMap_DiffSQL_SameConfig() throws Exception {
    final Configuration first  = SharedFixtures.configureConfiguration();
    final Configuration second = SharedFixtures.configureConfiguration();
    final String sql  = "Select * from tests";
    final String sql2 = "Select idx_1 from tests";
    final QueryRecord one = new QueryRecord(sql, first);
    final QueryRecord two = new QueryRecord(sql2, second);

    final Map<QueryRecord, String> dumbMap = Maps.newHashMap();
    dumbMap.put(one, "lalalalala");
    dumbMap.put(two, "lololololo");

    assertThat("lalalalala", equalTo(dumbMap.get(one)));
    assertThat("lololololo", equalTo(dumbMap.get(two)));
  }

  @Test public void testQueryRecord_RetrievalFromMap_SameAll() throws Exception {
    final Configuration first  = SharedFixtures.configureConfiguration();
    final Configuration second = SharedFixtures.configureConfiguration();
    final String sql  = "Select * from tests";
    final QueryRecord one = new QueryRecord(sql, first);
    final QueryRecord two = new QueryRecord(sql, second);

    final Map<QueryRecord, String> dumbMap = Maps.newHashMap();
    dumbMap.put(one, "lalalalala");
    dumbMap.put(two, "lololololo");

    assertThat("lololololo", equalTo(dumbMap.get(two)));
    assertThat(dumbMap.size(), is(1));
  }
}
