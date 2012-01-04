package edu.ucsc.dbtune.inum;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

import edu.ucsc.dbtune.SharedFixtures;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class QueryRecordTest {
  @Test public void testQueryRecord_Same() throws Exception {
    final Set<Index> first = SharedFixtures.configureConfiguration();
    final Set<Index> second = SharedFixtures.configureConfiguration();
    final String sql = "Select * from tests";
    final QueryRecord one = new QueryRecord(sql, first);
    final QueryRecord two = new QueryRecord(sql, second);

    assertThat(one, equalTo(two));
  }

  @Test public void testQueryRecord_Different() throws Exception {
    final Set<Index> first = SharedFixtures.configureConfiguration();
    final Set<Index> second = SharedFixtures
        .configureConfiguration(new Table(new Schema(new Catalog("testb"), "testz"), "testing"), 1,
            2);
    final String sql = "Select * from tests";
    final QueryRecord one = new QueryRecord(sql, first);
    final QueryRecord two = new QueryRecord(sql, second);

    assertThat(one, not(equalTo(two)));
  }

  @Test public void testQueryRecord_RetrievalFromMap_SameSQL_DiffConfig() throws Exception {
    final Set<Index> first = SharedFixtures.configureConfiguration();
    final Set<Index> second = SharedFixtures
        .configureConfiguration(new Table(new Schema(new Catalog("testb"), "testz"), "testing"), 1,
            2);
    final String sql = "Select * from tests";
    final QueryRecord one = new QueryRecord(sql, first);
    final QueryRecord two = new QueryRecord(sql, second);

    final Map<QueryRecord, String> dumbMap = new HashMap<QueryRecord, String>();
    dumbMap.put(one, "lalalalala");
    dumbMap.put(two, "lololololo");

    assertThat("lalalalala", equalTo(dumbMap.get(one)));
    assertThat("lololololo", equalTo(dumbMap.get(two)));
  }

  @Test public void testQueryRecord_RetrievalFromMap_DiffSQL_SameConfig() throws Exception {
    final Set<Index> first  = SharedFixtures.configureConfiguration();
    final Set<Index> second = SharedFixtures.configureConfiguration();
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
    final Set<Index> first  = SharedFixtures.configureConfiguration();
    final Set<Index> second = SharedFixtures.configureConfiguration();
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
