package edu.ucsc.dbtune.inum;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.spi.Console;
import edu.ucsc.dbtune.util.ConfigurationUtils;
import edu.ucsc.dbtune.util.Strings;
import edu.ucsc.dbtune.workload.SQLStatement;
import java.util.Set;

/**
 * Inumspace key consisting of a SQL query and an index configuration.
 * Matches the optimal plan at the matching step.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class QueryRecord {
  private final SQLStatement          sql;
  private final Set<Index>            configuration;
  private final Set<String>           indexedTableNames; // these map to the tables part of the interesting orders in a SQL query

  public QueryRecord(String sql, Set<Index> configuration) {
    this(createStatement(sql), configuration);
  }

  public QueryRecord(SQLStatement sql, Set<Index> configuration){
    this.sql                = sql;
    this.configuration      = configuration;
    this.indexedTableNames  = ConfigurationUtils.findUsedTables(this.configuration);
  }

  @Override public int hashCode() {
    return (sql + configuration.toString()).hashCode();
  }

  @Override public boolean equals(Object o) {
    if(!(o instanceof QueryRecord)) return false;
    final QueryRecord other = (QueryRecord)o;
    final boolean isSameSQL     = Strings.same(this.sql.getSQL(), other.sql.getSQL());
    final boolean isSameConfig  = Objects.equal(this.configuration, other.configuration);
    return isSameSQL && isSameConfig;
  }

  public String getSQL(){
    return this.sql.getSQL();
  }

  public Set<String> getUsedTablesNames(){
    return indexedTableNames;
  }

  public Set<Index> getConfiguration(){
    return configuration;
  }

  private static SQLStatement createStatement(String sql){
    Preconditions.checkArgument(!Strings.isEmpty(sql), "SQL query is empty.");
    try {
      return new SQLStatement(sql);
    } catch (Exception e){
      Console.streaming().error("Unable to create SQL Statement", e);
      throw new RuntimeException(e);
    }
  }

  @Override public String toString() {
    return Objects.toStringHelper(this)
        .add("sql", sql)
        .add("configuration", configuration)
        .toString();
  }
}
