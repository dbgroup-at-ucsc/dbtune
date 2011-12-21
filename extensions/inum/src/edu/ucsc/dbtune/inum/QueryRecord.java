package edu.ucsc.dbtune.inum;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.spi.Console;
import edu.ucsc.dbtune.util.ConfigurationUtils;
import edu.ucsc.dbtune.util.Strings;
import edu.ucsc.dbtune.util.ToStringBuilder;
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
  private final Configuration         configuration;
  private final Set<String>           indexedTableNames; // these map to the tables part of the interesting orders in a SQL query
  public QueryRecord(String sql, Configuration configuration) {
    this(createStatement(sql), configuration);
  }

  public QueryRecord(SQLStatement sql, Configuration configuration){
    this.sql            = sql;
    this.configuration  = configuration;
    this.indexedTableNames = ConfigurationUtils.findUsedTables(this.configuration);
  }

  @Override public int hashCode() {
    return Objects.hashCode(sql, configuration);//(sql + configuration.toString()).hashCode();
  }

  @Override public boolean equals(Object o) {
    if(!(o instanceof QueryRecord)) return false;
    final QueryRecord other = (QueryRecord)o;
    return isCoveredBy(other.sql, other.configuration);
  }

  public Set<String> getUsedTablesNames(){
    return ImmutableSet.copyOf(indexedTableNames);
  }

  private static SQLStatement createStatement(String sql){
    Preconditions.checkArgument(!Strings.isEmpty(sql), "SQL query is empty.");
    try {
      return new SQLStatement(sql);
    } catch (Exception e){
      Console.streaming().error("Unable to create SQL Statement", e);
      throw new RuntimeException(e);
    }

  private boolean isCoveredBy(SQLStatement sql, Configuration other){
    final boolean equalSQL    = Strings.same(this.sql.getSQL(), sql.getSQL());
    final boolean intersected = intersects(configuration, other);
    final boolean sameConfig  = configuration.equals(other);

    @Override
    public int hashCode()
    {
        return Objects.hashCode(sql, configuration);//(sql + configuration.toString()).hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof Key)) return false;
        final Key other = (Key)o;
        return isMatch(other.sql, other.configuration);
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
    //  'first' is a subset of 'second' iff every member of 'first' is a member of 'second'
    final boolean isEveryMemberInSecond = c.size() == first.size();
    return !c.toList().isEmpty() && isEveryMemberInSecond;
  }

  @Override public String toString() {
    return new ToStringBuilder<QueryRecord>(QueryRecord.class)
        .add("sql", sql)
        .add("configuration", configuration)
        .toString();
  }
}
