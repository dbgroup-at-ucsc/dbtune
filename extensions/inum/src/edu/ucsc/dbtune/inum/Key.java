package edu.ucsc.dbtune.inum;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.spi.Console;
import edu.ucsc.dbtune.util.Strings;
import edu.ucsc.dbtune.util.ToStringBuilder;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Inumspace key consisting of a SQL query and an index configuration.
 * Matches the optimal plan at the matching step.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class Key {
  private final SQLStatement  sql;
  private final Configuration configuration;
  public Key(String sql, Configuration configuration) {
    this(createStatement(sql), configuration);
  }

  public Key(SQLStatement sql, Configuration configuration){
    this.sql           = sql;
    this.configuration = configuration;
  }

  @Override public int hashCode() {
    return Objects.hashCode(sql, configuration);//(sql + configuration.toString()).hashCode();
  }

  @Override public boolean equals(Object o) {
    if(!(o instanceof Key)) return false;
    final Key other = (Key)o;
    return isCoveredBy(other.sql, other.configuration);
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

    @Override
    public String toString() {
        return new ToStringBuilder<Key>(Key.class)
            .add("sql", sql)
            .add("configuration", configuration)
            .toString();
    }
}
