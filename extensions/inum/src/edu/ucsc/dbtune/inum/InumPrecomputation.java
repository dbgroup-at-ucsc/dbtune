package edu.ucsc.dbtune.inum;

import com.google.caliper.internal.guava.collect.ImmutableList;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.inum.OptimalPlan.Subplan;
import edu.ucsc.dbtune.inum.SqlExecutionOptimalPlan.InternalSubplan;
import edu.ucsc.dbtune.inum.commons.Pair;
import edu.ucsc.dbtune.util.Strings;
import java.sql.Connection;
import static java.util.Arrays.asList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Default implementation of Inum's {@link Precomputation precomputation} step.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumPrecomputation implements Precomputation {
  private final DatabaseConnection          connection;
  private final AtomicReference<InumSpace>  inumSpace;
  private final Set<String>                 seenWorkloads;

  private static final int NOT_FOUND = -1;
  private static final int ROOT      = -1;

  private static List<Pair<String, String>> OPERATORS_MAPPINGS;
  static {
    @SuppressWarnings( {"unchecked"})
    final List<Pair<String, String>> pairs =  asList(  /* this throws a warning */
        Pair.of("Result", "RESULT"),
        Pair.of("Append", "APPEND"),
        Pair.of("BitmapAnd", "BITMAP_AND"),
        Pair.of("BitmapOr", "BITMAP_OR"),
        Pair.of("Nested Loop", "NLJOIN"),
        Pair.of("Merge Join", "MSJOIN"),
        Pair.of("Hash Join", "HSJOIN"),
        Pair.of("Hash", "HASH"),
        Pair.of("Seq Scan", "TBSCAN"),
        Pair.of("Bitmap Index Scan", "BP_IXSCAN"),
        Pair.of("Bitmap Heap Scan", "BP_HSCAN"),
        Pair.of("Index Scan", "IXSCAN"),
        Pair.of("Tid Scan", "TIDSCAN"),
        Pair.of("Subquery Scan", "SUBQUERY_SCAN"),
        Pair.of("Function Scan", "FUNCTION_SCAN"),
        Pair.of("Function Scan", "FUNCTION_SCAN"),
        Pair.of("Values Scan", "VALUES_SCAN"),
        Pair.of("Materialize", "MATERIALIZE"),
        Pair.of("Sort", "SORT"),
        Pair.of("Group", "GROUP"),
        Pair.of("Aggregate", "AGGREGATE"),
        Pair.of("GroupAggregate", "GP_AGGREGATE"),
        Pair.of("HashAggregate", "H_AGGREGATE"),
        Pair.of("Unique", "UNIQUE"),
        Pair.of("SetOp Intersect", "SETOP"),
        Pair.of("SetOp Intersect All", "SETOP"),
        Pair.of(" SetOp Except", "SETOP"),
        Pair.of("SetOp Except All", "SETOP"),
        Pair.of("LIMIT", "LIMIT")
    );

    OPERATORS_MAPPINGS = ImmutableList.copyOf(
        pairs
    );
  }


  public InumPrecomputation(DatabaseConnection connection){
    this.connection     = connection;
    this.inumSpace      = new AtomicReference<InumSpace>();
    this.seenWorkloads  = Sets.newHashSet();
  }


  @Override public InumSpace getInumSpace() {
    return Preconditions.checkNotNull(inumSpace.get());
  }

  @Override public Set<OptimalPlan> setup(String workload, Iterable<DBIndex> configuration) {
    if(inumSpace.get() == null) {
      inumSpace.set(new InmemoryInumSpace());
    }

    seenWorkloads.add(workload);
    final Set<OptimalPlan> optimalPlans = Sets.newHashSet();

    // todo(Huascar)
    // call optimizer given the workload and an input configuration
    //   get optimal plan as a String
    //   parse it and collect information needed to create a new instance of Optimal plan
    //   add all returned plans to optimalPlans
    //   save plans in InumSpace
    // return a reference to the set of optimal plans
    final String queryExecutionPlan = getQueryExecutionPlan(connection.getJdbcConnection(),
        workload, configuration);
    if(Strings.isEmpty(queryExecutionPlan)) return optimalPlans;

    optimalPlans.addAll(buildPlans(queryExecutionPlan));

    return getInumSpace().save(optimalPlans);
  }

  private void addIfHavenotSeenBefore(String query){
    Preconditions.checkArgument(!Strings.isEmpty(query));
    if(!seenWorkloads.contains(query)){
      seenWorkloads.add(query);
    }
  }

  private static String getQueryExecutionPlan(Connection connection, String query,
      Iterable<DBIndex> configuration){
    // example of a possible suggested plan
    return "Hash Join  (cost=174080.39..9364262539.50 rows=1 width=193)";   // we can have one or many query plans
  }

  // parsing plan suggested by optimizer
  private static Set<OptimalPlan> buildPlans(String queryExecutionPlan){
    // todo(Huascar) implement this.
    final Set<OptimalPlan> suggestedPlans = Sets.newHashSet();
    final OptimalPlan   optimalPlan = new SqlExecutionOptimalPlan();
    final List<String>  parsedlines = Lists.newArrayList();
    final List<Integer> parents     = Lists.newArrayList();
    final String        current     = ("->  " + queryExecutionPlan).replaceAll("\\r|\\n", "");

    final Matcher       matcher     = Pattern.compile("\\->").matcher(current);
    while(matcher.find()){
      int end          = matcher.end();
      int nextPosition = current.indexOf("->", end);
      int counter      = 0;
      String relevantText;
      if(nextPosition != NOT_FOUND){
        relevantText = current.substring(end + 2, nextPosition);
        for(int idx = (relevantText.length() - 1) ; idx > 0; idx--) {
          final boolean isWhiteSpace = Character.isWhitespace(relevantText.charAt(idx));
          if(isWhiteSpace) { counter++; }
          else             { break;     }
          parents.add(counter);
        }

      } else {
        relevantText = current.substring(end + 2);
      }

      if(!Strings.isEmpty(relevantText)){
        parsedlines.add(relevantText);
      }
    }

    parents.add(0, ROOT);

    // parse parents and actual plans
    for(int rowId = 0; rowId < parsedlines.size(); rowId++){
      final String each          = parsedlines.get(rowId);
      int posOpenParenthesis     = each.indexOf("(");
      int posCost                = each.indexOf("cost=");
      int posDoubleDot           = each.indexOf("..");
      int posSpaceAfterDoubleDot = each.indexOf(" ", posDoubleDot);
      int posRows                = each.indexOf("rows=");
      int posSpaceAfterRows      = each.indexOf(" ", posRows);

      final String operator           = findOperator(each.substring(0, posOpenParenthesis - 1));
      final String target             = findTarget(operator, each);
      final double costFirstRow       = Double.valueOf(each.substring(posCost + 5, posDoubleDot));
      final double costWholeOperation = Double.valueOf(each.substring(posDoubleDot + 2, posSpaceAfterDoubleDot));
      final long   rows               = Long.valueOf(each.substring(posRows + 5, posSpaceAfterRows));
      int   parent = ROOT;
      if(rowId == 0){
        parent = ROOT;
      } else {
        final int len = parents.get(rowId);
        for(int j = rowId; j >= 0; j--){
          if(parents.get(j) < len || parents.get(j) < 0) {
            parent = j;
            break;
          }
        }
      }

      final Subplan subplan = new InternalSubplan(
          rowId,
          parent,
          operator,
          target,
          costWholeOperation,
          costFirstRow,
          rows
      );

      optimalPlan.addSubplan(subplan);
    }
    suggestedPlans.add(optimalPlan);
    return suggestedPlans;
  }

  private static String findOperator(String name){
    for(Pair<String, String> each : OPERATORS_MAPPINGS){
      if(Strings.contains(name, each.getLeft())){
        return each.getRight();
      }
    }
    return "UNKNOWN TYPE in " + name;
  }

  private static String findTarget(String nodeName, String prim){
      String result = "";
      if (nodeName.equals("IXSCAN")) {//IndexScan: " using %s"
          int x = prim.indexOf(" using ");
          int y = prim.indexOf(" ", x + 7);
          result = prim.substring(x + 7, y);
      }

      if (nodeName.equals("TBSCAN") || nodeName.equals("BP_HSCAN") || nodeName.equals("TIDSCAN")) {
          int x = prim.indexOf(" on ");//FUNCTION_SCAN,VALUES_SCAN
          int y = prim.indexOf(" ", x + 4);
          result = prim.substring(x + 4, y);
      }


      if (nodeName.equals("BP_IXSCAN") || nodeName.equals("FUNCTION_SCAN") || nodeName.equals("VALUES_SCAN")) {
          int x = prim.indexOf(" on ");
          int y = prim.indexOf(" ", x + 4);
          result = prim.substring(x + 4, y);
      }

      if (nodeName.equals("SUBQUERY_SCAN")) {
          int x = prim.indexOf("Subquery Scan ");
          int y = prim.indexOf(" ", x + 14);
          result = prim.substring(x + 14, y);
      }

      return result;
  }


  @Override public boolean skip(String workload) {
    return seenWorkloads.contains(workload);
  }
}
