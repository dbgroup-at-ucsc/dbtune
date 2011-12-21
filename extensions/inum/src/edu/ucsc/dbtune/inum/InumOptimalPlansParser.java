package edu.ucsc.dbtune.inum;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.util.Environment;
import static edu.ucsc.dbtune.util.EnvironmentProperties.PG;
import edu.ucsc.dbtune.util.Pair;
import edu.ucsc.dbtune.util.Strings;
import static java.util.Arrays.asList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Default implementation of {@link OptimalPlansParser} interface.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumOptimalPlansParser implements OptimalPlansParser {
  private static final int NOT_FOUND = -1;
  private static final int ROOT = -1;

  private static List<Pair<String, String>> OPERATORS_MAPPINGS;

  static {
    @SuppressWarnings({"unchecked"})
    final List<Pair<String, String>> pairs = asList(  /* this throws a warning */
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

  // parsing plan suggested by optimizer
  private static Set<OptimalPlan> buildPlans(String queryExecutionPlan) {
    final Set<OptimalPlan> suggestedPlans = Sets.newHashSet();
    final OptimalPlan optimalPlan = isPG() ? new PostgresSqlExecutionPlan() : new DB2SqlExecutionPlan() ;
    final List<String> parsedlines = Lists.newArrayList();
    final List<Integer> parents = Lists.newArrayList();
    final String current = ("->  " + queryExecutionPlan).replaceAll("\\r|\\n", "");

    final Matcher matcher = Pattern.compile("\\->").matcher(current);
    while (matcher.find()) {
      int end = matcher.end();
      int nextPosition = current.indexOf("->", end);
      int counter = 0;
      String relevantText;
      if (nextPosition != NOT_FOUND) {
        relevantText = current.substring(end + 2, nextPosition);
        for (int idx = (relevantText.length() - 1); idx > 0; idx--) {
          final boolean isWhiteSpace = Character.isWhitespace(relevantText.charAt(idx));
          if (isWhiteSpace) { counter++; } else { break; }
          parents.add(counter);
        }
      } else {
        relevantText = current.substring(end + 2);
      }

      if (!Strings.isEmpty(relevantText)) {
        parsedlines.add(relevantText);
      }
    }

    parents.add(0, ROOT);

    // parse parents and actual plans
    for (int rowId = 0; rowId < parsedlines.size(); rowId++) {
      final String each = parsedlines.get(rowId);
      int posOpenParenthesis = each.indexOf("(");
      int posCost = each.indexOf("cost=");
      int posDoubleDot = each.indexOf("..");
      int posSpaceAfterDoubleDot = each.indexOf(" ", posDoubleDot);
      int posRows = each.indexOf("rows=");
      int posSpaceAfterRows = each.indexOf(" ", posRows);

      final String operator = findOperator(each.substring(0, posOpenParenthesis - 1));
      final String target = findTarget(operator, each);
      final double costFirstRow = Double.valueOf(each.substring(posCost + 5, posDoubleDot));
      final double costWholeOperation = Double
          .valueOf(each.substring(posDoubleDot + 2, posSpaceAfterDoubleDot));
      final long rows = Long.valueOf(each.substring(posRows + 5, posSpaceAfterRows));
      int parent = ROOT;
      if (rowId == 0) {
        parent = ROOT;
      } else {
        final int len = parents.get(rowId);
        for (int j = rowId; j >= 0; j--) {
          if (parents.get(j) < len || parents.get(j) < 0) {
            parent = j;
            break;
          }
        }
      }

      final PhysicalOperator subplan = new PhysicalOperatorImpl(
          rowId,
          parent,
          operator,
          target,
          costWholeOperation,
          costFirstRow,
          rows
      );

      optimalPlan.add(subplan);
    }
    suggestedPlans.add(optimalPlan);
    return suggestedPlans;
  }

  private static String findOperator(String name) {
    for (Pair<String, String> each : OPERATORS_MAPPINGS) {
      if (Strings.contains(name, each.getLeft())) {
        return each.getRight();
      }
    }
    return "UNKNOWN TYPE in " + name;
  }

  private static String findTarget(String nodeName, String prim) {
    String result = "";
    if ("IXSCAN".equals(nodeName)) {//IndexScan: " using %s"
      int x = prim.indexOf(" using ");
      int y = prim.indexOf(" ", x + 7);
      result = prim.substring(x + 7, y);
    }

    if ("TBSCAN".equals(nodeName) || "BP_HSCAN".equals(nodeName) || "TIDSCAN".equals(nodeName)) {
      int x = prim.indexOf(" on ");//FUNCTION_SCAN,VALUES_SCAN
      int y = prim.indexOf(" ", x + 4);
      result = prim.substring(x + 4, y);
    }

    if ("BP_IXSCAN".equals(nodeName) || "FUNCTION_SCAN".equals(nodeName) || "VALUES_SCAN"
        .equals(nodeName)) {
      int x = prim.indexOf(" on ");
      int y = prim.indexOf(" ", x + 4);
      result = prim.substring(x + 4, y);
    }

    if ("SUBQUERY_SCAN".equals(nodeName)) {
      int x = prim.indexOf("Subquery Scan ");
      int y = prim.indexOf(" ", x + 14);
      result = prim.substring(x + 14, y);
    }

    return result;
  }

  private static boolean isPG(){ /*this is the default vendor*/
    final Environment env = Environment.getInstance();
    final String vendor = env.getVendor();
    return Strings.isEmpty(vendor) || Strings.same(env.getVendor(), PG);
  }

  @Override public Set<OptimalPlan> parse(String returnedStatement) {
    return buildPlans(returnedStatement);
  }
}
