package edu.ucsc.dbtune.deployAware.test;

import static com.google.common.collect.Sets.cartesianProduct;
import static edu.ucsc.dbtune.optimizer.plan.Operator.NLJ;
import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesPerTable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;

public class Db2PlanExtractor {
    public Set<SQLStatementPlan> plans = new HashSet<SQLStatementPlan>();

    public Db2PlanExtractor(SQLStatement statement, Optimizer delegate,
            Set<Index> indexes) throws SQLException {
        ibg(statement, delegate, indexes, plans);
    }

    public void ibg(SQLStatement statement, Optimizer delegate,
            Set<Index> indexes, Set<SQLStatementPlan> inumSpace)
            throws SQLException {
        if (indexes.isEmpty())
            return;

        Rt.p(indexes.size());
        ExplainedSQLStatement estmt = delegate.explain(statement, indexes);

        List<Set<Index>> intersectedIndexes = new ArrayList<Set<Index>>();
        Set<Index> notIntersectedIndexes = new HashSet<Index>();

        for (Set<Index> indexesForTable : getIndexesPerTable(
                estmt.getPlan().getIndexes()).values()) {
            if (indexesForTable.size() > 1)
                intersectedIndexes.add(indexesForTable);
            else
                notIntersectedIndexes.addAll(indexesForTable);
        }

        if (!intersectedIndexes.isEmpty()) {

            for (List<Index> atomic : cartesianProduct(intersectedIndexes)) {
                Set<Index> conf = new HashSet<Index>();

                conf.addAll(notIntersectedIndexes);
                conf.addAll(atomic);

                ibg(statement, delegate, conf, inumSpace);
            }
        } else {
            inumSpace.add(estmt.getPlan());

            for (Index usedIndex : estmt.getPlan().getIndexes()) {
                Set<Index> conf = new HashSet<Index>();

                conf.addAll(indexes);
                conf.remove(usedIndex);

                ibg(statement, delegate, conf, inumSpace);
            }
        }
    }

}
