package edu.ucsc.dbtune.core.optimizer;

import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.connectivity.PGCommands;
import edu.ucsc.dbtune.core.metadata.Index;
import edu.ucsc.dbtune.core.metadata.PGIndex;
import edu.ucsc.dbtune.spi.core.Functions;
import edu.ucsc.dbtune.util.*;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.ArrayList;
import java.util.List;

/**
 * @author huascar.sanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class PostgresWhatIfOptimizer extends AbstractWhatIfOptimizer {
    private final DatabaseConnection        connection;
    private final AtomicReference<String>   cachedSQL;
    private final IndexBitSet               indexSet;

    public PostgresWhatIfOptimizer(DatabaseConnection connection){
        super();
        this.connection     = connection;
        indexSet            = Instances.newBitSet();
        cachedSQL           = Instances.newAtomicReference();
    }

    @Override
    public ExplainInfo explain(String sql) throws SQLException {
        return explain(sql, Instances.<Index>newList());
    }

    @Override
    public ExplainInfo explain(Iterable<? extends Index> indexes) throws SQLException {
        return explain(cachedSQL.get(), indexes);
    }

    @Override
    protected DatabaseConnection getConnection() {
        return connection;
    }

    @Override
    public ExplainInfo explain(String sql, Iterable<? extends Index> indexes) throws SQLException {
        Checks.checkSQLRelatedState(null != connection && connection.isOpened(), "Connection is closed.");
        Checks.checkArgument(!Strings.isEmpty(sql), "Empty SQL statement");
        updateCachedSQL(sql);
        final List<Index> indexSet = makeIndexList(indexes);
        if(getCachedIndexBitSet().isEmpty()) updateCachedIndexBitSet(Instances.newBitSet(indexes));
        final IndexBitSet   cachedIndexBitSet   = getCachedIndexBitSet();
        final Double[]      maintCost           = new Double[indexSet.size()];
        return Functions.supplyValue(
                PGCommands.explainIndexes(),             // postgres's explain index command
                connection,                              // live connection
                indexSet,                                // index set
                cachedIndexBitSet,                       // cached bit set
                sql,                                     // sql statement
                cachedIndexBitSet.cardinality(),         // cardinality
                maintCost                                // maintenance cost
        );
    }

	private List<Index> makeIndexList(Iterable<? extends Index> indexes) {
		List<Index> list = new ArrayList<Index>();
		for(Index idx : indexes) {
			list.add(idx);
		}
		return list;
	}

    @Override
    protected IndexBitSet getCachedIndexBitSet() {
        return indexSet;
    }

    @Override
    protected void updateCachedIndexBitSet(IndexBitSet newOne) {
        indexSet.set(newOne);
    }

    @Override
    public String toString() {
        return new ToStringBuilder<PostgresWhatIfOptimizer>(this)
               .add("Active", connection.isOpened())
               .add("CachedSQL", getCachedSQL())
               .add("CachedIndexSet", getCachedIndexBitSet())
               .toString();
    }
}
