package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.core.metadata.DB2ExplainInfo;
import edu.ucsc.dbtune.core.metadata.DB2QualifiedName;
import edu.ucsc.dbtune.util.*;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import static edu.ucsc.dbtune.core.metadata.DB2Commands.*;
import static edu.ucsc.dbtune.spi.core.Functions.submit;
import static edu.ucsc.dbtune.spi.core.Functions.submitAll;
import static edu.ucsc.dbtune.spi.core.Functions.supplyValue;

/**
* A DB2-specific implementation of {@link WhatIfOptimizer} type.
* @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
*/
class DB2WhatIfOptimizer extends AbstractWhatIfOptimizer {
    private final DatabaseConnection        connection;
    private final AtomicReference<String>   cachedSQL;

    DB2WhatIfOptimizer(DatabaseConnection connection){
        super();
        this.connection = connection;
        cachedSQL       = Instances.newAtomicReference();
    }

    @Override
    public ExplainInfo explain(String sql) throws SQLException {
        return explain(sql, Instances.<DBIndex>newList());
    }

    @Override
    public ExplainInfo explain(Iterable<? extends DBIndex> indexes) throws SQLException {
        return explain(cachedSQL.get(), indexes);
    }

    @Override
    protected DatabaseConnection getConnection() {
        return connection;
    }

    @Override
    protected IndexBitSet getCachedIndexBitSet() {
        return Instances.newBitSet();
    }

    @Override
    protected void updateCachedIndexBitSet(IndexBitSet newOne) {
        // do nothing
    }

    @Override
    public ExplainInfo explain(String sql, Iterable<? extends DBIndex> indexes) throws SQLException {
        Checks.checkSQLRelatedState(null != connection && connection.isOpened(), "Connection is closed.");
        Checks.checkArgument(!Strings.isEmpty(sql), "Empty SQL statement");
        updateCachedSQL(sql);

        SQLStatement.SQLCategory    category     = null;
        DB2QualifiedName            updatedTable = null;
        double                      updateCost   = 0.0;

        try {
            // a batch supplying of commands with no returned value.
            submitAll(
                    // clear out the tables we'll be reading
                    submit(clearExplainObject(), connection),
                    submit(clearExplainStatement(), connection),
                    submit(clearExplainOperator(), connection),
                    // execute statment in explain mode = explain
                    submit(explainModeExplain(), connection)
            );

            connection.execute(sql);
            submit(explainModeNo(), connection);
            category = supplyValue(fetchExplainStatementType(), connection);
            if(SQLStatement.SQLCategory.DML.isSame(category)){
                updatedTable = supplyValue(fetchExplainObjectUpdatedTable(), connection);
                updateCost   = supplyValue(fetchExplainOpUpdateCost(), connection);
            }
        } catch (RuntimeException e){
            connection.rollback();
        }

        try {
            connection.rollback();
        } catch (SQLException s){
            Debug.logError("Could not rollback transaction", s);
            throw s;
        }

        return new DB2ExplainInfo(category, updatedTable, updateCost);
    }

    @Override
    public String toString() {
        return new ToStringBuilder<DB2WhatIfOptimizer>(this)
               .add("Active", connection.isOpened())
               .add("CachedSQL", getCachedSQL())
               .toString();
    }
}
