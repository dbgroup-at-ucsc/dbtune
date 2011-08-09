package edu.ucsc.dbtune.core.optimizer;

import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.core.metadata.Index;
import edu.ucsc.dbtune.core.metadata.Table;
import edu.ucsc.dbtune.core.metadata.SQLCategory;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.*;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import static edu.ucsc.dbtune.connectivity.DB2Commands.*;
import static edu.ucsc.dbtune.spi.core.Functions.submit;
import static edu.ucsc.dbtune.spi.core.Functions.submitAll;
import static edu.ucsc.dbtune.spi.core.Functions.supplyValue;

/**
* A DB2-specific implementation of {@link WhatIfOptimizer} type.
* @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
*/
public class DB2WhatIfOptimizer extends AbstractWhatIfOptimizer {
    private final DatabaseConnection        connection;
    private final AtomicReference<String>   cachedSQL;

    private final static Console SCREEN = Console.streaming();

    public DB2WhatIfOptimizer(DatabaseConnection connection){
        super();
        this.connection = connection;
        cachedSQL       = Instances.newAtomicReference();
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
    protected IndexBitSet getCachedIndexBitSet() {
        return Instances.newBitSet();
    }

    @Override
    protected void updateCachedIndexBitSet(IndexBitSet newOne) {
        // do nothing
    }

    @Override
    public ExplainInfo explain(String sql, Iterable<? extends Index> indexes) throws SQLException {
        Checks.checkSQLRelatedState(null != connection && connection.isOpened(), "Connection is closed.");
        Checks.checkArgument(!Strings.isEmpty(sql), "Empty SQL statement");
        updateCachedSQL(sql);

        SQLCategory    category     = null;
        Table            updatedTable = null;
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
            if(SQLCategory.DML.isSame(category)){
                updatedTable = supplyValue(fetchExplainObjectUpdatedTable(), connection);
                updateCost   = supplyValue(fetchExplainOpUpdateCost(), connection);
            }
        } catch (RuntimeException e){
            connection.rollback();
        }

        try {
            connection.rollback();
        } catch (SQLException s){
            error("Could not rollback transaction", s);
            throw s;
        }

        return new DB2ExplainInfo(category, updatedTable, updateCost);
    }

    private static void error(String message, Throwable cause){
        SCREEN.error(message, cause);
    }

    @Override
    public String toString() {
        return new ToStringBuilder<DB2WhatIfOptimizer>(this)
               .add("Active", connection.isOpened())
               .add("CachedSQL", getCachedSQL())
               .toString();
    }
}
