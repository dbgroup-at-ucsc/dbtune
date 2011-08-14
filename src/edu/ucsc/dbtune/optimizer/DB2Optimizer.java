/* ************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied  *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.connectivity.DB2Commands;
import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.SQLCategory;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Strings;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.ArrayList;

import static edu.ucsc.dbtune.connectivity.DB2Commands.*;
import static edu.ucsc.dbtune.spi.core.Functions.submit;
import static edu.ucsc.dbtune.spi.core.Functions.submitAll;
import static edu.ucsc.dbtune.spi.core.Functions.supplyValue;
/**
 * Not implemented yet
 */
public class DB2Optimizer extends Optimizer
{
    private final DatabaseConnection connection;

    private final static Console SCREEN = Console.streaming();

    public DB2Optimizer(DatabaseConnection connection){
        this.connection = connection;
    }

    @Override
    public PreparedSQLStatement explain(String sql) throws SQLException
    {
        whatIfCount++;
        submitAll(
                submit(clearAdviseIndex(), connection),
                submit(loadAdviseIndex(), new ArrayList<Index>(), false, connection) );
        int explainCount; // should be equal to 1
        double totalCost;

        // silently supply a command and an input parameter so that it could
        // get executed in the background (since no return to the caller is needed)
        submitAll(
                // clear explain tables that we will end up reading
                submit(clearExplainObject(), connection),
                submit(clearExplainStatement(), connection),
                // enable indexes and set explain mode
                submit(enableAdviseIndexRows(), connection, new IndexBitSet()),
                submit(explainModeEvaluateIndexes(), connection)
        );

        // evaluate the query
        try {
            connection.getJdbcConnection().createStatement().execute(sql);
        } catch (SQLException e) {
            Console.streaming().dot();
        }

        // reset explain mode (indexes are left enabled...)
        submit(explainModeNo(), connection);

        // post-process the explain tables
        // first get workload cost
        DB2Commands.CostLevel costLevel = supplyValue(fetchExplainStatementTotals(), connection);

        try {
            explainCount = costLevel.getCount();
            totalCost    = costLevel.getTotalCost();
        } catch (Throwable e) {
            throw new SQLException(e);
        }

        Checks.checkSQLRelatedState(
                explainCount == 1,
                "Error: Unexpected number of statements: "
                        + explainCount
                        + " (expected 1)"
        );

        submit(fetchExplainObjectCandidates(), connection, new IndexBitSet());
        SQLCategory cat = getStatementType(connection.getJdbcConnection());
        connection.commit();

        return new PreparedSQLStatement(sql, cat, totalCost, new ArrayList<Index>());
    }

    @Override
    public PreparedSQLStatement explain(String sql, Iterable<? extends Index> indexes) throws SQLException {
        Checks.checkSQLRelatedState(null != connection && !connection.isClosed(), "Connection is closed.");
        Checks.checkArgument(!Strings.isEmpty(sql), "Empty SQL statement");

        SQLCategory category     = null;
        double      updateCost   = 0.0;
        int         count        = 0;

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

            connection.getJdbcConnection().createStatement().execute(sql);
            submit(explainModeNo(), connection);
            category = supplyValue(fetchExplainStatementType(), connection);
            if(SQLCategory.DML.isSame(category)){
                supplyValue(fetchExplainObjectUpdatedTable(), connection);
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

        for(Index idx : indexes) {
        	idx.getId();
            count++;
        }

        double[] updateCosts  = new double[count];
        Arrays.fill(updateCosts, updateCost);

        return new PreparedSQLStatement(sql, category, -1.0, updateCosts, indexes);
    }

    private static void error(String message, Throwable cause){
        SCREEN.error(message, cause);
    }

    /*
     *
    protected double estimateCost(WhatIfOptimizationBuilder builder) throws SQLException {
        whatIfCount++;
        if (whatIfCount % 80 == 0) {
            fixAnyExistingCandidates();
            System.err.println();
        }

        final WhatIfOptimizationBuilderImpl whatIfImpl  = Objects.cast(builder, WhatIfOptimizationBuilderImpl.class);

        if(!whatIfImpl.withProfiledIndex()){
           return estimateCostWithoutProfiledIndex(whatIfImpl, connection);
        }  else {
           return estimateCostBasedOnProfiledIndex(whatIfImpl, connection);
        }

    }
    */

    @Override
    public String toString() {
        return new ToStringBuilder<DB2Optimizer>(this)
               .toString();
    }

    private SQLCategory getStatementType(Connection connection) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(
                "SELECT TRIM(STATEMENT_TYPE) AS TYPE "
                + "FROM explain_statement "
                + "WHERE explain_level = 'P'"
                );

        ResultSet rs = ps.executeQuery();
        try {
            if(!rs.next()){
                throw new SQLException("could not derive stmt type: no rows");
            }

            String category = rs.getString(1);
            return SQLCategory.from(category);
        } finally {
            rs.close();
            connection.commit();
        }
    }
}
