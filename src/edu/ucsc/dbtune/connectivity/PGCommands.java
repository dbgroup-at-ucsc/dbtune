/*
 * ****************************************************************************
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
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */

package edu.ucsc.dbtune.connectivity;

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.PGIndex;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.spi.core.Function;
import edu.ucsc.dbtune.spi.core.Parameter;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Strings;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static edu.ucsc.dbtune.util.Instances.newList;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class PGCommands {
    private PGCommands(){}

    /**
     * returns a command that will recommend indexes.
     * @return
     *      a {@code Command<Void>} object.
     */
    public static Function<List<PGIndex>, SQLException> recommendIndexes(){
        return RecommendIndexes.INSTANCE;
    }

    /**
     * returns the version of the PostgreSQL instance that the given {@code connection} is 
     * communicating to.
     *
     * @param connection
     *     connection object from which the version will be retrieved from
     * @return
     *     a string containing the version number, e.g. "9.0.4"; "0.0.0" if not known
     * @throws SQLException
     *     if the underlying system is old enough such that it doesn't implement the {@code 
     *     version()} function; if another SQL error occurs while retrieving the system version.
     */
    public static String getVersion(Connection connection) throws SQLException {
        Statement st;
        ResultSet rs;
        String    version;

        st = connection.createStatement();
        rs = st.executeQuery("SELECT version()");

        version = "0.0.0";

        while(rs.next()) {
            version = rs.getString("version");
            version = version.substring(11,version.indexOf(" on "));
        }

        rs.close();
        st.close();

        return version;
    }

    // enum singleton pattern
    private enum RecommendIndexes implements Function<List<PGIndex>, SQLException> {
        INSTANCE;

        private Statement statement;
        @Override
        public List<PGIndex> apply(Parameter input) throws SQLException {
            final Connection         connection     = input.getParameterValue(DatabaseConnection.class).getJdbcConnection();
            final SQLStatement       sql            = input.getParameterValue(SQLStatement.class);
            final List<PGIndex>      candidateSet   = newList();

            if(statement == null){
                statement = connection.createStatement();
            }

            final ResultSet rs = statement.executeQuery("RECOMMEND INDEXES " + sql.getSQL());
            int id = 0;
            try {
                while(rs.next()){
                    touchIndexSchema(candidateSet, rs, id);
                    ++id;
                }
            } finally {
                rs.close();
                connection.commit();
            }
            return candidateSet;
        }

        private static void touchIndexSchema(List<PGIndex> candidateSet, ResultSet rs, int id) throws SQLException {
            final int       reloid = Integer.valueOf(rs.getString("reloid"));
            final boolean   isSync = rs.getString("sync").charAt(0) == 'Y';
            final List<Column> columns = newList();
            final String columnsString = rs.getString("atts");
            if(columnsString.length() > 0){
                for (String attnum  : columnsString.split(" ")){
                    columns.add(new Column(Integer.valueOf(attnum)));
                }
            }

            // descending
            final List<Boolean> isDescending        = newList();
            final String        descendingString    = rs.getString("desc");
            if(descendingString.length() > 0){
                for (String desc : rs.getString("desc").split(" ")){
                    isDescending.add(desc.charAt(0) == 'Y');
                }
            }

            final double        creationCost    = Double.valueOf(rs.getString("create_cost"));
            final double        megabytes       = Double.valueOf(rs.getString("megabytes"));

            final String indexName      = "sat_index_" + id;
            final String creationText   = updateCreationText(rs, isSync, indexName);

            try {
                candidateSet.add(
                    new PGIndex(
                        reloid, isSync, columns, isDescending, id,
                        megabytes, creationCost, creationText) );
            } catch(Exception ex) {
                throw new SQLException(ex);
            }
        }

        private static String updateCreationText(ResultSet rs, boolean sync, String indexName) throws SQLException {
            String creationText = rs.getString("create_text");
            if (sync){
                creationText = creationText.replace(
                        "CREATE SYNCHRONIZED INDEX ?",
                        "CREATE SYNCHRONIZED INDEX " + indexName
                );
            } else {
                creationText = creationText.replace(
                        "CREATE INDEX ?",
                        "CREATE INDEX " + indexName
                );
            }
            return creationText;
        }

        @Override
        public String toString() {
            return "";
        }
    }

    /**
     * Returns a command that will explain indexes's cost.
     * @param usedSet
     *      used index set.
     * @return
     *      a new {@code Command<Double>} object.
     */
    private static Function<Double, SQLException> explainCost(final IndexBitSet usedSet){
        return new Function<Double, SQLException>(){
            @Override
            public Double apply(Parameter input) throws SQLException {
                final ResultSet  resultSet  = input.getParameterValue(ResultSet.class);
                if(resultSet != null) {
                    try{

                        final double qCost =  Double.parseDouble(resultSet.getString("qcost"));
                        Console.streaming().info("PGCommands#explainCost(IndexBitSet) returned a workload cost of " + qCost);
                        usedSet.clear();
                        // todo(Huascar) I am getting actual costs, but no indexes? this is weird.
                        final String indexesString  = resultSet.getString("indexes");
                        if (!Strings.isEmpty(indexesString)){
                            for (String idString : resultSet.getString("indexes").split(" ")){
                                usedSet.set(Integer.parseInt(idString));
                            }
                        }

                        return qCost;
                        } finally {
                            resultSet.close();
                    }
                } else {
                    throw new SQLException("cannot get whatif cost: result set is not open");
                }
            }


        };
    }
}
