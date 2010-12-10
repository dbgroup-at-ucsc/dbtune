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

package edu.ucsc.dbtune.core.metadata;

import edu.ucsc.dbtune.core.DatabaseColumn;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.SQLStatement.SQLCategory;
import edu.ucsc.dbtune.spi.core.Command;
import edu.ucsc.dbtune.spi.core.Commands;
import edu.ucsc.dbtune.spi.core.Parameter;
import edu.ucsc.dbtune.spi.core.Parameters;
import edu.ucsc.dbtune.util.BitSet;
import edu.ucsc.dbtune.util.Instances;
import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.util.PreConditions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static edu.ucsc.dbtune.core.metadata.PGReifiedTypes.ReifiedPGIndexList;
import static edu.ucsc.dbtune.util.PreConditions.checkSQLRelatedState;
import static edu.ucsc.dbtune.util.Instances.newList;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class PGCommands {
    private PGCommands(){}

    /**
     * returns a command that will give you a {@link PGExplainInfo}.
     * @return
     *      a {@code Command<PGExplainInfo>} object.
     */
    public static Command<PGExplainInfo, SQLException> explainIndexes(){
        final Command<Parameter, SQLException>       rs;
        final Command<PGExplainInfo, SQLException>   eidx;
        rs     = shareResultSetCommand();
        eidx   = explainInfo();
        return Commands.compose(rs, eidx);
    }

    /**
     * returns a command that will recommend indexes.
     * @return
     *      a {@code Command<Void>} object.
     */
    public static Command<List<PGIndex>, SQLException> recommendIndexes(){
        return RecommendIndexes.INSTANCE;
    }

    // enum singleton pattern
    private enum RecommendIndexes implements Command<List<PGIndex>, SQLException>{
        INSTANCE;

        private Statement statement;
        @Override
        public List<PGIndex> apply(Parameter input) throws SQLException {
            final Connection         connection     = input.getParameterValue(DatabaseConnection.class).getJdbcConnection();
            final String             sql            = input.getParameterValue(String.class);
            final List<PGIndex>      candidateSet   = newList();

            if(statement == null){
                statement = connection.createStatement();
            }

            final ResultSet rs = statement.executeQuery("RECOMMEND INDEXES " + sql);
            int id = 0;
            try {
                while(rs.next()){
                    ++id;
                    touchIndexSchema(candidateSet, rs, id);
                }
            } finally {
                rs.close();
                connection.commit();
            }
            return candidateSet;
        }

        private static void touchIndexSchema(List<PGIndex> candidateSet,
        ResultSet rs, int id
        ) throws SQLException {
            final int       reloid = Integer.valueOf(rs.getString("reloid"));
            final boolean   isSync = rs.getString("sync").charAt(0) == 'Y';
            final List<DatabaseColumn> columns = newList();
            final String columnsString = rs.getString("atts");
            if(columnsString.length() > 0){
                for (String attnum  : columnsString.split(" ")){
                    columns.add(new PGColumn(Integer.valueOf(attnum)));
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

            final PGIndexSchema schema          = new PGIndexSchema(reloid, isSync, columns, isDescending);
            final double        creationCost    = Double.valueOf(rs.getString("create_cost"));
            final double        megabytes       = Double.valueOf(rs.getString("megabytes"));

            final String indexName      = "sat_index_" + id;
            final String creationText   = updateCreationText(rs, isSync, indexName);

            candidateSet.add(
                    new PGIndex(schema, id, creationCost, megabytes, creationText)
            );
        }

        private static String updateCreationText(ResultSet rs,
        boolean sync, String indexName
        ) throws SQLException {
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
    public static Command<Double, SQLException> explainIndexesCost(BitSet usedSet){
        final Command<Parameter, SQLException>       rs;
        final Command<Double, SQLException>          ec;
        rs   = shareResultSetCommand();
        ec   = explainCost(usedSet);
        return Commands.compose(rs, ec);
    }


    /**
     * Returns a command that will explain indexes's cost.
     * @param usedSet
     *      used index set.
     * @return
     *      a new {@code Command<Double>} object.
     */
    private static Command<Double, SQLException> explainCost(final BitSet usedSet){
        return new Command<Double, SQLException>(){
            @Override
            public Double apply(Parameter input) throws SQLException {
                final ResultSet  resultSet  = input.getParameterValue(ResultSet.class);
                if(resultSet != null) {
                    try{
                        final Double qCost =  Double.valueOf(resultSet.getString("qcost"));
                        usedSet.clear();
                        final String indexesString  = resultSet.getString("indexes");
                        if (indexesString.length() == 0){
                            return null; // this avoids a NumberFormatException on empty string
                        }

                        for (String idString : resultSet.getString("indexes").split(" ")){
                            usedSet.set(Integer.parseInt(idString));
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

    /**
     * Returns a command that will explain indexes.
     * @return
     *      a new {@code Command<PGExplainInfo>} object.
     */
    private static Command<PGExplainInfo, SQLException> explainInfo(){
        return new Command<PGExplainInfo, SQLException>(){

            @Override
            public PGExplainInfo apply(Parameter input) throws SQLException {
                final ResultSet             resultSet   = input.getParameterValue(ResultSet.class);
                final Integer               cardinality = input.getParameterValue(Integer.class);
                final Double[]              maintCost   = input.getParameterValue(Double[].class);

                SQLCategory category    = null;
                try {
                    category = SQLCategory.from(resultSet.getString("category"));
                    // overhead = oh
                    final String    indexOverhead   = resultSet.getString("index_overhead");
                    final String[]  ohArray         = indexOverhead.split(" ");
                    verifyOverheadArray(cardinality, ohArray);


                    final List<String> indexesAsStrings = Arrays.asList(ohArray);
                    for(String eachIndex : indexesAsStrings){
                        final String[] splitVals = eachIndex.split("=");
                        PreConditions.checkAssertion(splitVals.length == 2, "We got an unexpected result in index_overhead.");
                        final int    id         = Integer.valueOf(splitVals[0]);
                        final double overhead   = Double.valueOf(splitVals[1]);
                        maintCost[id]           = overhead;
                    }

                } finally {
                    if(resultSet != null){
                        resultSet.close();
                    }
                }

                // todo(huascar) try to come up with a better way to handle this 
                final List<Double> allNonZeroCost = Instances.newList();
                for (final Double eachCost : maintCost) {
                    if (eachCost != null) {
                        allNonZeroCost.add(eachCost);
                    }
                }

                final double[] castCost = new double[allNonZeroCost.size()];
                for(int i = 0; i < allNonZeroCost.size(); i++){
                    castCost[i] = allNonZeroCost.get(i);
                }
                return new PGExplainInfo(
                        category,
                        castCost
                );
            }

        };
    }

    /**
     * makes a command that will share an initialized {@link java.sql.ResultSet} object.
     *
     * @return a command that will initialize a {@link java.sql.ResultSet} and share it with
     *      other commands that will perform a more specific operation.
     *      <strong>NOTE</strong> these other commands will have the responsibility
     *      of closing the {@link java.sql.ResultSet} and commiting to the db by calling
     *      {@link java.sql.Connection#commit()}.
     */
    private static Command<Parameter, SQLException> shareResultSetCommand(){
        return new Command<Parameter, SQLException>(){
            private Statement statement;
            @Override
            public Parameter apply(Parameter input) throws SQLException {
                final Connection            connection  = input.getParameterValue(DatabaseConnection.class).getJdbcConnection();
                final ReifiedPGIndexList    indexes     = input.getParameterValue(ReifiedPGIndexList.class);
                final BitSet                config      = input.getParameterValue(BitSet.class);
                final String                sql         = input.getParameterValue(String.class);
                final Integer               cardinality = input.getParameterValue(Integer.class);
                final Double[]              maintCost   = input.getParameterValue(Double[].class);

                if(statement == null) {
                    statement = connection.createStatement();
                }

                final String explainSql = "EXPLAIN INDEXES " + indexListString(indexes, config) + sql;
                final ResultSet rs = statement.executeQuery(explainSql);
                checkSQLRelatedState(rs.next(), "no row returned from EXPLAIN INDEXES");
                return Parameters.makeAnonymousParameter(
                        rs,
                        cardinality, 
                        maintCost
                );
            }

        };
    }


    private static String indexListString(Iterable<PGIndex> indexes, BitSet config) {
        final StringBuilder sb = new StringBuilder();
        sb.append("( ");
        for (PGIndex idx : indexes) {
            if (config.get(idx.internalId())) {
                sb.append(idx.internalId()).append("(");
                if (idx.getSchema().isSync()) {
                    sb.append("synchronized ");
                }

                final PGTable table = Objects.as(idx.getSchema().getBaseTable());
                sb.append(table.getOid());
                for (int i = 0; i < idx.columnCount(); i++) {
                    sb.append(idx.getSchema().getDescending().get(i) ? " desc" : " asc");
                    final PGIndexSchema schema = idx.getSchema();
                    final List<DatabaseColumn>   cols   = schema.getColumns();
                    final PGColumn each    = Objects.as(cols.get(i));
                    sb.append(" ").append(each.getAttnum());
                }
                sb.append(") ");
            }
        }
        sb.append(") ");
        return sb.toString();
    }

    private static void verifyOverheadArray(Integer cardinality, String[] ohArray) {
        // verify ohArray contents
        if(cardinality == 0){
            // we expect ohArray to have one elt that is the empty string
            // but don't complain if it's empty
            if(ohArray.length != 0){
                PreConditions.checkAssertion(
                        ohArray.length == 1, "Too many elements in ohArray."
                );
                PreConditions.checkAssertion(
                        ohArray[0].length() == 0, "There is an unexpected element in ohArray."
                );
            }
        } else {
            PreConditions.checkAssertion(cardinality == ohArray.length, "Wrong length of ohArray.");
        }
    }

}
