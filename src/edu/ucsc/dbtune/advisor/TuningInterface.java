package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DBIndexSet;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.util.DBUtilities;

import java.sql.SQLException;

/**
 * Index Tuning Service.
 */
public class TuningInterface  {
    private final DatabaseConnection connection;
    private final Scheduler       taskScheduler;

    /**
     * construct an index tuning service.
     *
     * @param connection
     *      a live {@link DatabaseConnection connection}.
     */
    public TuningInterface(
            DatabaseConnection connection,
            int profilingQueueCapacity,
            int selectionQueueCapacity,
            int completionQueueCapacity,
            int maxNumStates,
            int maxHotSetSize,
            int partitionIterations,
            int indexStatisticsWindow)
    {
        this(
            connection,
            new TaskScheduler(
                connection,
                profilingQueueCapacity,
                selectionQueueCapacity,
                completionQueueCapacity,
                maxNumStates,
                maxHotSetSize,
                partitionIterations,
                indexStatisticsWindow));
    }

    /**
     * construct an index tuning service.
     *
     * @param connection
     *      a live {@link DatabaseConnection connection}.
     * @param taskScheduler
     *      a {@link Scheduler task scheduler}
     */
    TuningInterface(DatabaseConnection connection, Scheduler taskScheduler){
        this.connection    = connection;
        this.taskScheduler = taskScheduler;
    }

    /**
     * add a cold candidate index.
     * @param index
     *      index to be added.
     * @return
     *      a {@link Snapshot} of candidate indexes.
     */
    public Snapshot addColdCandidate(DBIndex index) {
        return taskScheduler.addColdCandidate(index);
    }

    /**
     * analyze some sql statement.
     * @param sql
     *      sql statement.
     * @return
     *      an {@link AnalyzedQuery} object.
     * @throws SQLException
     *      unable to analyze sql due to the stated reasons.
     */
    public AnalyzedQuery analyzeSQL(String sql) throws SQLException {
        sql = DBUtilities.trimSqlStatement(sql);
        return taskScheduler.analyzeQuery(sql);
    }

    /**
     * create a new index.
     * @param index
     *      index to be created.
     * @return
     *      transition cost due to the creation of the index.
     */
    public double createIndex(DBIndex index) {
        taskScheduler.positiveVote(index);
        return taskScheduler.create(index);
    }

    /**
     * drop an existing index.
     * @param index
     *      index to be dropped.
     * @return
     *      cost due to the dropping of the index.
     */
    public double dropIndex(DBIndex index) {
        taskScheduler.negativeVote(index);
        return taskScheduler.drop(index);
    }

    /**
     * execute profiled query.
     * @param qinfo
     *      a {@link ProfiledQuery} object.
     * @return
     *      the transition cost of this profiled query.
     * @throws SQLException
     *      unable to execute profiled query.
     */
    public double executeProfiledQuery(ProfiledQuery qinfo) throws SQLException {
        return taskScheduler.executeProfiledQuery(qinfo);
    }

    /**
     * @return
     *      a live {@link DatabaseConnection connection}.
     */
    DatabaseConnection getDatabaseConnection(){
        return connection;
    }

    /**
     * @return
     *      a list of recommended index.
     */
    public DBIndexSet getRecommendation() {
        DBIndexSet indexSet = new DBIndexSet();
        for (DBIndex index : taskScheduler.getRecommendation()) {
            indexSet.add(index);
        }
        return indexSet;
    }

    /**
     * give a negative vote to an index.
     * @param index
     *      index of interest.
     */
    public void negativeVote(DBIndex index) {
        taskScheduler.negativeVote(index);
    }

    /**
     * profile some sql statement.
     * @param sql
     *      sql statement.
     * @return
     *      an {@link ProfiledQuery} object.
     * @throws SQLException
     *      unable to profile sql due to the stated reasons.
     */
    public ProfiledQuery profileSQL(String sql) throws SQLException {
        sql = DBUtilities.trimSqlStatement(sql);
        return taskScheduler.profileQuery(sql);
    }

    /**
     * gives a positive vote to an index.
     * @param index
     *      index of interest.
     */
    public void positiveVote(DBIndex index) {
        taskScheduler.positiveVote(index);
    }    
    
}
