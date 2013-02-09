package edu.ucsc.dbtune.advisor;

import java.sql.SQLException;

import com.google.common.eventbus.Subscribe;

import edu.ucsc.dbtune.metadata.Index;

import edu.ucsc.dbtune.util.EventBusFactory;

import edu.ucsc.dbtune.workload.ObservableWorkloadReader;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

/**
 * An advisor that watches for changes in a workload and proceeds accordingly after a new statement
 * is added. A workload-observer advisor can behave as a non-observer by assigning a workload (in 
 * the construction) that will never correspond to statements sent through the event bus.
 * <p>
 * This class also posts events to the event bus every time that it processes a new statement. Thus, 
 * other classes can subscribe to it. The event identifier is a {@link String}
 * of the form {@code "workload.getWorkloadName()" + "_" + "workload.hashCode()"}
 *
 * @author Ivo Jimenez
 */
public abstract class WorkloadObserverAdvisor // a.k.a. ObservableWorkloadObserverAdvisor
    extends AbstractAdvisor
    implements ObservableAdvisor, VoteableAdvisor
{
    private Workload workload;

    /**
     * Constructs a workload-observer advisor that will be listening for statements that correspond 
     * to the given workload. If desired, an instance can behave as a non-observer by assigning a 
     * workload that will never correspond to statements sent through the event bus.
     *
     * @param workload
     *      the workload that the advisor is observing.
     */
    protected WorkloadObserverAdvisor(Workload workload)
    {
        this.workload = workload;

        EventBusFactory.getEventBusInstance().register(this);
    }

    /**
     * Constructs a workload-observer advisor that will be listening for statements that correspond 
     * to the given workload. If desired, an instance can behave as a non-observer by assigning a 
     * workload that will never correspond to statements sent through the event bus.
     *
     * @param workloadReader
     *      the workload that the advisor is observing.
     */
    protected WorkloadObserverAdvisor(ObservableWorkloadReader workloadReader)
    {
        this(workloadReader.getWorkload());
    }

    /**
     * The handler of new statements. This also posts to the event bus, the event being a string of 
     * the form {@code advisor.hashCode() + "_" + workload.hashCode()}
     *
     * @param statement
     *      new statement that is added to a workload observer
     * @throws SQLException
     *      if the {@link #process} method throws it
     */
    @Subscribe
    public void newStatementAdded(SQLStatement statement) throws SQLException
    {
        // check that the statement being added corresponds to the workload being observed
        if (statement.getWorkload().equals(workload)) {

            processNewStatement(statement);

            post();
        }

        // ignore it otherwise since the statement is not of interest to this advisor
    }

    /**
     * {@inheritDoc}
     */
    public void voteUp(Index index)
        throws SQLException
    {
        vote(index, true);

        post();
    }

    /**
     * {@inheritDoc}
     */
    public void voteDown(Index index)
        throws SQLException
    {
        vote(index, false);

        post();
    }

    /**
     * Gives a negative vote for the given index.
     *
     * @param index
     *      index being voted
     * @param up
     *      whether to vote the index up or down
     * @throws SQLException
     *      if the index can't be voted
     */
    public abstract void vote(Index index, boolean up)
        throws SQLException;

    /**
     * Posts to the event bus to indicate that the state of the advisor has change.
     */
    public void post()
    {
        String eventId = this.hashCode() + "_" + workload.hashCode();

        EventBusFactory.getEventBusInstance().post(eventId);
    }

    /**
     * @return
     *      the workload being observed
     */
    public Workload getWorkload()
    {
        return workload;
    }
}
