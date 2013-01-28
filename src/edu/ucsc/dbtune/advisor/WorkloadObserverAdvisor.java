package edu.ucsc.dbtune.advisor;

import java.sql.SQLException;

import com.google.common.eventbus.Subscribe;

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
public abstract class WorkloadObserverAdvisor extends AbstractAdvisor implements ObservableAdvisor
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

            String eventId = this.hashCode() + "_" + workload.hashCode();

            EventBusFactory.getEventBusInstance().post(eventId);
        }

        // ignore it otherwise since the statement is not of interest to this advisor
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
