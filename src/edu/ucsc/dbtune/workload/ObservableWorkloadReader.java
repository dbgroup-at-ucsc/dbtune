package edu.ucsc.dbtune.workload;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.eventbus.EventBus;

/**
 * A reader that watches the source for newcomers. Each new statement is included in the iterable
 * collection of {@link SQLStatement} objects that the reader can iterate on. Also, this class 
 * allows for subscribers (via guava's {@link EventBus}) that get notified whenever new statements 
 * are added.
 * <p>
 * This class can be thought of as a mutable workload.
 * <p>
 * In order to make use of the class, the implementor has to:
 * <ol>
 * <li>
 *    After setting up, start the watchdog "service", which is just a thread that invokes the {@link 
 *    #hasnewStatement()} method periodically (default: 10 second).
 * </li>
 * <li>
 *    Implement the {@link #hasNewStatement()} method in order for new statements to get included in 
 *    the underlying iterable collection and for them to be published through the event bus.
 * </li>
 * </ol>
 * <p>
 * This class is not thread-safe.
 *
 * @author Ivo Jimenez
 */
public abstract class ObservableWorkloadReader extends AbstractWorkloadReader
{
    // TODO: separate the publishing service from the watching facility,
    //       since this class merges the two things in one

    // flag to identify whether the watcher has been started
    private boolean alreadyStarted;

    /**
     * Invokes {@link #hasNewStatement} every second.
     */
    protected void startWatcher()
    {
        startWatcher(10);
    }

    /**
     * Invokes {@link #hasNewStatement} every {@code n} seconds.
     *
     * @param n number of seconds to wait for between every check
     */
    protected void startWatcher(int n)
    {
        if (alreadyStarted)
            throw new RuntimeException("watcher already started");

        final List<SQLStatement> newStatements = new ArrayList<SQLStatement>();

        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        final Runnable newStatementChecker = new Runnable() {
            public void run()
            {
                newStatements.clear();
                try {
                    newStatements.addAll(hasNewStatement());
                    sqls.addAll(newStatements);
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }
        };

        scheduler.scheduleAtFixedRate(newStatementChecker, n, n, TimeUnit.SECONDS);

        alreadyStarted = true;
    }

    /**
     * A method that checks for new statements. If one or more statements are returned, they are 
     * passed to every observer of this workload (through the event bus).
     *
     * @return
     *      a list with new statements, or empty of no new statements are in the source
     * @throws SQLException
     *      if an error occurs while checking for new statements
     */
    protected abstract List<SQLStatement> hasNewStatement() throws SQLException;
}
