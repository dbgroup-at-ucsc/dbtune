package edu.ucsc.dbtune.workload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.util.EventBusFactory;

/**
 * Reader of SQLStatement objects constructed from a plain text file. The file is read once and 
 * never updates its internal state. This means that observers will only get notified of new 
 * statements when the class is instantiated and will never get notified after that.
 *
 * @author Ivo Jimenez
 */
public class FileWorkloadReader extends ObservableWorkloadReader
{
    private boolean hasAlreadyPublishedAll;
    private List<SQLStatement> sqlsHolder;

    /**
     * Creates a workload containing the set of SQL statements provided by the {@code
     * workloadStream} object. It's assumed that statements are delimited by <strong> ONLY ONE
     * </strong> {@code ';'} semi-colon character (see examples below) and only one-line comments (
     * a line beginning with string {@code "--"}) are contained in the SQL script, i.e. multi-line
     * comments aren't supported.
     * <p>
     * The constructor can detect empty statements as long as there is one per line, i.e. the
     * following produces a Workload of {@link #size} 1:
     * <pre>
     * {@code .
     * "SELECT * FROM bla ; \n" +
     * "; \n" +
     * "; \n" +
     * }
     * </pre>
     * while the following:
     * <pre>
     * {@code .
     * "SELECT * FROM bla ; ; ; ; \n" +
     * "; \n" +
     * "; \n" +
     * }
     * </pre>
     * produces a workload of size 1, but with the first query being {@code SELECT * FROM bla ; ;
     * ;}, which will produce an error if gets executed by a JDBC-compliant driver.
     *
     * @param name
     *     name assigned to the workload
     * @param workloadStream
     *     stream that provides the set of SQL statements. One statement per line is assumed;
     *     single-line comments only.
     * @param publishImmediately
     *      whether to push immediately or not
     * @throws IOException
     *     if an error occurs while retrieving information from the given reader
     * @throws SQLException
     *     if a statement can't get a category assigned to it
     */
    public FileWorkloadReader(String name, Reader workloadStream, boolean publishImmediately)
        throws IOException, SQLException
    {
        super(new Workload(name));

        BufferedReader reader;
        StringBuilder sb;
        String line;
        int stmtNumber = 1;

        sqls = new ArrayList<SQLStatement>();
        sqlsHolder = new ArrayList<SQLStatement>();
        reader = new BufferedReader(workloadStream);
        sb = new StringBuilder();

        while ((line = reader.readLine()) != null) {

            line = line.trim();

            if (line.startsWith("--"))
                continue;

            if (line.endsWith(";")) {
                sb.append(line.substring(0, line.length() - 1));

                final String sqlStr = sb.toString();

                if (!sqlStr.isEmpty()) {
                    SQLStatement sql = new SQLStatement(sqlStr, getWorkload(), stmtNumber++);

                    if (publishImmediately) {
                        // we'll publish the statements right here, since new
                        // statements won't non-event-based workload reader
                        EventBusFactory.getEventBusInstance().post(sql);
                        sqls.add(sql);
                    } else {
                        // some users will expect to have a regular delay
                        sqlsHolder.add(sql);
                    }
                }

                sb = new StringBuilder();
            } else {
                sb.append(line + "\n");
            }
        }

        if (publishImmediately) {
            // start a watcher with a ten-hour check interval
            // (we don't need to have the bus to check again, since
            // we don't check the file for changes)
            startWatcher(36000);
        } else {
            // do as usual
            startWatcher(5);
        }
    }

    /**
     * Construct a workload extracted from the given file.
     *
     * @param workloadFile
     *      a plain text file containing SQL statements
     * @param publishImmediately
     *      whether to push immediately or not
     * @throws IOException
     *     if an error occurs while retrieving information from the given reader
     * @throws SQLException
     *     if a statement can't get a category assigned to it
     */
    public FileWorkloadReader(String workloadFile, boolean publishImmediately) throws IOException, 
           SQLException
    {
        this((new File(workloadFile)).getName(), new FileReader(workloadFile), publishImmediately);
    }

    /**
     * Construct a workload extracted from the given file.
     *
     * @param workloadFile
     *      a plain text file containing SQL statements
     * @throws IOException
     *     if an error occurs while retrieving information from the given reader
     * @throws SQLException
     *     if a statement can't get a category assigned to it
     */
    public FileWorkloadReader(String workloadFile) throws IOException, SQLException
    {
        this((new File(workloadFile)).getName(), new FileReader(workloadFile), true);
    }

    /**
     * {@inheritDoc}
     */
    protected List<SQLStatement> hasNewStatement() throws SQLException
    {
        if (!hasAlreadyPublishedAll) {

            hasAlreadyPublishedAll = true;

            return sqlsHolder;
        }

        return new ArrayList<SQLStatement>();
    }
}
