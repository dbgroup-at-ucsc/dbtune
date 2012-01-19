package edu.ucsc.dbtune.workload;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.metadata.Schema;

/**
 * Represents a workload.
 *
 * @author Ivo Jimenez
 */
public class Workload implements Iterable<SQLStatement>
{
    private List<SQLStatement> sqls;

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
     * @param workloadStream
     *     stream that provides the set of SQL statements. One statement per line is assumed; 
     *     single-line comments only.
     * @throws IOException
     *     if an error occurs while retrieving information from the given reader
     * @throws SQLException
     *     if a statement can't get a category assigned to it
     */
    public Workload(Reader workloadStream) throws IOException, SQLException
    {
        BufferedReader reader;
        StringBuilder sb;
        String line;

        sqls = new ArrayList<SQLStatement>();
        reader = new BufferedReader(workloadStream);
        sb = new StringBuilder();

        while ((line = reader.readLine()) != null) {

            line = line.trim();

            if (line.startsWith("--"))
                continue;

            if (line.endsWith(";")) {
                sb.append(line.substring(0, line.length() - 1));

                final String sql = sb.toString();

                if (!sql.isEmpty())
                    sqls.add(new SQLStatement(sb.toString()));

                sb = new StringBuilder();
            } else {
                sb.append(line + "\n");
            }
        }
    }

    /**
     * Returns the statement at the given position (zero-indexing).
     *
     * @param i
     *      index of the SQL statement retrieved
     * @return
     *      the statement at the given position
     * @throws IndexOutOfBoundsException
     *      if the index is out of range ({@code index < 0 || index >= size()})
     */
    public SQLStatement get(int i)
    {
        return sqls.get(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<SQLStatement> iterator()
    {
        return sqls.iterator();
    }

    /**
     * Returns the number of statements contained in the workload.
     *
     * @return
     *      size of the workload
     */
    public int size()
    {
        return sqls.size();
    }

    /**
     * Partitions the workload into subsets of statements (or subworkloads) where each corresponds 
     * to a different schema.
     *
     * @return
     *      a mapping containing a workload in each entry corresponding to a schema
     */
    public Map<Schema, Workload> getSchemaToWorkloadMapping()
    {
        // todo: in order to partition we need to parse and bind. Not doing it for now
        throw new RuntimeException("not yet");
    }

    // SQLStatement get(String sql) // return the statement corresponding to the given sql
    //                              // string. This would require string matching stuff, or
    //                              // even having to do some query processing (parse, rewrite
    //                              // views, query flattening, etc) in order to compare queries
}
