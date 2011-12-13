package edu.ucsc.dbtune.workload;

import java.io.Reader;
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.Iterable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
     * workloadStream} object. It's assumed that there's one statement per line and only one-line 
     * comments (line beginning with string {@code "--"}).
     *
     * @param workloadStream
     *     stream that provides the set of SQL statements. One statement per line is assumed; 
     *     single-line comments only.
     * @throws IOException
     *     if an error occurs while retrieving information from the given reader
     * @throws SQLException
     *     if a statement can't get a category assigned to it
     */
    public Workload(Reader workloadStream) throws IOException,SQLException
    {
        BufferedReader reader;
        String         line;
        String         lineLow;

        sqls   = new ArrayList<SQLStatement>();
        reader = new BufferedReader(workloadStream);

        while ((line = reader.readLine()) != null) {

            line    = line.trim();
            lineLow = line.toLowerCase();

            if (lineLow.startsWith("--")) {
                continue;
            }

            if (line.endsWith(";")) {
                sqls.add(new SQLStatement(line.substring(0, line.length()-1)));
            } else {
                sqls.add(new SQLStatement(line));
            }
        }
    }

    /**
     * Returns the statement at the given position (zero-indexing).
     *
     * @param i
     *     index of the SQL statement retrieved
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

    // SQLStatement get(String sql) // return the statement corresponding to the given sql
    //                              // string. This would require string matching stuff, or
    //                              // even having to do some query processing (parse, rewrite
    //                              // views, query flattening, etc) in order to compare queries
}
