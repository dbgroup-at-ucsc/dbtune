package edu.ucsc.dbtune.workload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Reader of SQLStatement objects constructed from a plain text file.
 *
 * @author Ivo Jimenez
 */
public class FileWorkloadReader extends AbstractWorkloadReader
{
    /**
     * Creates an emtpy workload.
     */
    public FileWorkloadReader()
    {
        sqls = new ArrayList<SQLStatement>();
    }

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
     * @throws IOException
     *     if an error occurs while retrieving information from the given reader
     * @throws SQLException
     *     if a statement can't get a category assigned to it
     */
    public FileWorkloadReader(String name, Reader workloadStream) throws IOException, SQLException
    {
        BufferedReader reader;
        StringBuilder sb;
        String line;
        int stmtNumber = 1;

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
                    sqls.add(new SQLStatement(name, sb.toString(), stmtNumber++));

                sb = new StringBuilder();
            } else {
                sb.append(line + "\n");
            }
        }

        this.name = name;
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
        this("temp", new FileReader(workloadFile));

        File f = new File(workloadFile);

        if (f.getName().split(".").length == 1)
            this.name = f.getName();
        else
            for (int i = 0; i < f.getName().split(".").length - 1; i++)
                this.name += f.getName().split(".")[i];
    }
}
