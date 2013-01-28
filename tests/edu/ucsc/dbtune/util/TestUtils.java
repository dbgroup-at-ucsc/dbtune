package edu.ucsc.dbtune.util;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.sql.Connection;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Iterables;

import edu.ucsc.dbtune.workload.FileWorkloadReader;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.WorkloadReader;

import static edu.ucsc.dbtune.util.SQLScriptExecuter.execute;

/**
 * Utilities for testing.
 *
 * @author Ivo Jimenez
 */
public final class TestUtils
{
    private static final Random RANDOM = new Random();

    /**
     * Utility class.
     */
    private TestUtils()
    {
    }

    /**
     * @param con
     *      connection to the database
     * @throws Exception
     *      if an error occurs while loading the data
     */
    public static void loadWorkloads(Connection con) throws Exception
    {
        for (String wlName : Environment.getInstance().getWorkloadFolders())
            if (!isSchemaForWorkloadCreated(wlName))
                execute(con, wlName + "/create.sql");
    }

    /**
     * Checks whether the data for a workload has been loaded.
     *
     * @param workloadName
     *      name of the workload for which the schemas it touches are checked for existence
     * @return
     *      {@code true} if the schema that the workload queries exits; {@code false} otherwise
     */
    private static boolean isSchemaForWorkloadCreated(String workloadName)
    {
        // TODO:
        // inside each workload.sql we have to put a header (in comments) that references the 
        // schemas that the statements contained in the file are querying, in this way we can check 
        // to see if the schema exists (by looking at the catalog)
        //
        // the above implies that we have to reorganize the way we organize workloads. Maybe 
        // something like:
        //
        //   * resources
        //      + db2
        //         - data
        //            * tpch
        //            * tpcds
        //            * movies
        //            * one_table
        //            * ...
        //         - workloads
        //            * mix
        //            * tpch
        //            * tpcds
        //            * tpcde
        //            * ...
        //      + mysql
        //         - data
        //            * tpch
        //            * tpcds
        //            * movies
        //            * one_table
        //            * ...
        //         - workloads
        //            * mix
        //            * tpch
        //            * tpcds
        //            * tpcde
        //            * ...
        //      + postgres
        //         - data
        //            * tpch
        //            * tpcds
        //            * movies
        //            * one_table
        //            * ...
        //         - workloads
        //            * mix
        //            * tpch
        //            * tpcds
        //            * tpcde
        //            * ...
        //
        return true;
    }

    /**
     * Returns a list of {@link Workload} objects containing all the workloads in the given fully 
     * qualified list of workload names.
     *
     * @param fullyQualifiedWorkloadFolderNames
     *      for each, an instance of {@link Workload} is created
     * @return
     *      list of workloads
     * @throws IOException
     *      if one of the files doesn't exist
     * @throws SQLException
     *      if {@link Workload} constructor throws an exception
     */
    public static List<List<SQLStatement>> workloads(List<String> fullyQualifiedWorkloadFolderNames)
        throws IOException, SQLException
    {
        List<List<SQLStatement>> workloads = new ArrayList<List<SQLStatement>>();

        for (String wlFolderName : fullyQualifiedWorkloadFolderNames) {
            if (new File(wlFolderName).isHidden())
                continue;
            workloads.add(workload(wlFolderName));
        }

        return workloads;
    }

    /**
     * Returns a {@link Workload} object containing the workload for the given fully qualified 
     * workload name.
     *
     * @param fullyQualifiedWorkloadFolderName
     *      an instance of {@link Workload} is created for it
     * @return
     *      a workload
     * @throws IOException
     *      if the files doesn't exist
     * @throws SQLException
     *      if {@link Workload} constructor throws an exception
     */
    public static List<SQLStatement> workload(String fullyQualifiedWorkloadFolderName)
        throws IOException, SQLException
    {
        WorkloadReader wr = new FileWorkloadReader(
                fullyQualifiedWorkloadFolderName,
                new FileReader(fullyQualifiedWorkloadFolderName + "/workload.sql"));

        List<SQLStatement> sqls = new ArrayList<SQLStatement>();

        for (SQLStatement sql : wr) {
            sqls.add(sql);
        }

        return sqls;
    }

    /**
     * returns a random subset from the given set. The size of the subset is between 0 and {@code 
     * maxSize}.
     *
     * @param set
     *      the set for which
     * @param maxSize
     *      maximum number of elements contained in the subset
     * @param <T>
     *      the type of object
     * @return
     *      a random subset
     */
    public static <T> Set<T> randomSubset(Set<T> set, int maxSize)
    {
        Set<T> subset = new HashSet<T>();

        int howManyToPick = RANDOM.nextInt(maxSize);

        for (int i = 0; i < howManyToPick; i++)
            subset.add(Iterables.get(set, RANDOM.nextInt(set.size() - 1)));

        return subset;
    }
}
