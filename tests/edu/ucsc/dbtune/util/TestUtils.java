package edu.ucsc.dbtune.util;

import edu.ucsc.dbtune.DatabaseSystem;

import static edu.ucsc.dbtune.util.SQLScriptExecuter.execute;

/**
 * Utilities for testing.
 *
 * @author Ivo Jimenez
 */
public final class TestUtils
{
    /**
     * Utility class.
     */
    private TestUtils()
    {
    }

    /**
     * @param db
     *      the database
     * @throws Exception
     *      if an error occurs while loading the data
     */
    public static void loadWorkloads(DatabaseSystem db) throws Exception
    {
        for (String wlName : Environment.getInstance().getWorkloadFolders())
            if (!isSchemaForWorkloadCreated(wlName))
                execute(db.getConnection(), wlName + "/create.sql");
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
        return true;
    }
}
