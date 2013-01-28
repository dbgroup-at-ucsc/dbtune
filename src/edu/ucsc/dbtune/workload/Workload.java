package edu.ucsc.dbtune.workload;

/**
 * Represents an workload.
 *
 * @author Ivo Jimenez
 */
public class Workload
{
    private String workloadName;

    /**
     * Creates a workload with the given name.
     *
     * @param name
     *      name of the workload
     */
    public Workload(String name)
    {
        this.workloadName = name;
    }

    /**
     * @return
     *      name of the workload that it's being read.
     */
    public String getWorkloadName()
    {
        return workloadName;
    }
}
