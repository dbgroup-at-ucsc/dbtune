package edu.ucsc.dbtune.bip.util;

import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.workload.Workload;

/**
 * This class contains all statement of the input workload that refers to the same schema
 * 
 * @author tqtrung@soe.ucsc.edu.sg
 *
 */
public class WorkloadPerSchema 
{
    private Schema schema;
    private Workload workload;
    
    public WorkloadPerSchema(Workload wl, Schema sch)
    {
        this.workload = wl;
        this.schema = sch;
    }
    
    /**
     * Methods to get schema and workload
     * 
     */
   
    public Schema getSchema()
    {
        return this.schema;
    }
    
    public Workload getWorkload()
    {
        return this.workload;
    }
    
}
