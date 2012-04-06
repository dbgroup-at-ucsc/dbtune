package edu.ucsc.dbtune.bip.indexadvisor;

public interface ConstraintIndexAdvisor 
{
    /**
     * Set the maximum space budget imposed on each replica, which the total size of materialized 
     * indexes does not exceed.
     *  
     * @param B
     *      The space budget
     */
    public void setSpaceBudget(double B);
}
