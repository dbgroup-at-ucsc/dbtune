package edu.ucsc.dbtune.bip.div;

/**
 * A standard set of parameters that a Divergent Index Tuning problem handles. 
 *
 */
public interface Divergent 
{
    /**
     * Set the number of replicas to deploy
     * 
     * @param n
     *      The number of replicas 
     */
    public void setNumberReplicas(int n);

    /**
     * Set the load-balancing factor. A query is supposed to sent to one of the top-m replicas
     * that give the best cost.
     * 
     * @param m
     *      The load balancing
     */
    public void setLoadBalanceFactor(int m);
    
    
    /**
     * Set the maximum space budget imposed on each replica, which the total size of materialized 
     * indexes does not exceed.
     *  
     * @param B
     *      The space budget
     */
    public void setSpaceBudget(double B);
}
