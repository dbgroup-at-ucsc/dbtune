package edu.ucsc.dbtune.bip.div;

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
     * Set the replica imbalance factor. That is, the total cost of one replica does not exceed
     * {@code beta} times the code of other replica.
     *  
     * @param beta
     *      The factor
     */
    public void setReplicaImbalanceFactor(int beta);
    
    /**
     * Set the maximum space budget imposed on each replica, which the total size of materialized 
     * indexes does not exceed.
     *  
     * @param B
     *      The space budget
     */
    public void setSpaceBudget(double B);
}
