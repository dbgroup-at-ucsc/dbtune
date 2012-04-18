package edu.ucsc.dbtune.bip.div;

import edu.ucsc.dbtune.bip.indexadvisor.ConstraintIndexAdvisor;

/**
 * A standard set of parameters that a Divergent Index Tuning problem handles. 
 *
 */
public interface Divergent extends ConstraintIndexAdvisor
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
}
