package edu.ucsc.dbtune.bip.div;

public interface OnlineBIP 
{
    /**
     * Populate the set of query plan descriptions for the given workload
     */
    void populateQueryPlanDescriptions() throws Exception;
    
    /**
     * Set the number of statements in a window
     * @param w
     */
    void setWindowDuration(int w);
    
    /**
     * Advance the next statement that is seen. 
     *      
     */
    void next() throws Exception;
    
    /**
     * Retrieve the total cost of the current processing window
     * 
     * @return
     *      The total cost
     */
    double getTotalCost();
    
    /**
     * Retrieve the total cost with respect to the intial configuration
     * @return
     */
    double getTotalCostInitialConfiguration() throws Exception;
    
    /**
     * Check if we need to re-configuration
     */
    boolean isNeedToReconfiguration();
}
