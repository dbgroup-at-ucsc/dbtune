package edu.ucsc.dbtune.bip.sim;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.metadata.Index;

/**
 * The class stores a materialization schedule that includes indexes to be created/dropped
 * at each maintenance window
 * 
 * @author tqtrung@soe.ucsc.edu
 *
 */
public class Schedule extends IndexTuningOutput
{
    
    private List<List<Index>> createdIndexesWindow;
    private List<List<Index>> droppedIndexesWindow;
    
    private Set<Index> Sinit;
    private int        W;
    
    /**
     * Constructor of a materialization schedule, given the number of maintenance window
     * that this schedule corresponds to 
     * 
     * @param W
     *      The number of maintenance windows
     * @param Sinit
     *      The set of indexes that are currently materialized in the system     
     */
    public Schedule(int W, Set<Index> Sinit)
    {
        this.W = W;
        this.Sinit = Sinit;
        
        createdIndexesWindow = new ArrayList<List<Index>>();
        droppedIndexesWindow = new ArrayList<List<Index>>();
        
        for (int w = 0; w < W; w++) {
            
            List<Index> createIndexes = new ArrayList<Index>();
            List<Index> dropIndexes = new ArrayList<Index>();
            
            createdIndexesWindow.add(createIndexes);
            droppedIndexesWindow.add(dropIndexes);
            
        }
    }
    
    
    /**
     * Retrieve the number of windows
     * @return
     *      The number of windows
     */
    public int getNumberWindows()
    {
        return W;
    }
    /**
     * Retrieve the set of indexes that are currently materialized in the system
     * 
     * @return
     *      A set of index
     */
    public Set<Index> getIntialIndexes()
    {
        return Sinit;
    }
    
    /**
     * Retrieve the set of indexes that are recommended to be materialized at the given window
     * 
     * @param window
     *      The window to retrieve materialized indexes
     *      
     * @return
     *      A set of indexes
     */
    public Set<Index> getMaterializedIndexes(int window)
    {
        return new HashSet<Index>(createdIndexesWindow.get(window));
    }
    
    /**
     * Retrieve the set of indexes that are recommended to be dropped at the given window
     * 
     * @param window
     *      The window to retrieve materialized indexes
     *      
     * @return
     *      A set of indexes
     */
    public Set<Index> getDroppedIndexes(int window)
    {
        return new HashSet<Index>(droppedIndexesWindow.get(window));
    }
    
    
    /**
     * Record the given {@code index} as being created/dropped, 
     * depending on the value of {@code type}, at maintenance window {@code window}
     * 
     * @param index
     *      The index that is created/dropped
     * @param window
     *      The window when the index is materialized          
     * @param type
     *      The operation is either create when {@code type = VAR_CREATE},
     *      or drop when {@code type = VAR_DROP})
     */
    public void addIndexWindow(Index index, int window, int type)
    {
        if (type == SimVariablePool.VAR_CREATE) 
            createdIndexesWindow.get(window).add(index);
        else if  (type == SimVariablePool.VAR_DROP) 
            droppedIndexesWindow.get(window).add(index);
        else 
            throw new RuntimeException("The type of index must be created or dropped");
    }

    @Override
    public String toString() 
    {   
        StringBuilder result = new StringBuilder();
        
        result.append("MaterializationSchedule [W=" + W + "]\n");
        
        for (int w = 0; w < W; w++) {
            
            result.append(" Window " + w + "-th: \n");
            
            for (Index index : createdIndexesWindow.get(w)) 
                result.append("CREATE INDEX "  + index + " Time: " + 
                               index.getCreationCost() + "\n");
            
            for (Index index : droppedIndexesWindow.get(w)) 
                result.append("DROP INDEX "  + index + "\n");
            
        }
        
        return result.toString();
    }
}
