package edu.ucsc.dbtune.bip.sim;

import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.bip.core.BIPOutput;
import edu.ucsc.dbtune.metadata.Index;

/**
 * The class stores a materialization schedule that includes indexes to be created/dropped
 * at each maintenance window
 * 
 * @author tqtrung@soe.ucsc.edu
 *
 */
public class MaterializationSchedule extends BIPOutput
{
    private int W;
    private List<List<Index>> listCreateIndexesWindow;
    private List<List<Index>> listDropIndexesWindow;
    
    /**
     * Constructor of a materialization schedule, given the number of maintenance window
     * that this schedule corresponds to 
     * 
     * @param _W
     *      The number of maintenance windows
     */
    public MaterializationSchedule(int _W)
    {
        this.W = _W;
        listCreateIndexesWindow = new ArrayList<List<Index>>();
        listDropIndexesWindow = new ArrayList<List<Index>>();
        for (int w = 0; w < W; w++) {
            List<Index> listCreateIndexes = new ArrayList<Index>();
            List<Index> listDropIndexes = new ArrayList<Index>();
            listCreateIndexesWindow.add(listCreateIndexes);
            listDropIndexesWindow.add(listDropIndexes);
        }
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
        if (type == SimVariablePool.VAR_CREATE) {
            listCreateIndexesWindow.get(window).add(index);
        } else if  (type == SimVariablePool.VAR_DROP) {
            listDropIndexesWindow.get(window).add(index);
        } else {
            throw new RuntimeException("The type of index must be created or dropped");
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("MaterializationSchedule [W=" + W + "]\n");
        for (int w = 0; w < this.W; w++) {
            result.append(" Window " + w + "-th: \n");
            for (Index index : this.listCreateIndexesWindow.get(w)) {
                result.append("CREATE INDEX "  + index.getFullyQualifiedName() + " Time: " + index.getCreationCost() + "\n");
            }
            for (Index index : this.listDropIndexesWindow.get(w)) {
                result.append("DROP INDEX "  + index.getFullyQualifiedName() + "\n");
            }
        }
        return result.toString();
    }
}
