package edu.ucsc.dbtune.bip.sim;

import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.metadata.Index;

public class MaterializationSchedule 
{
    private int W;
    private List<List<Index>> listCreateIndexesWindow;
    private List<List<Index>> listDropIndexesWindow;
     
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
    
    public void addIndexWindow(Index index, int window, int type)
    {
        if (type == SimVariablePool.VAR_CREATE) {
            listCreateIndexesWindow.get(window).add(index);
        } else if  (type == SimVariablePool.VAR_DROP) {
            listDropIndexesWindow.get(window).add(index);
        }
    }

    @Override
    public String toString() {
        String result =  "MaterializationSchedule [W=" + W + "]\n";
        for (int w = 0; w < this.W; w++) {
            result += (" Window " + w + "-th: \n");
            for (Index index : this.listCreateIndexesWindow.get(w)) {
                result += ("CREATE INDEX "  + index.getFullyQualifiedName() + " Time: " + index.getCreationCost() + "\n");
            }
            for (Index index : this.listDropIndexesWindow.get(w)) {
                result += ("DROP INDEX "  + index.getFullyQualifiedName() + "\n");
            }
        }
        return result;
    }
    
}
