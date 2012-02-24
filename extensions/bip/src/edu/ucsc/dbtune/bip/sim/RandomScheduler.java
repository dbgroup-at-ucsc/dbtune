package edu.ucsc.dbtune.bip.sim;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.metadata.Index;

public class RandomScheduler extends SimBIP 
{   
    /**
     * This method implements a simple random schedule algorithm 
     * that randomly places indexes into each window such that the 
     * number of indexes (resp. the total create cost of indexes) at each 
     * window does not exceed the given maximum number of indexes
     * (resp. maximum creation cost)
     *  
     */
    @Override
    public IndexTuningOutput solve() throws SQLException, IOException
    {
        if (super.isConstraintCreationCostWindow)
            return randomScheduleConstraintCreationCostWindow();
        
        if (super.isConstraintNumberIndexesWindow)
            return randomScheduleConstraintNumberIndexesWindow();
        
        return null;
    }
    
    /**
     * This method implements a simple random schedule algorithm 
     * that randomly places indexes into each window such that the 
     * number of indexes at each window does not exceed the given 
     * maximum number of indexes.
     *  
     */
    private Schedule randomScheduleConstraintNumberIndexesWindow()
    {
        // Assume Sdrop = \emptyset
        List<Integer> numberIndexesWindow = new ArrayList<Integer>();
        int w, numIndexes, numTries;
        
        for (w = 0; w < super.W; w++)  
            numberIndexesWindow.add(0);
        
        Random generator = new Random(19580427);
        Schedule ms = new Schedule(W, Sinit);
        
        for (Index index : super.Screate){
            
            numTries = 0;
            
            while (numTries <= 3 * W) {
                
                w = generator.nextInt(W);
                numIndexes = numberIndexesWindow.get(w);
                
                if (numIndexes < super.maxNumberIndexesWindow) {
                    // assign Index into window w
                    ms.addIndexWindow(index, w, SimVariablePool.VAR_CREATE);
                    numIndexes++;
                    numberIndexesWindow.set(w, numIndexes);
                    break;
                }
                
                numTries++;
            }
        }
        
        return ms;
    }    
    
    /**
     * This method implements a simple random schedule algorithm 
     * that randomly places indexes into each window such that the 
     * the cost to create index in each window does not exceed the 
     * given maximum-limit. 
     *  
     */
    private Schedule randomScheduleConstraintCreationCostWindow()
    {
        // Assume Sdrop = \emptyset
        List<Double> creationCostWindow = new ArrayList<Double>();
        int w, numTries;
        double accumulatedCost;
        
        for (w = 0; w < super.W; w++)  
            creationCostWindow.add(0.0);
        
        Random generator = new Random(19580427);
        Schedule ms = new Schedule(W, Sinit);
        
        for (Index index : super.Screate){
            
            numTries = 0;
            
            while (numTries <= 3 * W) {
                
                w = generator.nextInt(W);
                accumulatedCost = creationCostWindow.get(w);
                
                if (accumulatedCost < super.maxCreationCostWindow) {
                    // assign Index into window w
                    ms.addIndexWindow(index, w, SimVariablePool.VAR_CREATE);
                    accumulatedCost += index.getCreationCost();
                    creationCostWindow.set(w, accumulatedCost);
                    break;
                }
                
                numTries++;
            }
        }
        
        return ms;
    }   
}
