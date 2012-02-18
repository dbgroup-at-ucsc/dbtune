package edu.ucsc.dbtune.bip.interactions;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.advisor.interactions.IndexInteraction;
import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;

public class InteractionLP extends AbstractBIPSolver 
{    
    protected Map<IndexInteraction, Integer> cacheInteractingPairs;
    protected InteractionOutput              interactionOutput;
    protected Optimizer                      optimizer;
    
    protected double delta;
    protected int    numCplexCall;
    
    protected Map<Integer, Map<Index, Integer>>   mapStmtIndexSlotID;
    protected Map<Index, Set<Integer>>            mapIndexListStatements;
    
    
    public InteractionLP(double delta)
    {
        this.delta        = delta;
        this.numCplexCall = 0;
        
        this.cacheInteractingPairs = new HashMap<IndexInteraction, Integer>();
    }
    
    
    @Override
    public IndexTuningOutput solve() throws SQLException, IOException
    {   
        // 1. Communicate with INUM 
        // to derive the query plan description including internal cost, index access cost,
        // index at each slot, etc.  
        logger.setStartTimer();
        populatePlanDescriptionForStatements();
        logger.onLogEvent(LogListener.EVENT_POPULATING_INUM);
        
        // 2. Iterate over the list of query plan descs that have been derived
        interactionOutput = new InteractionOutput();
        
        // Derive list of indexes that might interact
        mapStmtIndexSlotID = new HashMap<Integer, Map<Index, Integer>>();
        mapIndexListStatements = new HashMap<Index, Set<Integer>>();
        
        int pos = 0;
        for (QueryPlanDesc desc : listQueryPlanDescs) {
            
            Map<Index, Integer> mapIndexSlotID = new HashMap<Index, Integer>();
            
            // Only consider indexes that are compatible with at least one sot
            // in one template plan of each statement
            for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                
                for (Index index : desc.getActiveIndexsAtSlot(i)) {
                    mapIndexSlotID.put(index, i);
                    
                    Set<Integer> listStmtPos = mapIndexListStatements.get(index);
                    if (listStmtPos == null) {
                        listStmtPos = new HashSet<Integer>();
                    } 
                    
                    listStmtPos.add(pos);                        
                    mapIndexListStatements.put(index, listStmtPos);
                }
            }
            
            mapStmtIndexSlotID.put(pos, mapIndexSlotID);
            pos++;
        }
        
        findInteractions();
        
        return getOutput();
    }
    
    protected void findInteractions()
    {
        List<Index> indexes = new ArrayList<Index>(mapIndexListStatements.keySet());
        
        Index indexc, indexd;
        IndexInteraction pair;
        numCplexCall = 0;
        
        for (int pos_c = 0; pos_c < indexes.size(); pos_c++) {
            
            indexc = indexes.get(pos_c);
            
            for (int pos_d = pos_c + 1; pos_d < indexes.size(); pos_d++) {
                
                indexd = indexes.get(pos_d);
                
                // derive the common set of statements
                Set<Integer> intersectID = new HashSet<Integer>
                                               (mapIndexListStatements.get(indexc));
                intersectID.retainAll(mapIndexListStatements.get(indexd));
                
                List<QueryPlanDesc> lsql = new ArrayList<QueryPlanDesc>();
                List<Integer>       listRelationC = new ArrayList<Integer>();
                List<Integer>       listRelationD = new ArrayList<Integer>();
                
                if (intersectID.size() == 0)
                    continue;
                
                numCplexCall++;
                
                for (int pos : intersectID) {
                    lsql.add(listQueryPlanDescs.get(pos));
                    listRelationC.add(mapStmtIndexSlotID.get(pos).get(indexc));
                    listRelationD.add(mapStmtIndexSlotID.get(pos).get(indexd));
                }

                RestrictLP restrict = new RestrictLP(null, logger, delta, indexc, indexd,
                                                    candidateIndexes, 0, 0);
                restrict.setListQueryDescriptions(lsql, listRelationC, listRelationD);
                
                if (restrict.solve()) {
                    // cache pairs of interaction
                    pair = new IndexInteraction (restrict.getFirstIndex(), restrict.getSecondIndex());
                    cacheInteractingPairs.put(pair, 1);
                    
                    // store in the output
                    pair.setDoiBIP(restrict.getDoiBIP());
                    pair.setDoiOptimizer(restrict.getDoiOptimizer());
                    interactionOutput.add(pair);
                }
                //restrict.clear();
            }
        }
        
        System.out.println("L144, number of CPLEX calls: " + numCplexCall);
    }
    
    
    @Override
    protected IndexTuningOutput getOutput() 
    {
        return interactionOutput;
    }
   
    
    @Override
    protected void buildBIP() 
    {

    }
}
