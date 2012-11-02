package edu.ucsc.dbtune.bip.div;

import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_X;
import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

/**
 * This class routes unseen queries using similarity metrics 
 * 
 * @author Quoc Trung Tran
 *
 */
public class RoutingDivergent extends RobustDivBIP
{   
    private Map<Integer, DivergentQueryFeature> seenQueryFeatures;
    private DivConfiguration divConf;
    
    public RoutingDivergent(double optCost, double nodeFactor,
            double failureFactor) 
    {
        super(optCost, nodeFactor, failureFactor);
    }
    
    /**
     * Find the routing replicas for the unseen workload
     * 
     * @param unseen
     *      The unseen workload
     *      
     * @return
     *      The routing function
     * @throws Exception
     */
    public DivConfiguration routeUnseenWorkload(Workload unseen, 
                                        List<QueryPlanDesc> unSeenDescs) 
            throws Exception
    {
        DivergentQueryFeature feature;
        // 1. derive the output
        divConf = (DivConfiguration) getOutput();
        
        // 2. derive the features of seen query
        deriveSeenQueryFeatures();
        
        // 3. find the similarity
        DivConfiguration result = new DivConfiguration(divConf.getNumberReplicas(),
                                    divConf.getLoadFactor());
        for (int r = 0; r < divConf.getNumberReplicas(); r++)
            for (Index index : divConf.indexesAtReplica(r))
                    result.addIndexReplica(r, index);

        List<SortableObject> scores; 
        double score;
        int q;
        int counterUnSeen = 0;
        int idUnSeen = 0;
        int idMostSimilar;
        
        for (SQLStatement sql : unseen) {
            // 4. compute the feature of sql
            feature = computeFeature(sql);
     
            // 5. compute the similarity
            scores = new ArrayList<SortableObject>();
            for (QueryPlanDesc desc : queryPlanDescs){
                if (desc.getSQLCategory().isSame(NOT_SELECT)) 
                    continue;
                    
                q =  desc.getStatementID();
                score = seenQueryFeatures.get(q).cosineSimilarity(feature);
                scores.add(new SortableObject(q, score));
            }
                    
            Collections.sort(scores); 
        
            // TODO: might consider top-2
            // For the time being: get the top query that is similar
            // to the given query
            idMostSimilar = scores.get(0).getID();
            idUnSeen = unSeenDescs.get(counterUnSeen).getStatementID(); 
            // Route the unseen query similar to the idMostSimilar query
            for (int r : divConf.getRoutingReplica(idMostSimilar))
                result.routeQueryToReplica(idUnSeen, r);
            
            counterUnSeen++;
        }
        
        return result;
    }
    
    /**
     * Compute the feature for a query
     * 
     * @param sql
     *      The SQL query
     * @return
     *      The query feature
     * @throws Exception
     */
    private DivergentQueryFeature computeFeature(SQLStatement sql)
            throws Exception
    {
        DivergentQueryFeature feature = new DivergentQueryFeature(divConf);
        InumPreparedSQLStatement space= (InumPreparedSQLStatement) inumOptimizer.prepareExplain(sql);
        
        ExplainedSQLStatement inumPlan;

        for (int r = 0; r < divConf.getNumberReplicas(); r++) {
            inumPlan = space.explain(divConf.indexesAtReplica(r));
            
            for (Index index : inumPlan.getUsedConfiguration())
                feature.addUsedIndexAtReplica(index, r);
        }
        
        return feature;    
    }
    
    /**
     * Derive the set feature for query query statement
     * @throws Exception
     */
    private void deriveSeenQueryFeatures() throws Exception
    {
        seenQueryFeatures = new HashMap<Integer, DivergentQueryFeature>();
        DivergentQueryFeature feature;
        
        for (QueryPlanDesc desc : queryPlanDescs) {
            
            feature = new DivergentQueryFeature(divConf);
            
            if (desc.getSQLCategory().isSame(NOT_SELECT)) 
                continue;
            
            for (int r = 0; r < divConf.getNumberReplicas(); r++)
                for (Index index : findUsedIndexAtReplica(desc, r))
                    feature.addUsedIndexAtReplica(index, r);
            
            seenQueryFeatures.put(desc.getStatementID(), feature);
        }
    }
    
    /**
     * Derive the set of indexes used to compute cost(q, r)
     * 
     * @param desc
     *      The given query
     * @param r
     *      The given replica
     * @return
     *      The set of indexes
     * @throws Exception
     */
    private Set<Index> findUsedIndexAtReplica(QueryPlanDesc desc, int r)
            throws Exception
    {
        int idX;
        int q = desc.getStatementID();
        Set<Index> indexes = new HashSet<Index>();
        
        // used index
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(k); i++)   
                for (Index index : desc.getIndexesAtSlot(k, i)) {
                    idX = poolVariables.get(VAR_X, r, q, k, i, index.getId()).getId();
                
                    if (cplex.getValue(cplexVar.get(idX)) > 0)
                        indexes.add(index);
                }
     
        return indexes;
    }
}
