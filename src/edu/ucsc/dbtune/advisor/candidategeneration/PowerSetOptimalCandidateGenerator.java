package edu.ucsc.dbtune.advisor.candidategeneration;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.ByContentIndex;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Permutations;

import edu.ucsc.dbtune.workload.SQLStatement;
import static edu.ucsc.dbtune.util.MetadataUtils.toByContent;

/**
 * Generate the set of candidate indexes that contain all subsets of columns of indexes obtained 
 * from a delegate {@link CandidateGenerator}.
 *
 * @author Quoc Trung Tran  
 */
public class PowerSetOptimalCandidateGenerator extends AbstractCandidateGenerator 
{
    private CandidateGenerator delegate;
    private Optimizer optimizer;
    private int maxCols;
    
    /**
     * Constructs a generator with the given delegate used to generate candidate indexes
     * and the maximum number of columns that an index can have.
     *
     * @param delegate
     *      an optimizer
     * @param maxCols
     *      the maximum number of columns in an index. If zero or negative, the size of an index is 
     *      taken as the maximum number of columns that an index has
     */
    public PowerSetOptimalCandidateGenerator(Optimizer optimizer, 
                                             CandidateGenerator delegate,
                                             int maxCols)
    {
        this.delegate = delegate;
        this.maxCols  = maxCols;
        this.optimizer = optimizer;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Set<ByContentIndex> generateByContent(SQLStatement sql)
        throws SQLException
    {
        Set<ByContentIndex>  indexes = new HashSet<ByContentIndex>();
        List<Column>         idxCols;
        Permutations<Column> per;
        Map<Column, Boolean> ascendingIndex;
        
        int max;
        StringBuilder query;
        String asc;
        Set<Index> recommendation;
       
        for (Index index : delegate.generate(sql)) {
            
            if (maxCols > (index.columns().size() - 1))
                max = index.columns().size() - 1;
            else
                max = maxCols;
          
            for (int num = 1; num <= max; num++) {
                
                per = new Permutations<Column>(index.columns(), num);                
                ascendingIndex = new HashMap<Column, Boolean>();
                
                while (per.hasNext()) {
                    
                    idxCols = per.next();
                    
                    query = new StringBuilder();
                    query.append(" SELECT ");
                    for (Column col : idxCols) {
                        ascendingIndex.put(col, index.isAscending(col));
                        query.append(col.getName() + " , ");
                    }
                    
                    query.delete(query.length() - 2, query.length());
                    query.append("\n FROM " + index.getTable().getFullyQualifiedName());
                    query.append("\n ORDER BY ");
                    
                    for (Column col : idxCols) {
                        asc = index.isAscending(col) ? " ASC " : " DESC ";
                        query.append(col.getName() + asc + " , ");
                    }
                    
                    query.delete(query.length() - 2, query.length());
                    
                    //Index idx = new Index(idxCols, ascendingIndex);
                    // indexes.add(new ByContentIndex(idx));
                    // String query = 
                    // "select " + columns + " from " + index.getTableName() 
                    // + " order by " + columns;
                    recommendation = optimizer.recommendIndexes(query.toString());
                    
                    /*
                    if (recommendation.size() != 1) 
                        throw new RuntimeException(" We expect the optimizer to recommend"
                                + " one index for the query: " + query.toString()
                                + "\n recommendation: " + recommendation);
                    */
                    indexes.addAll(toByContent(recommendation));
                }
            }  
            
        }

        return indexes;
    }
}
