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
import edu.ucsc.dbtune.util.Permutations;

import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Generate the set of candidate indexes that contain all subsets of columns of indexes obtained 
 * from a delegate {@link CandidateGenerator}.
 *
 * @author Quoc Trung Tran  
 */
public class PowerSetOptimalCandidateGenerator extends AbstractCandidateGenerator 
{
    private CandidateGenerator delegate;
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
    public PowerSetOptimalCandidateGenerator(CandidateGenerator delegate, int maxCols)
    {
        this.delegate = delegate;
        this.maxCols  = maxCols;
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
                    
                    for (Column col : idxCols)
                        ascendingIndex.put(col, index.isAscending(col));
                    
                    Index idx = new Index(idxCols, ascendingIndex);
                    indexes.add(new ByContentIndex(idx));
                    
                }
            }  
            
        }

        return indexes;
    }
}
