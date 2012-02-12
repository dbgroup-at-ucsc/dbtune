package edu.ucsc.dbtune.candidategeneration;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.Workload;

/**
 * This class abstracts the many different ways of generating candidate configurations.
 *
 * @author Quoc Trung Tran
 * @author Ivo Jimenez
 */
public interface CandidateGenerator
{
    /**
     * Generates candidates for the given workload.
     *
     * @param workload
     *      the workload for which to generate candidates
     * @return
     *      a candidate set for the given workload
     * @throws SQLException
     *      if the generation of candidates can't be done
     */
    Set<Index> generate(Workload workload) throws SQLException;
}
