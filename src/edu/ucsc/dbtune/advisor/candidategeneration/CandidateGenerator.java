package edu.ucsc.dbtune.advisor.candidategeneration;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static com.google.common.collect.Lists.reverse;

import static edu.ucsc.dbtune.util.EnvironmentProperties.ONE_COLUMN;
import static edu.ucsc.dbtune.util.EnvironmentProperties.OPTIMIZER;
import static edu.ucsc.dbtune.util.EnvironmentProperties.POWERSET;

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

    /**
     * Generates candidates for the given statement.
     *
     * @param statement
     *      the workload for which to generate candidates
     * @return
     *      a candidate set for the given statement
     * @throws SQLException
     *      if the generation of candidates can't be done
     */
    Set<Index> generate(SQLStatement statement) throws SQLException;

    /**
     */
    public abstract class Factory
    {
        /**
         * utility class.
         */
        private Factory()
        {
        }

        /**
         * Creates an instance of a candidate generator given a configuration.
         *
         * @param env
         *      an environment object used to access the properties of the system
         * @param opt
         *      if {@link Environment#OPTIMIZER} is specified, {@code opt} should be provided.
         * @return
         *      a new candidate generator
         * @throws InstantiationException
         *      if {@link Environment#OPTIMIZER} is specified and optimizer is null; if a generator 
         *      that accepts another as delegate is instantiated but a delegate is missing.
         */
        public static CandidateGenerator newCandidateGenerator(Environment env, Optimizer opt)
            throws InstantiationException
        {
            CandidateGenerator cg = null;

            for (String generatorOption : reverse(env.getCandidateGenerator()))

                if (generatorOption.equals(ONE_COLUMN) && cg == null)
                    throw new InstantiationException(
                            "Can't instantiate " + ONE_COLUMN + " without another generator");
                else if (generatorOption.equals(ONE_COLUMN))
                    cg = new OneColumnCandidateGenerator(cg);
                else if (generatorOption.equals(POWERSET) && cg == null)
                    throw new InstantiationException(
                            "Can't instantiate " + POWERSET + " without another generator");
                else if (generatorOption.equals(POWERSET))
                    cg = new PowerSetOptimalCandidateGenerator(opt, cg, 2);
                else if (generatorOption.equals(OPTIMIZER) && opt == null)
                    throw new InstantiationException(
                            "Can't instantiate " + OPTIMIZER + " generator without an optimizer");
                else if (generatorOption.equals(OPTIMIZER))
                    cg = new OptimizerCandidateGenerator(opt);
                else
                    throw new InstantiationException("Unknown " + generatorOption + " generator");
            
            return cg;
        }
    }
}
