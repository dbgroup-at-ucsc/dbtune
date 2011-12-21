package edu.ucsc.dbtune.inum;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.sql.Connection;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.spi.Console;
import edu.ucsc.dbtune.util.Combinations;
import edu.ucsc.dbtune.util.Strings;

/**
 * Default implementation of Inum's {@link Precomputation precomputation} step.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumPrecomputation implements Precomputation
{
    private final OptimalPlanProvider         provider;
    private final OptimalPlansParser          parser;
    private final InumSpace                   inumSpace;
    private final Set<String>                 seenWorkloads;

    InumPrecomputation(InumSpace inumSpace,
            OptimalPlanProvider provider, OptimalPlansParser parser)
    {
        this.provider         = provider;
        this.parser           = parser;
        this.inumSpace        = inumSpace;
        this.seenWorkloads    = Sets.newHashSet();
    }

    public InumPrecomputation(Connection connection)
    {
        this(new InMemoryInumSpace(), new SqlExecutionPlanProvider(connection), new InumOptimalPlansParser());
    }

    private void addQuerytoListOfSeenQueries(String query)
    {
        Preconditions.checkArgument(!Strings.isEmpty(query));
        if(!seenWorkloads.contains(query)){
            seenWorkloads.add(query);
        }
    }

    @Override public InumSpace getInumSpace() 
    {
        return inumSpace;
    }

      final QueryRecord key = new QueryRecord(query, o);
      getInumSpace().save(key, optimalPlansPerInterestingOrder);

            final Key key = new Key(query, o);
            getInumSpace().save(key, optimalPlansPerInterestingOrder);

            Console.streaming().info(
                    String.format("%d optimal plans were cached for %s key.",
                        getInumSpace().getOptimalPlans(key).size(),
                        key
                        )
                    );

        }

        return getInumSpace();
    }

    @Override public boolean skip(String query) {
        return seenWorkloads.contains(query);
    }
}
