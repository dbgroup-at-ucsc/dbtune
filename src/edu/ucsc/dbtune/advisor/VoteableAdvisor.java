package edu.ucsc.dbtune.advisor;

import java.sql.SQLException;

import edu.ucsc.dbtune.metadata.Index;

/**
 * An advisor whose candidate index set can members voted up or down.
 *
 * @author Ivo Jimenez
 */
public interface VoteableAdvisor
{
    /**
     * Gives a positive vote for the given index.
     *
     * @param index
     *      index being voted
     * @throws SQLException
     *      if the index can't be voted
     */
    void voteUp(Index index)
        throws SQLException;

    /**
     * Gives a negative vote for the given index.
     *
     * @param index
     *      index being voted
     * @throws SQLException
     *      if the index can't be voted
     */
    void voteDown(Index index)
        throws SQLException;
}
