package edu.ucsc.dbtune.advisor;

import java.sql.SQLException;

/**
 * An advisor that can be paused and replayed.
 *
 * @author Ivo Jimenez
 */
public interface PlayableAdvisor extends Advisor
{
    /**
     */
    void stop();

    /**
     */
    void play() throws SQLException;

    /**
     */
    void next() throws SQLException;
}
