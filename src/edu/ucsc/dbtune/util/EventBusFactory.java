package edu.ucsc.dbtune.util;

import com.google.common.eventbus.EventBus;

/**
 * A binary tree implementation.
 *
 * @author Ivo Jimenez
 */
public final class EventBusFactory
{
    private static EventBus eventBus;

    /**
     * Utility class.
     */
    private EventBusFactory()
    {
    }

    /**
     * Returns the bus singleton.
     *
     * @return
     *      the only instance of the event bus used to communicate throughout the DBTune API
     */
    public static EventBus getEventBusInstance()
    {
        if (eventBus == null) {
            eventBus = new EventBus();
        }
        return eventBus;
    }
}
