package edu.ucsc.dbtune.util;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;

import static org.junit.Assert.assertThat;

/**
 * A binary tree implementation.
 *
 * @author Ivo Jimenez
 */
public class EventBusFactoryTest
{
    /** checks that the access to the singleton works. */
    @Test
    public void testBasic()
    {
        EventBus bus = EventBusFactory.getEventBusInstance();

        SimpleSubscriber sub = new SimpleSubscriber();

        bus.register(sub);
        bus.post("this.should.get.to.the.subscriber");

        assertThat(sub.isGotNotified(), is(true));
    }

    /**
     * A simple subscriber.
     *
     * @author Ivo Jimenez
     */
    private class SimpleSubscriber
    {
        private boolean gotNotified;

        /**
         * handles the event.
         *
         * @param eventId
         *      eventid
         */
        @Subscribe
        public void handleEvent(String eventId)
        {
            if (eventId.equals("this.should.get.to.the.subscriber"))
                gotNotified = true;
        }

        /**
         * @return the gotNotified
         */
        public boolean isGotNotified()
        {
            return gotNotified;
        }
    }
}
