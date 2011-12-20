package edu.ucsc.dbtune.util;

/**
 * An object is identifiable if for every distinct instance, the {@link #getId} method returns a 
 * different integer identifier. This defines a total ordering for all of the instances of the 
 * class. 
 *
 * @author Ivo Jimenez
 */
public interface Identifiable
{
    /**
     * Returns the identifier of the object.
     *
     * @return
     *      the numeric identifier of the object
     */
    int getId();
}
