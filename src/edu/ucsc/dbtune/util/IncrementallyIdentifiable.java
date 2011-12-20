package edu.ucsc.dbtune.util;

/**
 * An object is incrementally identifiable if for every distinct instances, the {@link #getId} 
 * method returns a different integer identifier. The identifiers increase as more objects of the 
 * class get created. For example:
 * <pre>
 * {@code
 * Implementor a = new Implementor();
 * Implementor b = new Implementor();
 *
 * assertThat(a.getId() < b.getId(), is(true));
 * }
 * </pre>
 * <p>
 * the above test should pass for every two objects from the class. Preferably, the counter should 
 * begin from 0.
 *
 * @author Ivo Jimenez
 */
public interface IncrementallyIdentifiable extends Identifiable
{
    /**
     * Returns the identifier of the object.
     *
     * @return
     *      the numeric identifier of the object
     */
    int getId();
}
