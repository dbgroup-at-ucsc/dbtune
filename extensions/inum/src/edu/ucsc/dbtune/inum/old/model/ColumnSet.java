package edu.ucsc.dbtune.inum.old.model;

import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * Created by IntelliJ IDEA.
 * User: Debabrata Dash
 * Date: May 7, 2006
 * Time: 6:20:49 PM
 * The file is a copyright of CMU, all rights reserved.
 */
public class ColumnSet extends LinkedHashSet {
    private String _string;

    public ColumnSet(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);    //To change body of overridden methods use File | Settings | File Templates.
    }

    public ColumnSet(int initialCapacity) {
        super(initialCapacity);    //To change body of overridden methods use File | Settings | File Templates.
    }

    public ColumnSet() {
        super();    //To change body of overridden methods use File | Settings | File Templates.
    }

    public ColumnSet(Collection c) {
        super(c);    //To change body of overridden methods use File | Settings | File Templates.
    }

    public int hashCode() {
        return System.identityHashCode(this);
    }

    public boolean equals(Object o) {
        return this == o;
    }

    public String toString() {
        if (_string == null) {
            _string = super.toString();
        }

        return _string;
    }
}