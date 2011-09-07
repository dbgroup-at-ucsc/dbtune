/*
 * Copyright (c) 2010.  All rights reserved by DIAS Lab, EPFL.
 */

package edu.ucsc.dbtune.inum.old;

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CollectionEnumerator {

    ArrayList limits;
    private List<Collection> input;
    public Enumerator enum1;

    public CollectionEnumerator(List<Collection> input) {
        this.input = input;
        limits = new ArrayList();
        for (int i = 0; i < input.size(); i++) {
            Collection collection = input.get(i);
            limits.add(collection.size());
        }
        enum1 = new Enumerator(limits);
    }

    public ArrayList next() {
        List l = enum1.next();
        if(l != null) {
            ArrayList ret = new ArrayList(input.size());
            for (int i = 0; i < l.size(); i++) {
                int idx = (Integer) l.get(i);
                idx --;
                if(idx >= 0) {
                    ret.add(CollectionUtils.get(input.get(i), idx));
                }
            }

            return ret;
        } else {
            return null;
        }
    }
}