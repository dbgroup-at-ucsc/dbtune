/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */

package edu.ucsc.dbtune.util;

import edu.ucsc.dbtune.metadata.Index;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;

public class IndexSet implements Iterable<Index>, Serializable {
    
    private IndexBitSet bs;
    private Set<Index> set;
    private List<Index> list;
    private long maxInternalId;

    /* serialization support */
    public static final long serialVersionUID = 1L;
    
    public IndexSet() {
        clear();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeInt(list.size());
        for (Index idx : list)
            out.writeObject(idx);
    }
        
    public final void clear() {
        set = new HashSet<Index>();
        list = new ArrayList<Index>();
        bs = new IndexBitSet();
        maxInternalId = -1;
    }
    
    public void add(Index idx) {
        if (set.add(idx)) {
            list.add(idx);
            if(idx.getId() > Integer.MAX_VALUE) {
                throw new RuntimeException("Overflowed id " + idx.getId());
            }
            bs.set((int)idx.getId());
            if (idx.getId() > maxInternalId){
                maxInternalId = idx.getId();
            }
        }
    }
    
    public Iterator<Index> iterator() {
        return list.iterator();
    }
    
    public long maxInternalId() {
        return maxInternalId;
    }

    public int size() {
        return set.size();
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(size()).append(" indexes\n");
        
        for (Index idx : this) {
            sb.append(idx).append("\n");
        }
        
        return sb.toString();
    }

    // Renumber the indexes so they are sorted by their creation text 
    public void normalize() throws SQLException {
        Index[] array = new Index[list.size()];
        array = list.<Index>toArray(array);
        Arrays.sort(array, schemaComparator);
        
        // start from scratch to be safe
        clear();
        for (int i = 0; i < array.length; i++) {
            Index idx = new Index(array[i]);
            idx.setId(i);
            add(idx);
        }
    }

    public IndexBitSet bitSet() {
        return bs.clone();
    }
    
    /*
     * java.util.Comparator for displaying indexes in an easy to read format
     */
    private static Comparator<Index> schemaComparator = new Comparator<Index>() {
        public int compare(Index o1, Index o2) {
            return o1.getCreateStatement().compareTo(o2.getCreateStatement());
        }
    };
}
