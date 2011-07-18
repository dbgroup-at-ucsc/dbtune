package edu.ucsc.dbtune.tools.cmudb.inum;
import java.util.ArrayList;
import java.util.Arrays;

public class Enumerator {

    ArrayList state;
    ArrayList limits;

    public Enumerator(ArrayList input) {
        limits = new ArrayList(input);
        state = new ArrayList(input);
        for (int i = 0; i < state.size(); i++) {
            state.set(i, new Integer(0));
            state.set(state.size() - 1, new Integer(-1));
        }
    }

    
    public ArrayList next() 
    {
        if (state.size() == 0) //no interesting orders...
            return null;
        if (((Integer) state.get(state.size() - 1)).intValue() == ((Integer) limits.get(state.size() - 1)).intValue()) 
        {
            for (int index = state.size() - 2; index >= 0; index--) 
            {
                if (((Integer) state.get(index)).intValue() != ((Integer) limits.get(index)).intValue()) {
                    state.set(index, new Integer(((Integer) state.get(index)).intValue() + 1));
                    for (int index2 = index + 1; index2 <= state.size() - 1; index2++) {
                        state.set(index2, 0);
                    }
                    return state;
                }
            }

            return null;
        } else {
            state.set(state.size() - 1, new Integer(((Integer) state.get(state.size() - 1)).intValue() + 1));

            return state;
        }
    }

    public static void main(String[] args) {
        Enumerator enum1 = new Enumerator(new ArrayList(Arrays.asList(24,108,1)));
        int count = 0;
        ArrayList state = enum1.next();
        while(state != null) {
            count++;
            state = enum1.next();
        }

        System.out.println("count = " + count);
    }

}

