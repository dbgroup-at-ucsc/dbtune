package edu.ucsc.dbtune.inum.old.model;

import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: Debabrata Dash
 * Date: Feb 24, 2006
 * Time: 11:16:44 PM
 * The file is a copyright of CMU, all rights reserved.
 */
public class PermutationGenerator {
    private Object[] columns;
    private PermutationHandler callback;
    private int k;

    public PermutationGenerator(Object []columns, int k, PermutationHandler callback) {
        this.columns = new Object[columns.length];
        System.arraycopy(columns, 0, this.columns, 0, columns.length); // make a copy
        this.k = k;
        this.callback = callback;
    }

    private static void swap(Object v[], int i, int j) {
        Object t;
        t = v[i];
        v[i] = v[j];
        v[j] = t;
    }

    public void start() {
        _perm(0, k);
    }

    /* recursive function to generate permutations */
    private void _perm(int i, int k) {
        int j;
        //System.out.println("n = " + n + ", i = " + i + " v = " + Arrays.toString(v));

        /* if we are at the end of the array, we have one permutation
       * we can use (here we print it; you could as easily hand the
       * array off to some other function that uses it for something
       */
        if (i == k) {
            Object output[] = new Object[k];
            System.arraycopy(columns, 0, output, 0, k);
            callback.handle(output);
        } else
            /* recursively explore the permutations starting
            * at index i going through index n-1
            */
            for (j = i; j < columns.length; j++) {

                /* try the array with i and j switched */

                swap(columns, i, j);
                _perm(i + 1, k);
                /* swap them back the way they were */

                swap(columns, i, j);
            }
    }

    public static void main(String[] args) {
        PermutationGenerator gen = new PermutationGenerator(new String[]{"a", "b", "c", "d"}, 2, new PermutationHandler() {
            public void handle(Object []columns) {
                System.out.println(Arrays.toString(columns));
            }
        });
        gen.start();
    }
}
