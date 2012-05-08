package edu.ucsc.dbtune.util;

import java.math.BigInteger;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.collect.Iterables.get;

/**
 * @author Ivo Jimenez
 */
public final class MathUtils
{
    private static final double EPSILON = 0.001;

    /**
     */
    private MathUtils()
    {
    }

    /**
     * Returns true if two doubles are considered equal.  Tests if the absolute
     * difference between two doubles has a difference less then {@link #EPSILON}.
     *
     * @param a
     *      double to compare.
     * @param b
     *      double to compare.
     * @return
     *      {@code true} if two doubles are considered equal.
     */
    public static boolean equals(double a, double b)
    {
        return a == b ? true : Math.abs(a - b) < EPSILON;
    }
    
    /**
     * @param <T>
     *      the type of element
     * @param elements
     *      elements to be combined
     * @param n
     *      the parameter of the choose operation
     * @return
     *      a list containing
     */
    public static <T> Iterable<Set<T>> combinations(Collection<T> elements, int n)
    {
        if (n > elements.size())
            throw new IllegalArgumentException(
                    "n should be greater or equal than " + elements.size());

        return new MathUtils.CombinationIterator<T>(elements, n);
    }

    /**
     * An iterator of sets of a combination.
     *
     * @author Ivo Jimenez
     */
    private static class CombinationIterator<E> implements Iterator<Set<E>>, Iterable<Set<E>>
    {
        private Collection<E> combinedCollection;
        private CombinationGenerator combinations;

        /**
         * @param combinedCollection
         *      collection that is being combined
         * @param n
         *      number of elements that each iterated set has
         */
        public CombinationIterator(Collection<E> combinedCollection, int n)
        {
            this.combinedCollection = combinedCollection;
            
            combinations = new CombinationGenerator(combinedCollection.size(), n);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Set<E> next()
        {
            Set<E> combination = new HashSet<E>();

            for (Integer i : combinations.getNext())
                combination.add(get(combinedCollection, i));

            return combination;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("Can't remove");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Iterator<Set<E>> iterator()
        {
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext()
        {
            return combinations.hasMore();
        }
    }

    //CHECKSTYLE:OFF
    //
    // taken from: http://www.merriampark.com/comb.htm
    //
    //--------------------------------------
    // Systematically generate combinations.
    //--------------------------------------

    public static class CombinationGenerator
    {
        private int[] a;
        private int n;
        private int r;
        private BigInteger numLeft;
        private BigInteger total;

        //------------
        // Constructor
        //------------

        public CombinationGenerator (int n, int r) {
            if (r > n) {
                throw new IllegalArgumentException ();
            }
            if (n < 1) {
                throw new IllegalArgumentException ();
            }
            this.n = n;
            this.r = r;
            a = new int[r];
            BigInteger nFact = getFactorial (n);
            BigInteger rFact = getFactorial (r);
            BigInteger nminusrFact = getFactorial (n - r);
            total = nFact.divide (rFact.multiply (nminusrFact));
            reset ();
        }

        //------
        // Reset
        //------

        public void reset () {
            for (int i = 0; i < a.length; i++) {
                a[i] = i;
            }
            numLeft = new BigInteger (total.toString ());
        }

        //------------------------------------------------
        // Return number of combinations not yet generated
        //------------------------------------------------

        public BigInteger getNumLeft () {
            return numLeft;
        }

        //-----------------------------
        // Are there more combinations?
        //-----------------------------

        public boolean hasMore () {
            return numLeft.compareTo (BigInteger.ZERO) == 1;
        }

        //------------------------------------
        // Return total number of combinations
        //------------------------------------

        public BigInteger getTotal () {
            return total;
        }

        //------------------
        // Compute factorial
        //------------------

        private static BigInteger getFactorial (int n) {
            BigInteger fact = BigInteger.ONE;
            for (int i = n; i > 1; i--) {
                fact = fact.multiply (new BigInteger (Integer.toString (i)));
            }
            return fact;
        }

        //--------------------------------------------------------
        // Generate next combination (algorithm from Rosen p. 286)
        //--------------------------------------------------------

        public int[] getNext () {

            if (numLeft.equals (total)) {
                numLeft = numLeft.subtract (BigInteger.ONE);
                return a;
            }

            int i = r - 1;
            while (a[i] == n - r + i) {
                i--;
            }
            a[i] = a[i] + 1;
            for (int j = i + 1; j < r; j++) {
                a[j] = a[i] + j - i;
            }

            numLeft = numLeft.subtract (BigInteger.ONE);
            return a;

        }
    }
    //CHECKSTYLE:ON
}
