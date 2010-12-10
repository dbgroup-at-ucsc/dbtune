package edu.ucsc.dbtune.util;

import edu.ucsc.dbtune.core.*;
import edu.ucsc.dbtune.core.metadata.DB2Index;
import edu.ucsc.dbtune.core.metadata.PGIndex;
import edu.ucsc.dbtune.core.optimizers.WhatIfOptimizationBuilder;
import edu.ucsc.dbtune.core.optimizers.WhatIfOptimizationCostBuilder;
import org.junit.Test;

import java.sql.SQLException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.caliper.internal.guava.base.Preconditions.checkArgument;
import static edu.ucsc.dbtune.util.Instances.newAtomicReference;
import static edu.ucsc.dbtune.util.Instances.newFalseBoolean;
import static edu.ucsc.dbtune.util.Objects.as;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
@SuppressWarnings({"ALL"})
public class TypeReferenceTest {

    @Test(expected = IllegalArgumentException.class)
    public void testBasicArrayListExample() throws Exception {
        // Consider GuardingList<T>, which enforces type safety at runtime...
        final List<String> o = new GuardingList<String>(new TypeReference<String>(){});
        o.add("Yeah!");
        final List raw = as(o);
        raw.add(1234);  // it should throw an illegal argument exception 
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDealingWithRawTypes() throws Exception {
        final Dancing salsa = salsaDancingToRaw();
        final Dancing tango = tangoDancingToRaw();
        assertNotSame(tango, salsa);
        salsa.mix(tango);
    }


    // NO GENERICS!!! Type safety is enforced at runtime; OH YEAH! We just punched ERASURE in the face!!
    @Test(expected = IllegalArgumentException.class)
    public void testRawWhatIfOptimizer() throws Exception {
        DB2Index d = DBTuneInstances.newDB2Index();
        final AbbreviatedWhatIfOptimizer a = db2WhatIfOptimizer();
        // this should work as expected
        a.estimateCost("SELECT * FROM R;", new BitSet(), d, new BitSet());
        // this one should fail since TypeReference knows we are dealing with <DBTune<DB2Index>>
        // and not <DBTune<PGIndex>>
        PGIndex c = DBTuneInstances.newPGIndex();
        a.estimateCost("SELECT * FROM S;", new BitSet(), c, new BitSet());
    }
    
    @Test
    public void testGenericWhatIfOptimizer() throws Exception {
        final AbbreviatedWhatIfOptimizer<DB2Index> a = db2WhatIfOptimizer();
        final Double cost = a.estimateCost("Select * FROM R;", new BitSet(), DBTuneInstances.newDB2Index(), new BitSet());
        assertTrue(Double.compare(cost, 10.34) == 0);
    }

    static AbbreviatedWhatIfOptimizer db2WhatIfOptimizer(){
        return new MockWhatIfOptimizerImpl<DB2Index>(new TypeReference<DB2Index>(){});
    }

    static AbbreviatedWhatIfOptimizer pgWhatIfOptimizer(){
        return new MockWhatIfOptimizerImpl<PGIndex>(new TypeReference<PGIndex>(){});
    }

    interface AbbreviatedWhatIfOptimizer <I extends DBIndex<I>> extends DatabaseWhatIfOptimizer<I>{
        double estimateCost(String sql, BitSet a, BitSet b) throws SQLException;
        double estimateCost(String sql, BitSet a, I idx, BitSet b) throws SQLException;
    }

    static class MockWhatIfOptimizerImpl<I extends DBIndex<I>> extends AbstractDatabaseWhatIfOptimizer<I> implements AbbreviatedWhatIfOptimizer <I> {
        private final TypeReference<I> ref;
        private final AtomicInteger    count;
        private WhatIfOptimizationBuilder<I> o;

        MockWhatIfOptimizerImpl(TypeReference<I> ref){
            this.ref = ref;
            count = Instances.newAtomicInteger(0);
        }

        @Override
        protected void incrementWhatIfCount() {
            count.incrementAndGet();
        }

        @Override
        public WhatIfOptimizationBuilder<I> whatIfOptimize(String sql) throws SQLException {
            final SafeWhatIfOptimizationBuilder<I> o = new SafeWhatIfOptimizationBuilder<I>(this, sql, ref);
            this.o = o;
            return o;
        }

        @Override
        public Iterable<I> getCandidateSet() {
            return null;
        }

        @Override
        public void calculateOptimizationCost() throws SQLException {
            final SafeWhatIfOptimizationBuilder<I> each = as(getWhatIfOptimizationBuilder());
            each.addCost(runWhatIfTrial(each));
        }

        @Override
        public WhatIfOptimizationBuilder<I> getWhatIfOptimizationBuilder() {
            return o;
        }

        @Override
        protected Double runWhatIfTrial(WhatIfOptimizationBuilder<I> builder) throws SQLException {
            return 10.34;
        }

        @Override
        public ExplainInfo<I> explainInfo(String sql) throws SQLException {
            return null;  //tocode
        }

        @Override
        public void fixCandidates(Iterable<I> candidateSet) throws SQLException {
            //tocode
        }

        @Override
        public int getWhatIfCount() {
            return count.get();
        }

        @Override
        public double estimateCost(String sql, BitSet a, BitSet b) throws SQLException {
            return whatIfOptimize(sql).using(new BitSet(), new BitSet()).toGetCost();
        }

        @Override
        public double estimateCost(String sql, BitSet a, I idx, BitSet b) throws SQLException {
            return whatIfOptimize(sql).using(new BitSet(), idx, new BitSet()).toGetCost();
        }
    }

    static class SafeWhatIfOptimizationBuilder<I extends DBIndex<I>>
    implements WhatIfOptimizationBuilder<I> {

    private final String sql;
    private final AtomicReference<Double> cost;

    // optional variables
    private BitSet configuration;
    private BitSet usedSet;
    private I      profiledIndex;
    private BitSet usedColumns;
    private final AbstractDatabaseWhatIfOptimizer<I> whatIfOptimizer;
    private final TypeReference<I> ref;
    private final AtomicBoolean withProfiledIndex;

    public SafeWhatIfOptimizationBuilder(
            AbstractDatabaseWhatIfOptimizer<I> whatIfOptimizer,
            String sql,
            TypeReference<I> ref
    ){
        this.whatIfOptimizer    = whatIfOptimizer;
        this.ref = ref;
        this.cost               = newAtomicReference(0.0);
        this.sql                = sql;
        this.withProfiledIndex  = newFalseBoolean();
    }

    /**
     * updates the optimization cost's value.
     * @param value
     *      new cost value.
     */
    void addCost(double value){
        cost.compareAndSet(cost.get(), value);
    }

    /**
     * @return
     *      the configuration to be used.
     */
    public BitSet getConfiguration(){
        return configuration == null ? null : configuration.clone();
    }

    /**
     * @return
     *      a profiled index.
     */
    public I getProfiledIndex(){
        return profiledIndex;
    }

    /**
     * @return {@code true} if we are dealing with profiled indexes, {@code false}
     *      otherwise.
     */
    public boolean withProfiledIndex(){
        return withProfiledIndex.get();
    }

    /**
     * @return
     *      the sql to be used as workload.
     */
    public String getSQL(){
        return sql;
    }

    public BitSet getUsedSet(){
        return usedSet == null ? null : usedSet.clone();
    }

    /**
     * @return
     *      the db columns used in the optimization.
     */
    public BitSet getUsedColumns(){
        return usedColumns == null ? null : usedColumns.clone();
    }

    @Override
    public WhatIfOptimizationCostBuilder using(BitSet config, BitSet usedSet) {
        this.withProfiledIndex.set(false);
        this.configuration = config;
        this.usedSet       = usedSet;
        return this;
    }

    @Override
    public WhatIfOptimizationCostBuilder using(BitSet config,
           I profiledIndex, BitSet usedColumns
    ) {
        if(!ref.getGenericClass().isInstance(profiledIndex)) throw new IllegalArgumentException("ERROR, lalalala");
        this.withProfiledIndex.set(true);
        this.configuration = config;
        this.profiledIndex = profiledIndex;
        this.usedColumns   = usedColumns;
        return this;
    }


    @Override
    public Double toGetCost() throws SQLException{
        if(Double.compare(0.0, cost.get()) == 0){
            whatIfOptimizer.calculateOptimizationCost();
        }
        return cost.get();
    }
}


    static Dancing salsaDancingToRaw(){
        return new SalsaDancing(new TypeReference<SalsaDancing>(){});
    }

    static Dancing tangoDancingToRaw(){
        return new TangoDancing(new TypeReference<TangoDancing>(){});
    }


    interface Dancing <D extends Dancing<D>> {
        void mix(Dancing<D> that);
        D yeahLalala();
    }

    static class SalsaDancing implements Dancing<SalsaDancing>{
        private final TypeReference<SalsaDancing> ref;
        private Dancing<SalsaDancing> that;

        SalsaDancing(TypeReference<SalsaDancing> ref){
            this.ref = ref;
        }

        @Override
        public void mix(Dancing<SalsaDancing> that) {
            if(!ref.getGenericClass().isInstance(that)) throw new IllegalArgumentException();
            this.that = that;
        }

        @Override
        public SalsaDancing yeahLalala() {
            if(!ref.getGenericClass().isInstance(this)) throw new IllegalArgumentException();
            return this;
        }

        @Override
        public String toString() {
            return "salsa";
        }
    }

    static class TangoDancing implements Dancing<TangoDancing>{
        private final TypeReference<TangoDancing> ref;
        private Dancing<TangoDancing> that;

        TangoDancing(TypeReference<TangoDancing> ref){
            this.ref = ref;
        }

        @Override
        public void mix(Dancing<TangoDancing> that) {
            if(!ref.getGenericClass().isInstance(that)) throw new IllegalArgumentException();
            this.that = that;
        }

        @Override
        public TangoDancing yeahLalala() {
            return this;
        }

        @Override
        public String toString() {
            return "tango(" + that + ")";
        }
    }

    static class GuardingList<T> extends AbstractList<T> {
      private final TypeReference<T> elementType;
      private final ArrayList<T> delegate = new ArrayList<T>();

      GuardingList(TypeReference<T> elementType) {
        this.elementType = elementType;
      }

      public boolean add(T t) {
        checkArgument(elementType.getGenericClass().isInstance(t),
            "Cannot add %s which is not of type %s", t, elementType);
        return delegate.add(t);
      }

      public T get(int i) {
        return delegate.get(i);
      }

      public int size() {
        return delegate.size();
      }
    }
}
