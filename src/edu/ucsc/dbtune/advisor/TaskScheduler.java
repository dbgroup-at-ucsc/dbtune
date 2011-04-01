package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.Instances;
import edu.ucsc.dbtune.util.StopWatch;
import edu.ucsc.dbtune.util.Threads;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class TaskScheduler <I extends DBIndex> implements Scheduler <I> {

    static final int UNSCHEDULED = 0;
    static final int SCHEDULED   = 1;
    static final int CANCELLED   = 2;

    private final BlockingQueue<SchedulerTask<I>> profilingQueue;
    private final BlockingQueue<SchedulerTask<I>> selectionQueue;
    private final BlockingQueue<SchedulerTask<I>> completionQueue;
    private final ExecutorService                 executorService = Threads.explicitThreadPerCpuExecutor("[Task Scheduling].");

    private final WorkloadProfilerImpl<I> profiler;
    private final CandidatesSelector<I>   selector;

    /**
     * Construct a TaskScheduler.
     * @param connection
     *      a live {@link DatabaseConnection connection}.
     */
    public TaskScheduler(
            DatabaseConnection connection,
            int profilingQueueCapacity,
            int selectionQueueCapacity,
            int completionQueueCapacity,
            int maxNumStates,
            int maxHotSetSize,
            int partitionIterations,
            int indexStatisticsWindow)
    {
        this(
                connection,
                new CandidatePool<I>(),
                new CandidatesSelector<I>(maxNumStates,maxHotSetSize,partitionIterations,indexStatisticsWindow),
                new LinkedBlockingQueue<SchedulerTask<I>>(profilingQueueCapacity),
                new LinkedBlockingQueue<SchedulerTask<I>>(selectionQueueCapacity),
                new LinkedBlockingQueue<SchedulerTask<I>>(completionQueueCapacity)
        );
    }

    /**
     * Construct a TaskScheduler.
     * @param connection
     *      a live {@link DatabaseConnection connection}.
     * @param candidatePool
     *      a {@link CandidatePool pool of candidate indexes}.
     * @param selector
     *      a {@link CandidatesSelector selector of candidate indexes}.
     * @param profilingQueue
     *      profiling blocking queue
     * @param selectionQueue
     *      selection blocking queue
     * @param completionQueue
     *      completion blocking queue
     */
    TaskScheduler(DatabaseConnection connection, 
                  CandidatePool<I> candidatePool,  
                  CandidatesSelector<I> selector,
                  BlockingQueue<SchedulerTask<I>> profilingQueue,
                  BlockingQueue<SchedulerTask<I>> selectionQueue,
                  BlockingQueue<SchedulerTask<I>> completionQueue
    ){
        this.profilingQueue  = profilingQueue;
        this.selectionQueue  = selectionQueue;
        this.completionQueue = completionQueue;
        this.profiler        = new WorkloadProfilerImpl<I>(connection, candidatePool, true);
        this.selector        = selector;
    }
    
    
    @Override
    public Snapshot<I> addColdCandidate(I index) {
        final CandidateTask<I> task = new CandidateTask<I>(index, this);
        Snapshot<I> result = null;
        try {
            getProfilingQueue().put(task);
            waitForCompletion(task);
            result = task.candidateSet;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return result;
    }
    
    @Override
    public AnalyzedQuery<I> analyzeQuery(String sql){
        final AnalyzeTask<I> task   = new AnalyzeTask<I>(sql, this);
        AnalyzedQuery<I>     result = null;
        try {
            getProfilingQueue().put(task);
            waitForCompletion(task);
            Checks.checkAssertion(task.profiledInfo != null, "Error: Need query info to do processing for index selection");
            result = task.analyzedInfo;
        } catch (InterruptedException e){
            Thread.currentThread().interrupt();
        }
        return result;
    }


    @Override
    public double create(I index){
        return configChange(index, true);
    }
    
    private double configChange(I index, boolean create){
        final ConfigurationTask<I> task = new ConfigurationTask<I>(index, create, this);
        double cost = 0.0;
        try {
            getProfilingQueue().put(task);
            waitForCompletion(task);
            cost = task.transitionCost;
        } catch (InterruptedException e){
            Thread.currentThread().interrupt();
        }
        
        return cost;   
    }
    
    @Override
    public double drop(I index){
        return configChange(index, false);
    }

    /**
     * Called by main thread to process a query
     * @param qinfo
     *      a {@code profiled query object}.
     * @return 
     *      cost of query.
     */
    @Override
    public double executeProfiledQuery(ProfiledQuery<I> qinfo){
        final ExecuteTask<I> task = new ExecuteTask<I>(qinfo, this);
        double result = 0.0;
        try {
            getProfilingQueue().put(task);
            waitForCompletion(task);
            result = task.cost;
        } catch (InterruptedException e){
            Thread.currentThread().interrupt();
        }
        
        return result;
    }

    WorkloadProfilerImpl<I> getProfiler(){
        return profiler;
    }

    BlockingQueue<SchedulerTask<I>> getProfilingQueue(){
        return profilingQueue;
    }

    BlockingQueue<SchedulerTask<I>> getSelectionQueue(){
        return selectionQueue;
    }

    BlockingQueue<SchedulerTask<I>> getCompletionQueue(){
        return completionQueue;
    }

    /**
     * called by main thread to get a recommendation.
     * @return 
     *      a list of recommended indexes.
     */
    @Override
    public List<I> getRecommendation(){
        final RecommendationTask<I> task = new RecommendationTask<I>(this);
        List<I> result = Instances.newList();
        try {
            getProfilingQueue().put(task);
            waitForCompletion(task);
            result.addAll(task.recommendation);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return result;
        }
        
        return result;
    }

    /**
     * Called by main thread to make a positive vote.
     * @param index
     *      index of interest.
     */
    @Override
    public void negativeVote(I index){
        try {
            getProfilingQueue().put(new VoteTask<I>(index, false, this));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Called by main thread to make a negative vote.
     * @param index
     *      index of interest.
     */
    @Override
    public void positiveVote(I index){
        try {
            getProfilingQueue().put(new VoteTask<I>(index, true, this));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    @Override
    public ProfiledQuery<I> profileQuery(String sql){
        final AnalyzeTask<I> task   = new AnalyzeTask<I>(sql, true, this);
        ProfiledQuery<I>     result = null;
        try {
            getProfilingQueue().put(task);
            waitForCompletion(task);
            Checks.checkAssertion(task.profiledInfo != null, "Error: Need query info.");
            result = task.profiledInfo;
        } catch (InterruptedException e){
           Thread.currentThread().interrupt();
        }
        return result;
    }    



    /**
     * shutdown the task scheduler and terminates any ongoing task.
     */
    @Override
    public void shutdown(){
        for(SchedulerTask<I> each : profilingQueue){
            each.cancel();
        }

        for(SchedulerTask<I> each : selectionQueue){
            each.cancel();
        }

        for(SchedulerTask<I> each : completionQueue){
            each.cancel();
        }

        profilingQueue.clear();
        selectionQueue.clear();
        completionQueue.clear();

        executorService.shutdownNow();
        profiler.shutdown();
    }

    /**
     * convenient method for testing.
     * @param selectionThread
     *      a {@link SelectionThread} object.
     * @param profilingThread
     *      a {@link ProfilingThread} object.
     */
    void start(SelectionThread<I> selectionThread, ProfilingThread<I> profilingThread){
        getProfiler().runProfiler();
        executorService.execute(selectionThread);
        executorService.execute(profilingThread);
    }
    
    @Override
    public void start() {
        start(new SelectionThread<I>(this), new ProfilingThread<I>(this));
    }
    
    
    private void waitForCompletion(SchedulerTask<I> task){
        try {
            final SchedulerTask<I> task2 = getCompletionQueue().take();
            Checks.checkAssertion(task == task2, "Error: Wrong task placed on completion queue");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }    


    static class ProfilingThread <I extends DBIndex> implements Runnable {
        private final TaskScheduler<I> taskScheduler;

        ProfilingThread(TaskScheduler<I> taskScheduler){
            this.taskScheduler = taskScheduler;
        }
        
        @Override
        public void run() {
            while(true){                
                try {
                    if(Thread.currentThread().isInterrupted()){
                        break;
                    }
                    StopWatch stop = new StopWatch();
                    SchedulerTask<I> task = taskScheduler.profilingQueue.take();
                    task.profiling();
                    stop.resetAndLog("Profiling Task finished at ");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    
    static class SelectionThread <I extends DBIndex> implements Runnable {
        private final TaskScheduler<I> taskScheduler;

        SelectionThread(TaskScheduler<I> taskScheduler){
            this.taskScheduler = taskScheduler;
        }
        
        @Override
        public void run() {
            while(true){                
                try {
                    if(Thread.currentThread().isInterrupted()){
                        break;
                    }
                    StopWatch stop = new StopWatch();
                    SchedulerTask<I> task = taskScheduler.selectionQueue.take();
                    task.selection();
                    stop.resetAndLog("Selection Task finished at ");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * construct a scheduler task.
     * @param <I>       
     *     bound type.
     */
    abstract static class SchedulerTask <I extends DBIndex> {
        final Object lock = new Object();
        int      state    = UNSCHEDULED;
        
        final Console           console;
        final TaskScheduler<I>  scheduler;

        SchedulerTask(TaskScheduler<I> scheduler, Console console){
            this.scheduler = scheduler;
            this.console   = console;
        }
        
        /**
         * cancels this scheduler task. This method may be called repeatedly; the second
         * and subsequent calls have no effect.
         * @return {@code true} if this task was already scheduled to run.
         */        
        boolean cancel(){
            synchronized (lock){   
                boolean result = (state == SCHEDULED);
                state = CANCELLED;
                console.info("Task got cancelled.");
                return result;              
          
            }
        }
        
        void checkLiveness(){
            Checks.checkArgument(state != CANCELLED, "Task is already cancelled.");
        }
        

        /**
         * runs profiling task.
         */
        abstract void profiling();
        
        void placeOnComplection(SchedulerTask<I> task) throws InterruptedException {
            scheduler.getCompletionQueue().put(task);
        }
        
        void placeOnProfiling(SchedulerTask<I> task) throws InterruptedException {
            task.state = SCHEDULED;
            scheduler.getProfilingQueue().put(task);
        }
                                              
        void placeOnSelection(SchedulerTask<I> task) throws InterruptedException {
            task.state = SCHEDULED;
            scheduler.getSelectionQueue().put(task);
        }

        /**
         * runs the selection task.
         */
        abstract void selection();  
    }
    
    
    static class RecommendationTask <I extends DBIndex> extends SchedulerTask<I> {
        private List<I> recommendation = Instances.newList();
        RecommendationTask(TaskScheduler <I> taskScheduler) {
            super(taskScheduler, Console.streaming());
        }
        
        List<I> getRecommendation(){
            return recommendation;
        }

        @Override
        void selection() {
            checkLiveness();
            try {
                placeOnSelection(this);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        void profiling() {
            checkLiveness();
            recommendation = scheduler.selector.getRecommendation();
            try {
                placeOnComplection(this);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
    
    static class AnalyzeTask <I extends DBIndex> extends SchedulerTask<I> {
        String              sql;
        boolean             profileOnly;
        ProfiledQuery<I>    profiledInfo;
        AnalyzedQuery<I>    analyzedInfo;
        
        AnalyzeTask(String sql, TaskScheduler<I> taskScheduler){
            this(sql, false, taskScheduler);
        }
        
        AnalyzeTask(String sql, boolean profileOnly, TaskScheduler<I> taskScheduler){
            this(taskScheduler);
            this.sql            = sql;
            this.profileOnly    = profileOnly;
            this.profiledInfo   = null;
            this.analyzedInfo   = null;
        }
        
        AnalyzeTask(TaskScheduler<I> taskScheduler){
            super(taskScheduler, Console.streaming());
        }
    

        @Override
        void profiling() {
            checkLiveness();
            try {
                profiledInfo = scheduler.getProfiler().processQuery(sql);
                placeOnSelection(this);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        void selection() {
            checkLiveness();
            Checks.checkAssertion(profiledInfo != null, "Need query info to do processing for index selection");

            try {
                if(!profileOnly){
                    analyzedInfo = scheduler.selector.analyzeQuery(profiledInfo);
                }
                placeOnComplection(this);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

        }
    }
    
    
    static class ExecuteTask <I extends DBIndex> extends SchedulerTask <I> {
        ProfiledQuery<I> profiledQuery;
        double cost = -1;
        
        ExecuteTask(ProfiledQuery<I> profiledQuery, TaskScheduler<I> taskScheduler){
            super(taskScheduler, Console.streaming());
            this.profiledQuery = profiledQuery;
        }
        
        @Override
        void profiling() {
            checkLiveness();
            try {
                placeOnSelection(this);
            } catch (InterruptedException e){
                Thread.currentThread().interrupt();
            }
        }

        @Override
        void selection() {
            checkLiveness();
            try {
                cost = scheduler.selector.currentCost(profiledQuery);
                placeOnComplection(this);
            } catch (InterruptedException e){
                Thread.currentThread().interrupt();
            }
        }
    }
    
    
    static class VoteTask <I extends DBIndex> extends SchedulerTask<I> {
        boolean     isPositive;
        I           index;
        Snapshot<I> candidateSet;
        
        VoteTask(I index, boolean isPositive, TaskScheduler<I> taskScheduler){
            super(taskScheduler, Console.streaming());
            this.index        = index;
            this.isPositive   = isPositive;
            this.candidateSet = null;
        }
        
        @Override
        void profiling() {
            checkLiveness();
            try {
                candidateSet = scheduler.getProfiler().processVote(index, isPositive);
                placeOnSelection(this);
            } catch (SQLException sqlCause){
                console.error("Could not process vote", sqlCause);
            } catch (InterruptedException interrupt){
                Thread.currentThread().interrupt();
            }
        }

        @Override
        void selection() {
            checkLiveness();
            if(isPositive){
                scheduler.selector.positiveVote(index, candidateSet);
            } else {
                scheduler.selector.negativeVote(index);
            }
        }
    }
    
    
    static class ConfigurationTask <I extends DBIndex> extends SchedulerTask <I> {
        I       index;
        boolean isTobeMaterialized;
        double  transitionCost = -1;
        
        ConfigurationTask(I index, boolean isTobeMaterialized, TaskScheduler<I> taskScheduler){
            super(taskScheduler, Console.streaming());
            this.index              = index;
            this.isTobeMaterialized = isTobeMaterialized;
        }
        
        @Override
        void profiling() {
            checkLiveness();
            try {
                placeOnSelection(this);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        void selection() {
            try {
                transitionCost = isTobeMaterialized
                        ? scheduler.selector.create(index)
                        : scheduler.selector.drop(index);
                placeOnComplection(this);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
    
    
    static class CandidateTask <I extends DBIndex> extends SchedulerTask <I> {
        I           index;
        Snapshot<I> candidateSet;

        CandidateTask(I index, TaskScheduler<I> taskScheduler){
            super(taskScheduler, Console.streaming());
            this.index = index;
        }
        
        @Override
        void profiling() {
            checkLiveness();
            try {
                candidateSet = scheduler.getProfiler().addCandidate(index);
                placeOnSelection(this);
            } catch (SQLException sqlCause){
                console.error("Could not add candidate", sqlCause);
            } catch (InterruptedException interrupt){
                Thread.currentThread().interrupt();
            }
        }

        @Override
        void selection() {
            checkLiveness();
            try {
                placeOnComplection(this);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }


}
