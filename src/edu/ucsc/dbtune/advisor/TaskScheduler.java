package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.metadata.Index;
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
public class TaskScheduler implements Scheduler  {

    static final int UNSCHEDULED = 0;
    static final int SCHEDULED   = 1;
    static final int CANCELLED   = 2;

    private final BlockingQueue<SchedulerTask> profilingQueue;
    private final BlockingQueue<SchedulerTask> selectionQueue;
    private final BlockingQueue<SchedulerTask> completionQueue;
    private final ExecutorService                 executorService = Threads.explicitThreadPerCpuExecutor("[Task Scheduling].");

    private final WorkloadProfilerImpl profiler;
    private final CandidatesSelector   selector;

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
                new CandidatePool(),
                new CandidatesSelector(maxNumStates,maxHotSetSize,partitionIterations,indexStatisticsWindow),
                new LinkedBlockingQueue<SchedulerTask>(profilingQueueCapacity),
                new LinkedBlockingQueue<SchedulerTask>(selectionQueueCapacity),
                new LinkedBlockingQueue<SchedulerTask>(completionQueueCapacity)
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
                  CandidatePool candidatePool,  
                  CandidatesSelector selector,
                  BlockingQueue<SchedulerTask> profilingQueue,
                  BlockingQueue<SchedulerTask> selectionQueue,
                  BlockingQueue<SchedulerTask> completionQueue
    ){
        this.profilingQueue  = profilingQueue;
        this.selectionQueue  = selectionQueue;
        this.completionQueue = completionQueue;
        this.profiler        = new WorkloadProfilerImpl(connection, candidatePool, true);
        this.selector        = selector;
    }
    
    
    @Override
    public Snapshot addColdCandidate(Index index) {
        final CandidateTask task = new CandidateTask(index, this);
        Snapshot result = null;
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
    public AnalyzedQuery analyzeQuery(String sql){
        final AnalyzeTask task   = new AnalyzeTask(sql, this);
        AnalyzedQuery     result = null;
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
    public double create(Index index){
        return configChange(index, true);
    }
    
    private double configChange(Index index, boolean create){
        final ConfigurationTask task = new ConfigurationTask(index, create, this);
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
    public double drop(Index index){
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
    public double executeProfiledQuery(ProfiledQuery qinfo){
        final ExecuteTask task = new ExecuteTask(qinfo, this);
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

    WorkloadProfilerImpl getProfiler(){
        return profiler;
    }

    BlockingQueue<SchedulerTask> getProfilingQueue(){
        return profilingQueue;
    }

    BlockingQueue<SchedulerTask> getSelectionQueue(){
        return selectionQueue;
    }

    BlockingQueue<SchedulerTask> getCompletionQueue(){
        return completionQueue;
    }

    /**
     * called by main thread to get a recommendation.
     * @return 
     *      a list of recommended indexes.
     */
    @Override
    public List<Index> getRecommendation(){
        final RecommendationTask task = new RecommendationTask(this);
        List<Index> result = Instances.newList();
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
    public void negativeVote(Index index){
        try {
            getProfilingQueue().put(new VoteTask(index, false, this));
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
    public void positiveVote(Index index){
        try {
            getProfilingQueue().put(new VoteTask(index, true, this));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    @Override
    public ProfiledQuery profileQuery(String sql){
        final AnalyzeTask task   = new AnalyzeTask(sql, true, this);
        ProfiledQuery     result = null;
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
        for(SchedulerTask each : profilingQueue){
            each.cancel();
        }

        for(SchedulerTask each : selectionQueue){
            each.cancel();
        }

        for(SchedulerTask each : completionQueue){
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
    void start(SelectionThread selectionThread, ProfilingThread profilingThread){
        getProfiler().runProfiler();
        executorService.execute(selectionThread);
        executorService.execute(profilingThread);
    }
    
    @Override
    public void start() {
        start(new SelectionThread(this), new ProfilingThread(this));
    }
    
    
    private void waitForCompletion(SchedulerTask task){
        try {
            final SchedulerTask task2 = getCompletionQueue().take();
            Checks.checkAssertion(task == task2, "Error: Wrong task placed on completion queue");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }    


    static class ProfilingThread implements Runnable {
        private final TaskScheduler taskScheduler;

        ProfilingThread(TaskScheduler taskScheduler){
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
                    SchedulerTask task = taskScheduler.profilingQueue.take();
                    task.profiling();
                    stop.resetAndLog("Profiling Task finished at ");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    
    static class SelectionThread implements Runnable {
        private final TaskScheduler taskScheduler;

        SelectionThread(TaskScheduler taskScheduler){
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
                    SchedulerTask task = taskScheduler.selectionQueue.take();
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
     */
    abstract static class SchedulerTask {
        final Object lock = new Object();
        int      state    = UNSCHEDULED;
        
        final Console           console;
        final TaskScheduler  scheduler;

        SchedulerTask(TaskScheduler scheduler, Console console){
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
        
        void placeOnComplection(SchedulerTask task) throws InterruptedException {
            scheduler.getCompletionQueue().put(task);
        }
        
        void placeOnProfiling(SchedulerTask task) throws InterruptedException {
            task.state = SCHEDULED;
            scheduler.getProfilingQueue().put(task);
        }
                                              
        void placeOnSelection(SchedulerTask task) throws InterruptedException {
            task.state = SCHEDULED;
            scheduler.getSelectionQueue().put(task);
        }

        /**
         * runs the selection task.
         */
        abstract void selection();  
    }
    
    
    static class RecommendationTask extends SchedulerTask {
        private List<Index> recommendation = Instances.newList();
        RecommendationTask(TaskScheduler taskScheduler) {
            super(taskScheduler, Console.streaming());
        }
        
        List<Index> getRecommendation(){
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
    
    static class AnalyzeTask extends SchedulerTask {
        String              sql;
        boolean             profileOnly;
        ProfiledQuery    profiledInfo;
        AnalyzedQuery    analyzedInfo;
        
        AnalyzeTask(String sql, TaskScheduler taskScheduler){
            this(sql, false, taskScheduler);
        }
        
        AnalyzeTask(String sql, boolean profileOnly, TaskScheduler taskScheduler){
            this(taskScheduler);
            this.sql            = sql;
            this.profileOnly    = profileOnly;
            this.profiledInfo   = null;
            this.analyzedInfo   = null;
        }
        
        AnalyzeTask(TaskScheduler taskScheduler){
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
    
    
    static class ExecuteTask extends SchedulerTask  {
        ProfiledQuery profiledQuery;
        double cost = -1;
        
        ExecuteTask(ProfiledQuery profiledQuery, TaskScheduler taskScheduler){
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
    
    
    static class VoteTask extends SchedulerTask {
        boolean     isPositive;
        Index           index;
        Snapshot candidateSet;
        
        VoteTask(Index index, boolean isPositive, TaskScheduler taskScheduler){
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
    
    
    static class ConfigurationTask extends SchedulerTask  {
        Index       index;
        boolean isTobeMaterialized;
        double  transitionCost = -1;
        
        ConfigurationTask(Index index, boolean isTobeMaterialized, TaskScheduler taskScheduler){
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
    
    
    static class CandidateTask extends SchedulerTask  {
        Index           index;
        Snapshot candidateSet;

        CandidateTask(Index index, TaskScheduler taskScheduler){
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
