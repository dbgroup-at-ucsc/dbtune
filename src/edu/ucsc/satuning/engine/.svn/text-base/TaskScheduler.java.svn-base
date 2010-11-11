package edu.ucsc.satuning.engine;

import java.sql.SQLException;
import java.util.List;

import edu.ucsc.satuning.Configuration;
import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.db.DatabaseConnection;
import edu.ucsc.satuning.engine.selection.Selector;
import edu.ucsc.satuning.engine.CandidatePool.Snapshot;
import edu.ucsc.satuning.engine.profiling.Profiler;
import edu.ucsc.satuning.util.BlockingQueue;
import edu.ucsc.satuning.util.Debug;

public class TaskScheduler<I extends DBIndex<I>> {
	// XXX: the profiling queue is blocking -- this would not be good in a non-experimental environment
	private BlockingQueue<Task> profilingQueue = new BlockingQueue<Task>(Configuration.queryQueueCapacity);
	private BlockingQueue<Task> selectionQueue = new BlockingQueue<Task>(Configuration.queryQueueCapacity);
	private BlockingQueue<Task> completionQueue = new BlockingQueue<Task>(1);
	
	private CandidatePool<I> candPool;
	private Selector<I> selector;
	private Profiler<I> profiler;
	private Thread selectionThread;
	private Thread profilingThread;
	
	public TaskScheduler(DatabaseConnection<I> conn) {
		candPool = new CandidatePool<I>();
		selector = new Selector<I>();
		selectionThread = new Thread(new SelectionThread());
		selectionThread.start();
		profiler = new Profiler<I>(conn, candPool, true);
		profilingThread = new Thread(new ProfilingThread());
		profilingThread.start();
	}
	
	class ProfilingThread implements Runnable {
		public void run() {
			while (true) {
				Task task = profilingQueue.get();
				task.doProfilingSteps();
			}
		}
	}
	
	class SelectionThread implements Runnable {
		public void run() {
			while (true) {
				Task task = selectionQueue.get();
				task.doSelectionSteps();
			}
		}
	}
	
	private interface Task {
		public void doProfilingSteps();
		public void doSelectionSteps();
	}
	
	private void waitForCompletion(Task task) {
		Task task2 = completionQueue.get();
		Debug.assertion(task == task2, "wrong task placed on completion queue");
	}
	
	/*
	 * Called by main thread to get a recommendation
	 */
	public List<I> getRecommendation() {
		RecommendationTask task = new RecommendationTask();
		profilingQueue.put(task); // put on profilingQueue so that all pending profiling tasks will finish
		waitForCompletion(task);
		return task.recommendation;
	}
	
	/*
	 * RecommendationTask class
	 */
	private class RecommendationTask implements Task {
		public java.util.List<I> recommendation = null;

		public void doProfilingSteps() {
			selectionQueue.put(this);
		}

		public void doSelectionSteps() {
			recommendation = selector.getRecommendation();
			completionQueue.put(this);
		}
	}
	
	/*
	 * Called by main thread to process a query
	 */
	public AnalyzedQuery<I> analyzeQuery(String sql) {
		AnalyzeTask task = new AnalyzeTask(sql);
		profilingQueue.put(task);
		waitForCompletion(task);
		Debug.assertion(task.qinfo2 != null, "Need query info to do processing for index selection");
		return task.qinfo2;
	}
	
	/*
	 * AnalyzeTask class
	 */
	private class AnalyzeTask implements Task {
		String sql;
		boolean profileOnly;
		ProfiledQuery<I> qinfo;
		AnalyzedQuery<I> qinfo2;
		
		AnalyzeTask(String sql0, boolean profileOnly0) {
			sql = sql0;
			profileOnly = profileOnly0;
			qinfo = null;
			qinfo2 = null;
		}
		
		AnalyzeTask(String sql) {
			this(sql, false);
		}

		public void doProfilingSteps() {
			qinfo = profiler.processQuery(sql);
			selectionQueue.put(this);
		}

		public void doSelectionSteps() {
			if (qinfo == null)
				throw new AssertionError("Need query info to do processing for index selection");
			if (!profileOnly)
				qinfo2 = selector.analyzeQuery(qinfo);
			completionQueue.put(this);
		}
	}

	/*
	 * Called by main thread to profile a query
	 * This is like the "analyzeQuery" method, but the second step of index selection is skipped
	 */
	public ProfiledQuery<I> profileQuery(String sql) {
		AnalyzeTask task = new AnalyzeTask(sql, true);
		profilingQueue.put(task);
		waitForCompletion(task);
		Debug.assertion(task.qinfo != null, "Need query info");
		return task.qinfo;
	}
	
	/*
	 * Called by main thread to process a query
	 */
	public double executeProfiledQuery(ProfiledQuery<I> qinfo) {
		ExecuteTask task = new ExecuteTask(qinfo);
		profilingQueue.put(task);
		waitForCompletion(task);
		return task.cost;
	}
	
	/*
	 * ExecuteTask class
	 */
	private class ExecuteTask implements Task {
		ProfiledQuery<I> qinfo;
		double cost = -1;
		
		ExecuteTask(ProfiledQuery<I> qinfo0) {
			qinfo = qinfo0;
		}

		public void doProfilingSteps() {
			selectionQueue.put(this);
		}

		public void doSelectionSteps() {
			cost = selector.currentCost(qinfo);
			completionQueue.put(this);
		}
	}
	
	/*
	 * Called by main thread to make a positive vote
	 */
	public void negativeVote(I index) {
		profilingQueue.put(new VoteTask(index, false));
	}
	
	/*
	 * Called by main thread to make a negative vote
	 */
	public void positiveVote(I index) {
		profilingQueue.put(new VoteTask(index, true));
	}
	
	/*
	 * VoteTask class
	 */
	private class VoteTask implements Task {
		private boolean isPositive;
		private I index;
		private Snapshot<I> candSet;
		
		VoteTask(I index0, boolean isPositive0) {
			index = index0;
			isPositive = isPositive0;
		}

		public void doProfilingSteps() {
			try {
				candSet = profiler.processVote(index, isPositive);
				selectionQueue.put(this);
			} catch (SQLException e) {
				Debug.logError("Could not process vote", e);
			}
		}

		public void doSelectionSteps() {
			if (isPositive) 
				selector.positiveVote(index, candSet);
			else
				selector.negativeVote(index);
		}
	}

	public double create(I index) {
		return configChange(index, true);
	}
	
	public double drop(I index) {
		return configChange(index, false);
	}

	private double configChange(I index, boolean create) {
		ConfigurationTask task = new ConfigurationTask(index, create);
		profilingQueue.put(task);
		waitForCompletion(task);
		return task.transitionCost;
	}
	
	private class ConfigurationTask implements Task {
		I index;
		boolean willBeMaterialized;
		double transitionCost = -1;
		
		ConfigurationTask(I index0, boolean mat) {
			index = index0;
			willBeMaterialized = mat;
		}

		public void doProfilingSteps() {
			selectionQueue.put(this);
		}

		public void doSelectionSteps() {
			transitionCost = (willBeMaterialized) ? selector.create(index) : selector.drop(index);
			completionQueue.put(this);
		}
	}

	public Snapshot<I> addColdCandidate(I index) {
		CandidateTask task = new CandidateTask(index);
		profilingQueue.put(task);
		waitForCompletion(task);
		return task.candidateSet;
	}
	
	private class CandidateTask implements Task {
		I index;
		Snapshot<I> candidateSet = null;
		
		CandidateTask(I index0) {
			index = index0;
		}
		
		public void doProfilingSteps() {
			try {
				candidateSet = profiler.addCandidate(index);
			} catch (SQLException e) {
				Debug.logError("Could not add condidate", e);
			}
			selectionQueue.put(this);
		}
		
		public void doSelectionSteps() {
			completionQueue.put(this);
		}
	}
}
