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
 * ****************************************************************************
 */

package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.advisor.ProfiledQuery;
import edu.ucsc.dbtune.advisor.IndexPartitions;
import edu.ucsc.dbtune.advisor.WorkFunctionAlgorithm;
import edu.ucsc.dbtune.advisor.WfaTrace;
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.util.Debug;

import edu.ucsc.satuning.WFALog;
import edu.ucsc.satuning.offline.OfflineAnalysis;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.BitSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

/**
 * Functional test for the WFIT use case.
 *
 * Some of the documentation contained in this class refers to sections of Karl Schnaitter's 
 * Doctoral Thesis "On-line Index Selection for Physical Database Tuning". Specifically to chapter 
 * 6.
 *
 * 
 *
 * @see <a href="http://proquest.umi.com/pqdlink?did=2171968721&Fmt=7&clientId=1565&RQT=309&VName=PQD">
 *     "On-line Index Selection for Physical Database Tuning"</a>
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class WFITTestFunctional
{
    public final static Environment env = Environment.getInstance();

    /**
     */
    @BeforeClass
    public static void setUp() throws Exception
    {
    }

    /**
     */
    @AfterClass
    public static void tearDown() throws Exception
    {
    }

    /**
     * The test has to check first just one query and one index. We need to make sure that the index 
     * gets recommended at some point and that it is consistent always with what we expect.
     *
     * Then with 2 indexes. We'll have the following scenarios:
     *
     *  * keep both in the same partition:
     *     * 4 states
     *  * keep in different partitions:
     *     * 2 states
     *     * 2 WFIT (one per each partition)
     */
    @Test
    public void testWFIT() throws Exception
    {
        List<ProfiledQuery<DBIndex>>   qinfos;
        CandidatePool<DBIndex>         pool;
        Snapshot<DBIndex>              snapshot;
        IndexPartitions<DBIndex>       parts;
        WorkFunctionAlgorithm<DBIndex> wfa;
        WfaTrace<DBIndex>              trace;
        WFALog                         log;
        File                           logFile;
        File                           wfFile;

        BitSet[] wfitSchedule;
        BitSet[] optSchedule;
        BitSet[] minSched;
        double[] overheads;
        double[] minWfValues;
        int      maxNumIndexes;
        int      maxNumStates;
        int      queryCount;
        boolean  keepHistory;

        Debug.println("read " + queryCount + " queries");
    
        maxNumIndexes = env.getMaxNumIndexes();
        maxNumStates  = env.getMaxNumStates();
        keepHistory   = env.getWFAKeepHistory();
        pool          = readCandidatePool();
        qinfos        = readProfiledQueries();
        queryCount    = qinfos.size();
        overheads     = new double[queryCount];
        wfitSchedule  = new BitSet[queryCount];
        snapshot      = pool.getSnapshot();
        parts         = OfflineAnalysis.getPartition(snapshot, qinfos, maxNumIndexes, maxNumStates);
        wfa           = new WorkFunctionAlgorithm<DBIndex>(parts, keepHistory);
            
        getRecs(qinfos, wfa, wfitSchedule, overheads);
        
        if (exportWfit) {
            log     = WFALog.generateFixed(qinfos, wfitSchedule, snapshot, parts, overheads);
            logFile = Mode.WFIT.logFile();

            writeLog(logFile, log, snapshot);
            processLog(logFile);

            Debug.println();
            Debug.println("wrote log to " + logFile);

            log.dump();

            for (I index: snapshot) {
                System.out.println(index.creationText());
            }
        }
        
        if (keepHistory) {
            trace = wfa.getTrace();

            optSchedule = trace.optimalSchedule(parts, qinfos.size(), qinfos);
            log         = WFALog.generateFixed(qinfos, optSchedule, snapshot, // say zero overhead
                                               parts, new double[queryCount]);
            logFile     = Mode.OPT.logFile();

            writeLog(logFile, log, snapshot);
            processLog(logFile);

            Debug.println();
            Debug.println("wrote log to " + logFile);

            log.dump();
            
            // write min wf values
            minWfValues = new double[qinfos.size()+1];

            for (int q = 0; q <= qinfos.size(); q++) {
                minSched       = trace.optimalSchedule(parts, q, qinfos);
                minWfValues[q] = wfa.getScheduleCost(snapshot, q, qinfos, parts, minSched);

                Debug.println("Optimal cost " + q + " = " + minWfValues[q]);

//              for (int i = 0; i < q; i++) {
//                  Debug.println(minSched[i]);
//              }
//              Debug.println();
            }

            wfFile = Configuration.minWfFile();

            Files.writeObjectToFile(wfFile, minWfValues);

            processWfFile(wfFile);
        }
    }

	private CandidatePool<DBIndex> readCandidatePool()
        throws IOException, ClassNotFoundException
    {
        String            workload = env.getWorkloadFolder() + "/" + env.getWorkloadName();
        File              file     = new File(workload + "/" + env.getCandidatePoolFilename());
        ObjectInputStream in       = new ObjectInputStream(new FileInputStream(file));

		try {
			return (CandidatePool<DBIndex>) in.readObject();
		} finally {
			in.close(); // closes underlying stream
		}
	}

	private void getRecs(List<ProfiledQuery<DBIndex>>   qinfos,
                        WorkFunctionAlgorithm<DBIndex> wfa,
                        BitSet[]                       recs,
                        double[]                       overheads)
    {
		for (int q = 0; q < recs.length; q++) {

			ProfiledQuery<DBIndex> query = qinfos.get(q);

            // Debug.println("issuing query: " + query.sql);
				
			// analyze the query and get the recommendation
			long uStart = System.nanoTime();

			wfa.newTask(query);
			Iterable<DBIndex> rec = wfa.getRecommendation();

			long uEnd = System.nanoTime();
			
			recs[q] = new BitSet();

			for (DBIndex idx : rec) {
				recs[q].set(idx.internalId());
			}

			overheads[q] = (uEnd - uStart) / env.getOverheadFactor();
		}
	}

	public ArrayList<ProfiledQuery<DBIndex>> readProfiledQueries()
        throws IOException, ClassNotFoundException
    {
		File file = null; // TODO Configuration.mode.profiledQueryFile();
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
		Debug.println("Reading profiled queries from " + file);
		try {
			return (ArrayList<ProfiledQuery<DBIndex>>) in.readObject();
		} finally {
			in.close(); // closes underlying stream
		}
	}
}
