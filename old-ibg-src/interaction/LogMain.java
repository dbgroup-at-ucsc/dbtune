package interaction;

import static interaction.cand.Generation.Strategy.*;
import interaction.cand.Generation;
import interaction.ibg.*;
import interaction.ibg.log.AnalysisLog;
import interaction.ibg.log.BasicLog;
import interaction.ibg.serial.SerialIndexBenefitGraph;
import interaction.util.Files;
import static interaction.ibg.AnalysisMode.PARALLEL;
import static interaction.ibg.AnalysisMode.SERIAL;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

public class LogMain {
	public static void main(String[] args) {
		// process arguments
		if (args.length > 0) {
			System.out.println("No arguments are accepted");
		}
		
		// do the steps
		try {
			runSteps();
		} catch (Throwable t) {	
			t.printStackTrace();
		} finally { 	
			// ensures that all threads exit
			System.exit(0);
		}
	}

	private static void runSteps() throws IOException, ClassNotFoundException {
		Generation.Strategy[] strategies = new Generation.Strategy[] {
//				UNION_OPTIMAL,
//				FULL_BUDGET,
//				HALF_BUDGET,
				OPTIMAL_1C
		};
		AnalysisMode[] modes = new AnalysisMode[] {
				SERIAL,
				PARALLEL
		};
		double[] thresholds = new double[] {
				0.01,
				0.1,
				1.0
		};
		
		for (Generation.Strategy s : strategies) {
			File ibgFile = Configuration.ibgFile(s);
			SerialIndexBenefitGraph[] ibgs = (SerialIndexBenefitGraph[]) Files.readObjectFromFile(ibgFile);
			for (int i = 0; i < ibgs.length; i++) {
				System.out.println("Query "+(i+1)+" used set = " + ibgs[i].usedSetToString());
			}
			for (AnalysisMode m : modes) {
				File analysisFile = Configuration.analysisFile(s, m);
				BasicLog basicLog = (BasicLog) Files.readObjectFromFile(analysisFile);
				for (double t : thresholds) {
					File logFile = Configuration.logFile(s, m, t);
					AnalysisLog log = basicLog.getAnalysisLog(t);
					
					OutputStream out = Files.initOutputFile(logFile);
					PrintWriter writer = new PrintWriter(out);
					try {
						log.output(writer);
					} finally {
						writer.close();
						out.close();
					}
				}
			}
		}
	}
}
