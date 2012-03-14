package satuning.ibg.log;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AnalysisLog implements Iterable<AnalysisLog.Entry> {
	InteractionBank bank;
	List<Entry> list = new ArrayList<Entry>();
	
	public AnalysisLog(InteractionBank bank) {
		this.bank = bank;
	}
	
	public void add(Entry entry) {
		list.add(entry);
	}
	
	public Iterator<Entry> iterator() {
		return list.iterator();
	}
	
	public void output(PrintWriter out) throws IOException {
		// print header row first
		Entry.headerRow(bank).output(out);
		for (Entry entry : list) 
			entry.output(out);
		if (out.checkError())
			throw new IOException("could not write log");
	}
	
	public static class Entry {
		int a;
		int b;
		double doi;
		
		long millis;
		int whatifCalls;
		
		int interactingCount;
		int partitionCount;
		
		double pairError;
		double overallError;
		
		public Entry(int a, int b, double doi, 
				     long millis, int whatif, 
				     int interactingCount, int partitionCount, 
				     double pairError, double overallError) {
			this.a = a;
			this.b = b;
			this.doi = doi;
			
			this.millis = millis;
			this.whatifCalls = whatif;
			
			this.interactingCount = interactingCount;
			this.partitionCount = partitionCount;
			
			this.pairError = pairError;
			this.overallError = overallError;
		}
		
		private void output(PrintWriter out) {
			out.printf("%d\t%d\t%f\t", a, b, doi);
			out.printf("%d\t%d\t", millis, whatifCalls);
			out.printf("%d\t%d\t", interactingCount, partitionCount);
			out.printf("%f\t%f", pairError, overallError);
			out.println();
		}
		
		static Entry headerRow(InteractionBank bank) {	
			int indexCount = bank.indexCount;
			double pairCount = (indexCount*(indexCount-1)) / 2.0; // n choose 2	
			double sumRelativeError = 0;
			for (int a = 0; a < indexCount; a++)
				for (int b = 0; b < a; b++) 
					if (bank.interactionLevel(a, b) > 0)
						++sumRelativeError;
			return new Entry(-1, -1, -1, 0L, 0, 0, indexCount, -1.0, sumRelativeError/pairCount);
		}
	}
}
