package interaction.workload;

import interaction.util.Files;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class SQLWorkload implements Iterable<SQLTransaction> {
	private static final long serialVersionUID = 1L;

	private List<SQLTransaction> list;

	/* singleton list */
	public SQLWorkload(SQLTransaction xact) {
		list = new ArrayList<SQLTransaction>();
		list.add(xact);
	}

	/* empty list */
	public SQLWorkload(File queryFile) throws IOException {
		list = new ArrayList<SQLTransaction>();
		loadQueries(list, queryFile);
		filterQueries(list);
	}
	
	public void createWorkloadFile(File workloadFile) throws IOException {
		PrintStream out = new PrintStream(Files.initOutputFile(workloadFile));
		try {
			for (SQLTransaction xact : list) {
				for (SQLStatement stmt : xact) {
					out.print(stmt.sql);
					out.println(';');
				}
			}
			if (out.checkError()) 
				throw new IOException("Could not write to workload file");
		} finally {
			out.close(); // closes underlying stream
		}
	}
	
	/* 
	 * get workload from a file
	 * 
	 * The file contains a list of other file. Each of the other files is
	 * an SQL file in the same directory as he queryFile
	 */
	private static void loadQueries(List<SQLTransaction> list, File queryFile) throws IOException {
		// read list of files, one file per query
		// then read each file to get the query itself	
		// each xact gets an id according to the position in the list, starting with 1
		int id = 1;
		for (String xactFileName : Files.getLines(queryFile)) {
			File xactFile = new File(queryFile.getParent(), xactFileName);
			java.util.List<String> queryLines = Files.getLines(xactFile);
			list.add(new SQLTransaction(queryLines, id));
			++id;
		}
	}
	
	private static void filterQueries(List<SQLTransaction> list) {
		ListIterator<SQLTransaction> iter = list.listIterator(); 
		while (iter.hasNext()) {
			iter.next();
//			if ((xact.id == 2 && xact.id != 8 && xact.id != 20))
//			if (xact.id != 5)
			if (false)
				iter.remove();
		}
	}

	public final int transactionCount() {
		return list.size();
	}

	public Iterator<SQLTransaction> iterator() {
		return list.iterator();
	}
}
