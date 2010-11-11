package edu.ucsc.satuning;

public class Main {	
	public static void main(String[] args) {
		try {
			Configuration.processArgs(args);
			if (Configuration.logging) {
				Configuration.dbms.runLogging(Configuration.mode);
			}
			else { 
				Configuration.dbms.run(Configuration.mode);
			}
		} catch (Throwable t) {
			System.err.print("Uncaught: ");
			System.err.println(t);
			t.printStackTrace();
			System.exit(1);
		}
		System.exit(0);
	}
}
