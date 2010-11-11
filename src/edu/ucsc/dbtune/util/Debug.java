package edu.ucsc.dbtune.util;

public class Debug {
	public static void println() {
		System.out.println();
	}
	
	public static void println(Object obj) {
		System.out.println(obj.toString());
	}

	public static void print(Object obj) {
		System.out.print(obj.toString());
	}
	
	public static void logError(String info, Exception e) {
		System.err.print(info);
		System.err.print(": ");
		System.err.println(e.getMessage());
		e.printStackTrace();
	}

	public static void logError(String info) {
		System.err.println("ERROR: " + info);
	}

	public static void logNotice(String info) {
		System.err.println("NOTICE: " + info);
	}

    public static void logNotice(String info, Throwable cause){
        logNotice(info + " Cause: " + cause.toString());
    }

	public static void assertion(boolean b, String string) {
		if (!b) throw new AssertionError(string);
	}
}
