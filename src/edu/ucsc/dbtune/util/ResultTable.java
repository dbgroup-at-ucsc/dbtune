package edu.ucsc.dbtune.util;

import java.util.Vector;
import java.io.PrintStream;
import java.sql.*;

/**
 * @author Rui Wang
 */
public class ResultTable {
	int[] lengths;

	String[] headers;
	boolean noHeader = false;
	public static boolean forceHtmlAll = false;

	Vector<String[]> vector = new Vector<String[]>();

	public ResultTable(String... headers) {
		this.headers = headers;
		this.lengths = new int[headers.length];
		for (int i = 0; i < lengths.length; i++) {
			lengths[i] = headers[i].length();
		}
	}

	public ResultTable(int n) {
		this.headers = new String[n];
		this.lengths = new int[headers.length];
		for (int i = 0; i < lengths.length; i++) {
			lengths[i] = 0;
		}
		noHeader = true;
	}

	public void addRow(String... strings) {
		if (strings.length != headers.length)
			throw new Error(strings.length + " " + headers.length);
		strings = strings.clone();
		vector.add(strings);
		for (int i = 0; i < lengths.length; i++) {
			String s = strings[i];
			if (s == null)
				s = "null";
			if (s != null && s.length() > lengths[i])
				lengths[i] = s.length();
		}
	}

	private void printSep(int maxWidth) {
		PrintStream p = System.out;
		p.print("+");
		for (int len : lengths) {
			p.print("-");
			len = Math.min(len, maxWidth);
			for (int i = 0; i < len; i++)
				p.print("-");
			p.print("-+");
		}
		p.println();
	}

	public void print() {
		print(false);
	}

	public void print(boolean printNull) {
		print(printNull, Integer.MAX_VALUE);
	}

	public void print(boolean printNull, int maxWidth) {
		if (forceHtmlAll) {
			printHtml(printNull);
			return;
		}
		PrintStream p = System.out;
		printSep(maxWidth);
		if (!noHeader) {
			p.print("|");
			for (int i = 0; i < headers.length; i++) {
				String header = headers[i];
				int len = Math.min(lengths[i], maxWidth);
				p.print(" ");
				if (header.length() <= len)
					p.print(header);
				else
					p.print(header.substring(0, len - 3) + "...");
				for (int j = 0; j < len - header.length(); j++)
					p.print(" ");
				p.print(" |");
			}
			p.println();
			printSep(maxWidth);
		}
		for (String[] ss : vector) {
			p.print("|");
			for (int i = 0; i < ss.length; i++) {
				String s = ss[i];
				int len = Math.min(lengths[i], maxWidth);
				p.print(" ");
				if (s == null)
					s = printNull ? "(null)" : "";
				if (s.length() <= len)
					p.print(s);
				else
					p.print(s.substring(0, len - 3) + "...");
				for (int j = 0; j < len - s.length(); j++)
					p.print(" ");
				p.print(" |");
			}
			p.println();
		}
		printSep(maxWidth);
	}

	public void printHtml() {
		printHtml(false);
	}

	public void printHtml(boolean printNull) {
		PrintStream p = System.out;
		p.print("<table cellpadding=0 cellspaceing=0 border=1>");
		if (!noHeader) {
			p.print("<tr>");
			for (int i = 0; i < headers.length; i++) {
				String header = headers[i];
				p.print("<td>");
				p.print(header);
				p.print("</td>");
			}
			p.println("</tr>");
		}
		for (String[] ss : vector) {
			p.print("<tr>");
			for (int i = 0; i < ss.length; i++) {
				String s = ss[i];
				p.print("<td>");
				if (s == null)
					s = printNull ? "(null)" : "&nbsp;";
				p.print(s);
				p.print("</td>");
			}
			p.println("</tr>");
		}
		p.println("</table>");
	}

	public static void printResult(ResultSet rs) throws Exception {
		printResult(rs, Integer.MAX_VALUE);
	}

	public static void printResult(ResultSet rs, int len) throws Exception {
		if (rs == null)
			return;
		ResultSetMetaData rsmd = rs.getMetaData();
		String[] ss = new String[rsmd.getColumnCount()];
		for (int i = 0; i < ss.length; i++) {
			ss[i] = rsmd.getColumnName(i + 1);
		}
		ResultTable rt = new ResultTable(ss);
		int n = 0;
		while (rs.next()) {
			ss = new String[rsmd.getColumnCount()];
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				ss[i - 1] = rs.getString(i);
			}
			rt.addRow(ss);
			if (n++ > 100)
				break;
		}
		rt.print(true, len);
	}
}
