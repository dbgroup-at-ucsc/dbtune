package edu.ucsc.dbtune.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public abstract class Files {

	public static String readFile(File file) 
	throws IOException {
		InputStream stream = new FileInputStream(file);
		try { return readStream(stream); } 
		finally { stream.close(); }
	}

	public static String readStream(InputStream stream) 
	throws IOException {
		StringBuffer sb = new StringBuffer();
		for (int c  = stream.read(); c != -1; c = stream.read()) {
			sb.append((char) c);
		}	
		return sb.toString();
	}
	
	public static List<String> getLines(File file) 
	throws IOException {
		InputStream stream = new FileInputStream(file);
		try { return getLines(stream); }
		finally { stream.close(); }
	}
	
	public static List<String> getLines(InputStream stream) 
	throws IOException {
		List<String> list;
		StringBuffer sbuf;
		
		list = new ArrayList<String>();
		sbuf = new StringBuffer();

		for (int c  = stream.read(); c != -1; c = stream.read()) {
			switch (c) {
				case '\n':
				case '\r':
					String s = sbuf.toString().trim();
					if (s.length() > 0)
						list.add(s);
					sbuf.setLength(0);
					break;
				case ' ':
				case '\t':
					if (sbuf.length() > 0)
						sbuf.append((char) c);
					break;
				default:
					sbuf.append((char) c);
			}
		}

		/* get the last line */
		String s = sbuf.toString().trim();
		if (s.length() > 0) 
			list.add(s);

		return list;
	}

	public static OutputStream initOutputFile(File file) throws IOException {
		file.getParentFile().mkdirs();
		OutputStream stream = new FileOutputStream(file);
		return stream;
	}
	
	public static Object readObjectFromFile(File file) throws IOException, ClassNotFoundException {
		ObjectInputStream stream = new ObjectInputStream(new FileInputStream(file));
		try {
			return stream.readObject();
		} finally {
			stream.close(); // closes underlying stream
		}
	}

	public static void writeObjectToFile(File file, Object o) throws IOException {
		ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(file));
		try {
			out.writeObject(o);
		} finally {
			out.close();
		}
	}
}
