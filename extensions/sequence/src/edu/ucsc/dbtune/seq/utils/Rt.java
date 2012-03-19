package edu.ucsc.dbtune.seq.utils;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Frame;
import java.awt.Graphics;
import java.awt.GraphicsEnvironment;
import java.awt.Point;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.LookAndFeel;
import javax.swing.UIDefaults;
import javax.swing.UIManager;
import javax.xml.transform.TransformerException;


/**
 * @author Rui Wang
 */
public class Rt {
	public static String charSet;

	public static void init() {
		if ("amd64".equals(System.getProperty("os.arch")))
			Rt.addJavaLibraryPath("lib/java3d/amd64");
		else
			Rt.addJavaLibraryPath("lib/java3d/i386");
	}

	public static boolean showDate = false;

	public static String getInfo() {
		StackTraceElement[] elements = Thread.currentThread().getStackTrace();
		String line = "(" + elements[3].getFileName() + ":"
				+ elements[3].getLineNumber() + ")";
		if (showDate)
			return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
					.format(new Date())
					+ " " + line;
		else
			return line;
	}

	private static SimpleDateFormat df = new SimpleDateFormat("yyMMdd HH:mm:ss");

	public static String getDate() {
		synchronized (df) {
			return df.format(new Date());
		}
	}

	public static String getDate(long time) {
		synchronized (df) {
			return df.format(new Date(time));
		}
	}

	public static String getInfoWithDate() {
		StackTraceElement[] elements = Thread.currentThread().getStackTrace();
		return getDate() + " (" + elements[3].getFileName() + ":"
				+ elements[3].getLineNumber() + ")";
	}

	public static String getClassInfoOutof(Class c) {
		StackTraceElement[] elements = Thread.currentThread().getStackTrace();
		int pos = 3;
		String filename = c.getSimpleName() + ".java";
		for (; pos < elements.length; pos++) {
			if (!elements[pos].getFileName().equals(filename))
				break;
		}
		if (pos == elements.length)
			pos--;
		return "(" + elements[pos].getFileName() + ":"
				+ elements[pos].getLineNumber() + ")";
	}

	public static String getClassInfoOutofPrefix(String prefix) {
		StackTraceElement[] elements = Thread.currentThread().getStackTrace();
		int pos = 3;
		for (; pos < elements.length; pos++) {
			if (!elements[pos].getFileName().startsWith(prefix))
				break;
		}
		if (pos == elements.length)
			pos--;
		return "(" + elements[pos].getFileName() + ":"
				+ elements[pos].getLineNumber() + ")";
	}

	public static String getInfoOutof(Class c) {
		SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
		StackTraceElement[] elements = Thread.currentThread().getStackTrace();
		int pos = 3;
		String filename = c.getSimpleName() + ".java";
		for (; pos < elements.length; pos++) {
			if (!elements[pos].getFileName().equals(filename))
				break;
		}
		if (pos == elements.length)
			pos--;
		return df.format(new Date()) + " (" + elements[pos].getFileName() + ":"
				+ elements[pos].getLineNumber() + ")";
	}

	public static void sleep(int milliseconds) {
		try {
			Thread.sleep(milliseconds);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void p() {
		// Print current
		System.out.println(getInfo());
	}

	public static void np() {
		System.out.println();
	}

	public static void np(String format) {
		System.out.println(format);
	}

	public static void np(Object format) {
		System.out.println(format);
	}

	public static void npn(String format) {
		System.out.print(format);
	}

	public static void p(String format) {
		System.out.println(getInfo() + ": " + format);
	}

	public static void pDate(String format) {
		System.out.println(getInfoWithDate() + ": " + format);
	}

	public static void p(byte[] bs) {
		System.out.println(getInfo() + ": " + bytesToHexString(bs));
	}

	public static String getBits(byte[] bs) {
		StringBuilder sb = new StringBuilder();
		for (byte b : bs) {
			for (int i = 7; i >= 0; i--) {
				if (((b >>> i) & 1) == 1)
					sb.append('1');
				else
					sb.append('0');
			}
			sb.append(" ");
		}
		return sb.toString();
	}

	public static String getBitsRev(byte[] bs) {
		StringBuilder sb = new StringBuilder();
		for (byte b : bs) {
			for (int i = 0; i < 8; i++) {
				if (((b >>> i) & 1) == 1)
					sb.append('1');
				else
					sb.append('0');
			}
			sb.append(" ");
		}
		return sb.toString();
	}

	public static void pBits(byte[] bs) {
		System.out.println(getInfo() + ": " + getBits(bs));
	}

	public static void p(byte[] bs, int len) {
		System.out.println(getInfo() + ": " + bytesToHexString(bs, len));
	}

	public static void pstr(byte[] bs) {
		try {
			if (charSet != null)
				System.out.println(getInfo() + ": " + new String(bs, charSet));
			else
				System.out.println(getInfo() + ": " + new String(bs));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			System.out.println(getInfo() + ": " + new String(bs));
		}
	}

	public static void error(Object format) {
		System.err.println(getInfo() + ": " + format);
	}

	public static void error(String format) {
		System.err.println(getInfo() + ": " + format);
	}

	public static void p(int format) {
		System.out.println(getInfo() + ": " + format);
	}

	public static void p(Object format) {
		synchronized (System.out) {
			StackTraceElement[] elements = Thread.currentThread()
					.getStackTrace();
			System.out.println(getInfo() + ": " + format);
		}
	}

	public static void p(String format, Object... args) {
		synchronized (System.out) {
			System.out.println(getInfo() + ": " + String.format(format, args));
		}
	}

	public static String bitStringHeader = "1098 7654 - 3210 9876 - 5432 1098 - 7654 3210";

	public static String getBitString(int t) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 32; i++) {
			if ((t & (1 << (31 - i))) != 0)
				sb.append("1");
			else
				sb.append("0");
			if (i > 0 && i < 31 && i % 4 == 3) {
				if (i % 8 == 7) {
					sb.append(" - ");
				} else {
					sb.append(" ");
				}
			}
		}
		return sb.toString();
	}

	public static void pb(int t) {
		synchronized (System.out) {
			String info = getInfo();
			System.out.println(info + ": " + bitStringHeader);
			char[] cs = info.toCharArray();
			Arrays.fill(cs, ' ');
			System.out.println(new String(cs) + "  " + getBitString(t));
		}
	}

	public static void np(String format, Object... args) {
		synchronized (System.out) {
			System.out.println(String.format(format, args));
		}
	}

	public static void error(String format, Object... args) {
		System.err.println(getInfo() + ": " + String.format(format, args));
	}

	public static void pn(String format, Object... args) {
		System.out.format(getInfo() + ": " + format, args);
	}

	public static void displayFileHex(File file, long start, int len)
			throws IOException {
		byte[] bs = Rt.readFileByte(file, start, len);
		Rt.printHex(start, bs);
	}

	public static void printHex(long address, byte[] bs, String format,
			Object... args) {
		System.out.println(getInfo() + ": " + String.format(format, args));
		System.out.println(getHex(address, bs, bs.length));
	}

	public static void printHex(long address, byte[] bs) {
		printHex(address, bs, bs.length);
	}

	public static void printHex(long address, byte[] bs, int length) {
		System.out.println(getInfoOutof(Rt.class));
		System.out.println(getHex(address, bs, length));
	}

	public static void printHex(long address, byte[] bs, int offset, int length) {
		System.out.println(getHex(address, bs, offset, length));
	}

	public static String getHex(long address, byte[] bs, int length) {
		return getHex(address, bs, 0, length);
	}

	public static String getHex(long address, byte[] bs, int offset, int length) {
		return getHex(address, bs, offset, length, false);
	}

	public static String getHex(long address, byte[] bs, int offset,
			int length, boolean simple) {
		if (length > bs.length - offset)
			length = bs.length - offset;
		int col = 16;
		int row = (length - 1) / col + 1;
		int len = 0;
		for (long t = address; t > 0; t >>= 4) {
			len++;
		}
		len++;
		StringBuilder sb = new StringBuilder();
		for (int y = 0; y < row; y++) {
			if (simple)
				sb.append(String.format("%04x ", address + y * col));
			else
				sb.append(String.format("%" + len + "x:  ", address + y * col));
			for (int x = 0; x < col; x++) {
				if (!simple && x > 0 && x % 4 == 0)
					sb.append("- ");
				int index = y * col + x;
				if (index < length)
					sb.append(String.format("%02X ", bs[offset + index]));
				else
					sb.append("   ");
			}
			if (!simple) {
				sb.append(" ");
				for (int x = 0; x < col; x++) {
					// if (x > 0 && x % 4 == 0)
					// System.out.print(" - ");
					char c;
					int index = y * col + x;
					if (index < length)
						c = (char) bs[offset + index];
					else
						c = ' ';
					if (c < 32 || c >= 127)
						c = '.';
					sb.append(c);
				}
			}
			sb.append("\n");
		}
		return sb.toString();
	}

	public static String createString(byte[] bs) throws IOException {
		return createString(bs, charSet);
	}

	public static String createString(byte[] bs, String charSet)
			throws IOException {
		return createString(bs, 0, bs.length, charSet);
	}

	public static String createString(byte[] bs, int offset, int len)
			throws IOException {
		return createString(bs, offset, len, charSet);
	}

	public static String createString(byte[] bs, int offset, int len,
			String charSet) {
		if (charSet == null)
			charSet = Rt.charSet;
		if (charSet == null)
			return new String(bs, offset, len);
		else {
			try {
				return new String(bs, offset, len, charSet);
			} catch (UnsupportedEncodingException e) {
				return new String(bs, offset, len);
			}
		}
	}

	public static byte[] getBytes(String s, String charSet) throws IOException {
		if (charSet == null)
			charSet = Rt.charSet;
		if (charSet == null)
			return s.getBytes();
		else {
			return s.getBytes(charSet);
		}
	}

	public static byte[] read(InputStream inputStream) throws IOException {
		java.io.ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		byte[] buf = new byte[1024];
		while (true) {
			int len = inputStream.read(buf);
			if (len < 0)
				break;
			outputStream.write(buf, 0, len);
		}
		return outputStream.toByteArray();
	}

	public static int fill(InputStream inputStream, byte[] bs)
			throws IOException {
		int start = 0;
		int left = bs.length;
		while (left > 0) {
			int len = inputStream.read(bs, start, left);
			if (len < 0)
				break;
			start += len;
			left -= len;
		}
		return start;
	}

	public static void read(InputStream inputStream, OutputStream outputStream)
			throws IOException {
		byte[] buf = new byte[1024];
		while (true) {
			int len = inputStream.read(buf);
			if (len < 0)
				break;
			outputStream.write(buf, 0, len);
		}
	}

	public static String readAsString(InputStream inputStream)
			throws IOException {
		return createString(read(inputStream), null);
	}

	public static String readAsString(InputStream inputStream, String codeset)
			throws IOException {
		return createString(read(inputStream), codeset);
	}

	public static URL getResource(Class c, String name) throws IOException {
		String s = c.getName().replace('.', '/');
		int t = s.lastIndexOf('/');
		s = s.substring(0, t + 1) + name;
		return c.getClassLoader().getResource(s);
	}

	public static File getResourceFile(Class c, String name) throws IOException {
		String s = c.getName().replace('.', '/');
		int t = s.lastIndexOf('/');
		s = s.substring(0, t + 1) + name;
		URL url = c.getClassLoader().getResource(s);
		if (!"file".equals(url.getProtocol()))
			throw new IOException(url.toExternalForm());
		return new File(url.getPath());
	}

	public static InputStream openResource(Class c, String name)
			throws IOException {
		String s = c.getName().replace('.', '/');
		int t = s.lastIndexOf('/');
		s = s.substring(0, t + 1) + name;
		InputStream is = c.getClassLoader().getResourceAsStream(s);
		return is;
	}

	public static String[] readAsLines(InputStream inputStream)
			throws IOException {
		return readAsLines(inputStream, charSet);
	}

	public static String[] readAsLines(InputStream inputStream, String charSet)
			throws IOException {
		return readAsLines(inputStream, Integer.MAX_VALUE, charSet);
	}

	public static String[] readAsLines(InputStream inputStream, int max,
			String charSet) throws IOException {
		java.io.BufferedInputStream stream = new BufferedInputStream(
				inputStream);
		Vector<String> lines = new Vector<String>();
		java.io.ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		RTimer timer = new RTimer();
		while (true) {
			int t = stream.read();
			if (t < 0)
				break;
			if (t == '\n') {
				byte[] bs = outputStream.toByteArray();
				int len = bs.length;
				if (bs.length > 0 && bs[bs.length - 1] == '\r')
					len--;
				lines.add(createString(bs, 0, len, charSet));
				timer.next();
				if (lines.size() >= max) {
					return lines.toArray(new String[lines.size()]);
				}
				outputStream.reset();
			} else
				outputStream.write(t);
		}
		if (outputStream.size() > 0)
			lines.add(createString(outputStream.toByteArray(), charSet));
		return lines.toArray(new String[lines.size()]);
	}

	public static String[] readAsStrings(InputStream inputStream)
			throws IOException {
		java.io.BufferedInputStream stream = new BufferedInputStream(
				inputStream);
		Vector<String> lines = new Vector<String>();
		java.io.ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		while (true) {
			int t = stream.read();
			if (t < 0)
				break;
			if (t == 0) {
				byte[] bs = outputStream.toByteArray();
				int len = bs.length;
				lines.add(createString(bs, 0, len));
				outputStream.reset();
			} else
				outputStream.write(t);
		}
		if (outputStream.size() > 0)
			lines.add(createString(outputStream.toByteArray()));
		return lines.toArray(new String[lines.size()]);
	}

	public static String[] readAsLines(byte[] bs) throws IOException {
		java.io.ByteArrayInputStream inputStream = new ByteArrayInputStream(bs);
		return readAsLines(inputStream);
	}

	public static String[] readAsStrings(byte[] bs) throws IOException {
		java.io.ByteArrayInputStream inputStream = new ByteArrayInputStream(bs);
		return readAsStrings(inputStream);
	}

	public static byte[] readResource(Class c, String name) throws IOException {
		InputStream is = openResource(c, name);
		if (is == null)
			return null;
		return read(is);
	}

	public static byte[] readResource(String name) throws Exception {
		StackTraceElement[] elements = Thread.currentThread().getStackTrace();
		String rt = Rt.class.getSimpleName() + ".java";
		for (int i = 0; i < elements.length; i++) {
			StackTraceElement e = elements[i];
			String s = e.getFileName();
			if ("Thread.java".equals(s) || rt.equals(s))
				continue;
			InputStream is = openResource(Class.forName(elements[i]
					.getClassName()), name);
			if (is == null)
				return null;
			return read(is);
		}
		return null;
	}

	public static String readResourceAsString(String name) throws Exception {
		StackTraceElement[] elements = Thread.currentThread().getStackTrace();
		String rt = Rt.class.getSimpleName() + ".java";
		for (int i = 0; i < elements.length; i++) {
			StackTraceElement e = elements[i];
			String s = e.getFileName();
			if ("Thread.java".equals(s) || rt.equals(s))
				continue;
			return readResourceAsString(Class.forName(elements[i]
					.getClassName()), name, charSet);
		}
		return null;
	}

	public static String readResourceAsString(Class c, String name)
			throws IOException {
		return readResourceAsString(c, name, charSet);
	}

	public static String readResourceAsString(Class c, String name,
			String charSet) throws IOException {
		InputStream is = openResource(c, name);
		if (is == null)
			return null;
		return readAsString(is, charSet);
	}

	public static void updateResource(Class c, String name, byte[] bs)
			throws IOException {
		String s = c.getName().replace('.', '/');
		int t = s.lastIndexOf('/');
		s = s.substring(0, t + 1) + name;
		URL url = c.getClassLoader().getResource(s);
		if (!url.getProtocol().equals("file"))
			throw new IOException("Unknow protocol " + url.getProtocol());
		if (!url.getHost().equals(""))
			throw new IOException("Unknow host " + url.getHost());
		String path = url.getFile();
		String resourcePath = path;
		if (!path.endsWith(s))
			throw new IOException(path);
		path = path.substring(0, path.length() - s.length());
		if (!path.endsWith("bin/"))
			throw new IOException(path);
		path = path.substring(0, path.length() - "bin/".length()) + "src/java/"
				+ s;
		// System.out.println(s);
		// System.out.println(path);
		File file = new File(path);
		if (!file.exists())
			throw new IOException("Can't find file " + file.getAbsolutePath());
		// System.out.println(file.getAbsolutePath());
		Rt.write(file, bs);
		file = new File(resourcePath);
		if (!file.exists())
			throw new IOException("Can't find file " + file.getAbsolutePath());
		// System.out.println(file.getAbsolutePath());
		Rt.write(file, bs);
	}

	public static void updateResource(Class c, String name, String text)
			throws UnsupportedEncodingException, IOException {
		updateResource(c, name, getBytes(text, charSet));
	}

	public static String[] readResourceAsLines(Class c, String name)
			throws IOException {
		InputStream is = openResource(c, name);
		if (is == null)
			return null;
		return readAsLines(is);
	}

	public static String[] readResourceAsLines(Class c, String name,
			String charSet) throws IOException {
		InputStream is = openResource(c, name);
		if (is == null)
			return null;
		return readAsLines(is, charSet);
	}

	public static String[] readResourceAsLines(String name) throws Exception {
		StackTraceElement[] elements = Thread.currentThread().getStackTrace();
		String rt = Rt.class.getSimpleName() + ".java";
		for (int i = 0; i < elements.length; i++) {
			StackTraceElement e = elements[i];
			String s = e.getFileName();
			if ("Thread.java".equals(s) || rt.equals(s))
				continue;
			InputStream is = openResource(Class.forName(elements[i]
					.getClassName()), name);
			if (is == null)
				return null;
			return readAsLines(is);
		}
		return null;
	}

	public static byte[] readFileByte(File file) throws IOException {
		if (!file.exists())
			throw new IOException("Can't find file " + file.getAbsolutePath());
		FileInputStream fileInputStream = new FileInputStream(file);
		byte[] buf = new byte[(int) file.length()];
		int start = 0;
		while (start < buf.length) {
			int len = fileInputStream.read(buf, start, buf.length - start);
			if (len < 0)
				break;
			start += len;
		}
		fileInputStream.close();
		return buf;
	}

	public static byte[] readFileByte(File file, long start, int len)
			throws IOException {
		RandomAccessFile fileInputStream = new RandomAccessFile(file, "r");
		fileInputStream.seek(start);
		byte[] buf = new byte[len];
		int s = 0;
		while (s < buf.length) {
			int l = fileInputStream.read(buf, s, buf.length - s);
			if (l < 0)
				break;
			s += l;
			if (s == len)
				break;
		}
		fileInputStream.close();
		return buf;
	}

	public static void modifyFileByte(File file, long start, byte[] bs)
			throws IOException {
		if (!file.exists())
			throw new FileNotFoundException(file.getAbsolutePath());
		RandomAccessFile ra = new RandomAccessFile(file, "rw");
		ra.seek(start);
		ra.write(bs);
		ra.close();
	}

	public static String readFile(File file) throws IOException {
		return createString(readFileByte(file));
	}

	public static String[] readFileAsLines(File file) throws IOException {
		return readFileAsLines(file, charSet);
	}

	public static String[] readFileAsLines(File file, int max)
			throws IOException {
		return readFileAsLines(file, max, charSet);
	}

	public static String[] readFileAsLines(File file, String charSet)
			throws IOException {
		return readFileAsLines(file, Integer.MAX_VALUE, charSet);
	}

	public static String[] readFileAsLines(File file, int max, String charSet)
			throws IOException {
		FileInputStream fileInputStream = new FileInputStream(file);
		String[] lines = readAsLines(new BufferedInputStream(fileInputStream),
				max, charSet);
		fileInputStream.close();
		return lines;
	}

	public static String readFile(File file, String charset) throws IOException {
		return createString(readFileByte(file), charset);
	}

	public static void write(File file, Rx rx) throws IOException,
			TransformerException {
		write(file, rx.getXml());
	}

	public static void write(File file, String s) throws IOException {
		byte[] bs = getBytes(s, charSet);
		write(file, bs);
	}

	public static void append(File file, String s) throws IOException {
		FileOutputStream fileOutputStream = new FileOutputStream(file, true);
		byte[] bs = getBytes(s, charSet);
		fileOutputStream.write(bs);
		fileOutputStream.close();
	}

	public static void write(File file, byte[] bs) throws IOException {
		FileOutputStream fileOutputStream = new FileOutputStream(file);
		fileOutputStream.write(bs);
		fileOutputStream.close();
	}

	public static void writeIfChanged(File file, byte[] bs) throws IOException {
		if (file.exists()) {
			byte[] bs2 = readFileByte(file);
			if (bytesEquals(bs, bs2)) {
				Rt.np(file.getName() + " not changed");
				return;
			}
		}
		FileOutputStream fileOutputStream = new FileOutputStream(file);
		fileOutputStream.write(bs);
		fileOutputStream.close();
	}

	public static void write(File file, InputStream inputStream)
			throws IOException {
		FileOutputStream fileOutputStream = new FileOutputStream(file);
		byte[] buf = new byte[1024];
		while (true) {
			int len = inputStream.read(buf);
			if (len < 0)
				break;
			fileOutputStream.write(buf);
		}
		fileOutputStream.close();
	}

	public static byte[] getFileHash(File file) throws IOException {
		FileInputStream fileInputStream = new FileInputStream(file);
		MessageDigest md5;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new IOException("md5 error");
		}
		byte[] buf = new byte[1024];
		while (true) {
			int len = fileInputStream.read(buf);
			if (len < 0)
				break;
			md5.update(buf, 0, len);
		}
		fileInputStream.close();
		return md5.digest();
	}

	public static void clearDir(File dir) {
		File[] files = dir.listFiles();
		for (File file : files) {
			if (file.isDirectory())
				clearDir(file);
			file.delete();
		}
	}

	public static void cleanup(File dir) {
		File[] files = dir.listFiles();
		boolean hasFile = false;
		for (File file : files) {
			if (file.isDirectory())
				cleanup(file);
			if (file.exists())
				hasFile = true;
		}
		if (!hasFile && dir.isDirectory())
			dir.delete();
	}

	public static void removeDir(File dir) {
		clearDir(dir);
		dir.delete();
	}

	public static void copyFile(File file1, File file2) throws IOException {
		if (file2.exists() && file2.isDirectory()) {
			file2 = new File(file2, file1.getName());
		}
		FileInputStream fileInputStream = new FileInputStream(file1);
		FileOutputStream fileOutputStream = new FileOutputStream(file2);
		byte[] buffer = new byte[1024];
		while (true) {
			int read = fileInputStream.read(buffer);
			if (read < 0)
				break;
			fileOutputStream.write(buffer, 0, read);
		}
		fileInputStream.close();
		fileOutputStream.close();
		file2.setLastModified(file1.lastModified());
	}

	public static void copyStream(InputStream fileInputStream,
			OutputStream fileOutputStream) throws IOException {
		byte[] buffer = new byte[1024];
		while (true) {
			int read = fileInputStream.read(buffer);
			if (read < 0)
				break;
			fileOutputStream.write(buffer, 0, read);
		}
		fileInputStream.close();
		fileOutputStream.close();
	}

	public static boolean isFileSame(File file, byte[] bs2) throws IOException {
		byte[] bs1 = readFileByte(file);
		if (bs1.length != bs2.length)
			return false;
		for (int i = 0; i < bs1.length; i++) {
			if (bs1[i] != bs2[i])
				return false;
		}
		return true;
	}

	public static void copyDir(File file1, File file2) throws IOException {
		if (!file2.exists())
			file2.mkdirs();
		File[] files = file1.listFiles();
		for (File file : files) {
			File dest = new File(file2, file.getName());
			if (file.isDirectory())
				copyDir(file, dest);
			else
				copyFile(file, dest);
		}
	}

	public static void copyDirFileName(File srcDir, File destDir)
			throws IOException {
		if (srcDir.isDirectory()) {
			if (!destDir.exists())
				destDir.mkdirs();
			File[] files = srcDir.listFiles();
			for (File file : files) {
				File dest = new File(destDir, file.getName());
				if (file.isDirectory())
					copyDirFileName(file, dest);
				else if (!dest.exists())
					dest.createNewFile();
			}
		} else if (!destDir.exists()) {
			destDir.createNewFile();
		}
	}

	public static void write(File file, byte[] bs, int start, int len)
			throws IOException {
		FileOutputStream fileOutputStream = new FileOutputStream(file);
		fileOutputStream.write(bs, start, len);
		fileOutputStream.close();
	}

	public static void write(File file, byte[] bs, int len) throws IOException {
		FileOutputStream fileOutputStream = new FileOutputStream(file);
		fileOutputStream.write(bs, 0, len);
		fileOutputStream.close();
	}

	public static byte[] deflate(byte[] bs) {
		return deflate(bs, null);
	}

	public static byte[] deflate(byte[] bs, byte[] dictionary) {
		Deflater compresser = new Deflater();
		if (dictionary != null)
			compresser.setDictionary(dictionary);
		compresser.setInput(bs);
		compresser.finish();
		java.io.ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		byte[] bs2 = new byte[1024];
		while (true) {
			int len = compresser.deflate(bs2);
			if (len <= 0)
				break;
			outputStream.write(bs2, 0, len);
		}
		return outputStream.toByteArray();
	}

	public static byte[] deflate(byte[] bs, int level) {
		Deflater compresser = new Deflater(level);
		compresser.setInput(bs);
		compresser.finish();
		java.io.ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		byte[] bs2 = new byte[1024];
		while (true) {
			int len = compresser.deflate(bs2);
			if (len <= 0)
				break;
			outputStream.write(bs2, 0, len);
		}
		return outputStream.toByteArray();
	}

	public static byte[] inflate(byte[] bs) throws DataFormatException {
		return inflate(bs, null);
	}

	public static byte[] inflate(byte[] bs, byte[] dict)
			throws DataFormatException {
		Inflater compresser = new Inflater();
		compresser.setInput(bs);
		if (dict != null)
			compresser.setDictionary(dict);
		java.io.ByteArrayOutputStream outputStream = new ByteArrayOutputStream(
				bs.length);
		byte[] bs2 = new byte[1024];
		while (true) {
			int len = compresser.inflate(bs2);
			if (len <= 0)
				break;
			outputStream.write(bs2, 0, len);
		}
		compresser.end();
		return outputStream.toByteArray();
	}

	public static byte[] ungzip(byte[] bs) throws IOException {
		GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(bs));
		bs = Rt.read(in);
		return bs;
	}

	public static byte[] ungzipOS(final byte[] bs) throws IOException {
		Process p = Runtime.getRuntime().exec("gzip -dc");
		final OutputStream out = p.getOutputStream();
		new Thread() {
			public void run() {
				try {
					out.write(bs);
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}.start();
		return Rt.read(p.getInputStream());
	}

	public static byte[] gzip(byte[] bs) throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		GZIPOutputStream out = new GZIPOutputStream(outputStream);
		out.write(bs);
		out.close();
		return outputStream.toByteArray();
	}

	public static void writeObjectToFile(Object o, String filename)
			throws IOException {
		FileOutputStream fileOutputStream = new FileOutputStream(filename);
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(
				fileOutputStream);
		objectOutputStream.writeObject(o);
		objectOutputStream.close();
		fileOutputStream.close();
	}

	public static void writeObjectToFile(Object o, File filename)
			throws IOException {
		FileOutputStream fileOutputStream = new FileOutputStream(filename);
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(
				fileOutputStream);
		objectOutputStream.writeObject(o);
		objectOutputStream.close();
		fileOutputStream.close();
	}

	public static Object readObjectFromFile(File filename) throws Exception {
		FileInputStream fileInputStream = new FileInputStream(filename);
		ObjectInputStream objectInputStream = new ObjectInputStream(
				fileInputStream);
		Object o = objectInputStream.readObject();
		objectInputStream.close();
		fileInputStream.close();
		return o;
	}

	public static Object readObjectFromFile(String filename) throws Exception {
		FileInputStream fileInputStream = new FileInputStream(filename);
		ObjectInputStream objectInputStream = new ObjectInputStream(
				fileInputStream);
		Object o = objectInputStream.readObject();
		objectInputStream.close();
		fileInputStream.close();
		return o;
	}

	private static char toHex(int nibble) {
		return hexDigit[(nibble & 0xF)];
	}

	public static int[] toIntArray(Vector<Integer> vector) {
		int[] is = new int[vector.size()];
		for (int i = 0; i < is.length; i++)
			is[i] = vector.get(i);
		return is;
	}

	public static float[] toFloatArray(Vector<Float> vector) {
		float[] is = new float[vector.size()];
		for (int i = 0; i < is.length; i++)
			is[i] = vector.get(i);
		return is;
	}

	private static final char[] hexDigit = { '0', '1', '2', '3', '4', '5', '6',
			'7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

	private static String saveConvert(String theString, boolean escapeSpace,
			boolean escapeUnicode) {
		int len = theString.length();
		int bufLen = len * 2;
		if (bufLen < 0) {
			bufLen = Integer.MAX_VALUE;
		}
		StringBuffer outBuffer = new StringBuffer(bufLen);

		for (int x = 0; x < len; x++) {
			char aChar = theString.charAt(x);
			if ((aChar > 61) && (aChar < 127)) {
				if (aChar == '\\') {
					outBuffer.append('\\');
					outBuffer.append('\\');
					continue;
				}
				outBuffer.append(aChar);
				continue;
			}
			switch (aChar) {
			case ' ':
				if (x == 0 || escapeSpace)
					outBuffer.append('\\');
				outBuffer.append(' ');
				break;
			case '\t':
				outBuffer.append('\\');
				outBuffer.append('t');
				break;
			case '\n':
				outBuffer.append('\\');
				outBuffer.append('n');
				break;
			case '\r':
				outBuffer.append('\\');
				outBuffer.append('r');
				break;
			case '\f':
				outBuffer.append('\\');
				outBuffer.append('f');
				break;
			case '=': // Fall through
			case ':': // Fall through
			case '#': // Fall through
			case '!':
				outBuffer.append('\\');
				outBuffer.append(aChar);
				break;
			default:
				if (((aChar < 0x0020) || (aChar > 0x007e)) & escapeUnicode) {
					outBuffer.append('\\');
					outBuffer.append('u');
					outBuffer.append(toHex((aChar >> 12) & 0xF));
					outBuffer.append(toHex((aChar >> 8) & 0xF));
					outBuffer.append(toHex((aChar >> 4) & 0xF));
					outBuffer.append(toHex(aChar & 0xF));
				} else {
					outBuffer.append(aChar);
				}
			}
		}
		return outBuffer.toString();
	}

	public static void writeProperties(Properties properties, File file,
			String comments) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(file));
		if (comments != null) {
			bw.write("#" + comments);
			bw.newLine();
		}
		bw.write("#" + new Date().toString());
		bw.newLine();
		synchronized (properties) {
			String[] ss = properties.keySet().toArray(
					new String[properties.size()]);
			Arrays.sort(ss, new Comparator<String>() {
				public int compare(String o1, String o2) {
					return o1.compareTo(o2);
				}
			});
			for (String key : ss) {
				bw.write(saveConvert(key, true, false)
						+ "="
						+ saveConvert((String) properties.get(key), false,
								false));
				bw.newLine();
			}
		}
		bw.flush();
		bw.close();
	}

	public static String writeProperties(Properties properties, String comments)
			throws IOException {
		StringWriter stringWriter = new StringWriter();
		BufferedWriter bw = new BufferedWriter(stringWriter);
		if (comments != null) {
			bw.write("#" + comments);
			bw.newLine();
		}
		bw.write("#" + new Date().toString());
		bw.newLine();
		synchronized (properties) {
			String[] ss = properties.keySet().toArray(
					new String[properties.size()]);
			Arrays.sort(ss, new Comparator<String>() {
				public int compare(String o1, String o2) {
					return o1.compareTo(o2);
				}
			});
			for (String key : ss) {
				bw.write(saveConvert(key, true, false)
						+ "="
						+ saveConvert((String) properties.get(key), false,
								false));
				bw.newLine();
			}
		}
		bw.flush();
		bw.close();
		return stringWriter.toString();
	}

	public static void addJavaLibraryPath(String path) {
		try {
			Class classLoaderClass = ClassLoader.class;
			Field classLoaderUserPathField = classLoaderClass
					.getDeclaredField("usr_paths");
			classLoaderUserPathField.setAccessible(true);
			String[] orgValue = (String[]) classLoaderUserPathField.get(null);
			if (orgValue.length > 0 && orgValue[0].equalsIgnoreCase(path))
				return;
			String[] newValue = new String[orgValue.length + 1];
			newValue[0] = path;
			for (int i = 0; i < orgValue.length; i++)
				newValue[i + 1] = orgValue[i];
			classLoaderUserPathField.set(null, newValue);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void testZip() throws Exception {
		File file = new File("D:\\down\\crawler\\shanghaiA\\data.zip");
		File file2 = new File("D:\\down\\crawler\\shanghaiA\\data.ziplist");
		// zip2list(file, file2);
		// byte[] bs=new byte[4096];
		// for (int i=0;i<bs.length;i++) {
		// bs[i]=(byte)i;
		// }
		// byte[] bs2= deflact(bs);
		// System.out.println(bs2.length);
		// byte[] bs3= inflact(bs2);
		// System.out.println(bs3.length);
		// for (int i=0;i<bs.length;i++)
		// if (bs[i]!=bs3[i])
		// throw new Error();
	}

	public static void scrollJScrollPaneToTop(final javax.swing.JScrollPane pane) {
		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				pane.getViewport().setViewPosition(new Point(0, 0));
			}
		});
	}

	private static void addFileToZip(
			java.util.zip.ZipOutputStream outputStream, File dir, String name)
			throws IOException {
		if (dir.isDirectory()) {
			for (File file : dir.listFiles()) {
				addFileToZip(outputStream, file, name.length() > 0 ? name + "/"
						+ file.getName() : file.getName());
			}
		} else {
			java.util.zip.ZipEntry entry = new ZipEntry(name);
			outputStream.putNextEntry(entry);
			outputStream.write(Rt.readFileByte(dir));
			outputStream.closeEntry();
		}
	}

	public static void createZip(File dir, File file) throws IOException {
		if (!dir.exists())
			throw new IOException(dir.getAbsolutePath());
		java.util.zip.ZipOutputStream stream = new ZipOutputStream(
				new FileOutputStream(file));
		addFileToZip(stream, dir, "");
		stream.close();
	}

	public static void setUIFont(javax.swing.plaf.FontUIResource f) {
		// not working
		//
		// sets the default font for all Swing components.
		// ex.
		// setUIFont (new javax.swing.plaf.FontUIResource
		// ("Serif",Font.ITALIC,12));
		//
		java.util.Enumeration keys = UIManager.getDefaults().keys();
		while (keys.hasMoreElements()) {
			Object key = keys.nextElement();
			Object value = UIManager.get(key);
			if (value instanceof javax.swing.plaf.FontUIResource) {
				UIManager.put(key, f);
			}
		}
	}

	public static void setLookAndFeel() {
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void initUIFont() {
		Rt.setUIFont(new javax.swing.plaf.FontUIResource(
				"AR PL ShanHeiSun Uni", 0, 16));
	}

	public static void initUI() {
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (Exception e) {
			e.printStackTrace();
		}
		// javax.swing.plaf.FontUIResource f = new
		// javax.swing.plaf.FontUIResource(
		// "AR PL ShanHeiSun Uni", 0, 16);
		// java.util.Enumeration keys = UIManager.getDefaults().keys();
		// while (keys.hasMoreElements()) {
		// Object key = keys.nextElement();
		// Object value = UIManager.get(key);
		// if (value instanceof javax.swing.plaf.FontUIResource) {
		// UIManager.put(key, f);
		// }
		// }
		//
		// Rt.p(UIManager.get("Label.font"));
		// Rt.p(UIManager.getDefaults().get("Label.font"));
		// Rt.p(UIManager.getLookAndFeel().getDefaults().get("Label.font"));
	}

	public static void listChineseFont() {
		Vector<String> chinesefonts = new Vector<String>();
		Font[] allfonts = GraphicsEnvironment.getLocalGraphicsEnvironment()
				.getAllFonts();
		int fontcount = 0;
		String chinesesample = "\u4e00";
		for (int j = 0; j < allfonts.length; j++) {
			if (allfonts[j].canDisplayUpTo(chinesesample) == -1) {
				chinesefonts.add(allfonts[j].getFontName());
			}
			fontcount++;
		}
		System.out.println(fontcount);
		for (String string : chinesefonts) {
			System.out.println(string);
		}
	}

	public static String bytesToHexString(byte[] bs) {
		if (bs == null)
			return null;
		return bytesToHexString(bs, bs.length);
	}

	public static String bytesToHexString(byte[] bs, int len) {
		return bytesToHexString(bs, 0, len);
	}

	public static String bytesToHexString(byte[] bs, int offset, int len) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++) {
			sb.append(String.format("%02X", bs[offset + i] & 0xFF));
		}
		return sb.toString();
	}

	public static String bytesToHexStringSpace(byte[] bs, int offset, int len) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++) {
			sb.append(String.format("%02X ", bs[offset + i] & 0xFF));
		}
		return sb.toString();
	}

	public static byte[] hexStringToBytes(String s) {
		StringBuilder sb = new StringBuilder(s.length());
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (c != ' ')
				sb.append(c);
		}
		if (sb.length() % 2 != 0)
			Rt.error("s.length()==" + sb.length());
		byte[] bs = new byte[sb.length() / 2];
		for (int i = 0; i < bs.length; i++) {
			bs[i] = (byte) Integer.parseInt(sb.substring(i + i, i + i + 2), 16);
		}
		return bs;
	}

	public static String bytesToIntString(byte[] bs) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < bs.length; i++) {
			if (i > 0)
				sb.append(".");
			sb.append(bs[i] & 0xFF);
		}
		return sb.toString();
	}

	public static byte[] intStringToBytes(String ip) {
		StringTokenizer st = new StringTokenizer(ip, ".");
		byte[] bs = new byte[st.countTokens()];
		for (int i = 0; i < bs.length; i++) {
			bs[i] = (byte) Integer.parseInt(st.nextToken());
		}
		return bs;
	}

	public static String showPasswordDialog(String message) {
		final JDialog frame = new JDialog((Frame) null, "Input password");
		JLabel label = new JLabel(message);
		final JPasswordField field = new JPasswordField();
		field.addKeyListener(new KeyAdapter() {
			@Override
			public void keyPressed(KeyEvent e) {
				if (e.getKeyCode() == KeyEvent.VK_ENTER) {
					frame.setVisible(false);
				} else if (e.getKeyCode() == KeyEvent.VK_ESCAPE) {
					field.setText("");
					frame.setVisible(false);
				}
			}
		});
		frame.addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosing(WindowEvent e) {
				field.setText("");
			}
		});
		frame.add(label, BorderLayout.NORTH);
		frame.add(field, BorderLayout.CENTER);
		frame.setModal(true);
		frame.setSize(300, 100);
		frame.setLocation(300, 200);
		frame.setVisible(true);
		char[] cs = field.getPassword();
		if (cs.length == 0)
			return null;
		return new String(cs);
	}

	public static String[] sortHashSet(HashSet<String> hashSet) {
		String[] ss = new String[hashSet.size()];
		int pos = 0;
		for (String s : hashSet) {
			ss[pos++] = s;
		}
		Arrays.sort(ss, new java.util.Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		});
		return ss;
	}

	public static void sortStringArray(String[] set) {
		Arrays.sort(set, new java.util.Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		});
	}

	public static String[] sort(Set<String> set) {
		String[] ss = set.toArray(new String[set.size()]);
		Arrays.sort(ss, new java.util.Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		});
		return ss;
	}

	public static void sortStringVector(Vector<String> set) {
		Collections.sort(set, new java.util.Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		});
	}

	public static void sortIntStringArray(String[] set) {
		Arrays.sort(set, new java.util.Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return Integer.parseInt(o1) - Integer.parseInt(o2);
			}
		});
	}

	public static int[] sortIntegerSet(Set<Integer> set) {
		int[] ss = new int[set.size()];
		int pos = 0;
		for (Integer s : set) {
			ss[pos++] = s;
		}
		Arrays.sort(ss);
		return ss;
	}

	public static String getStdCommand() throws IOException {
		if (System.in.available() > 0) {
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			while (System.in.available() > 0) {
				int t = System.in.read();
				if (t == '\n')
					break;
				outputStream.write(t);
			}
			return new String(outputStream.toByteArray());
		}
		return null;
	}

	public static int getActualStringLength(String s) throws Exception {
		Field field = String.class.getDeclaredField("value");
		field.setAccessible(true);
		char[] cs = (char[]) field.get(s);
		return cs.length;
	}

	public static boolean compareFiles(File file1, File file2)
			throws IOException {
		byte[] bs1 = Rt.readFileByte(file1);
		byte[] bs2 = Rt.readFileByte(file2);
		if (bs1.length != bs2.length)
			return false;
		for (int i = 0; i < bs1.length; i++) {
			if (bs1[i] != bs2[i])
				return false;
		}
		return true;
	}

	public static String getSystemInfo(String execName, String arg,
			int minThreads) throws Exception {
		String cmd = "ps --cols 4000 -C " + execName + " -o nlwp,rss,args";
		Process process = Runtime.getRuntime().exec(cmd);
		byte[] bs = Rt.read(process.getInputStream());
		StringBuilder sb = new StringBuilder();
		String[] lines = new String(bs).split("\n");
		for (int i = 1; i < lines.length; i++) {
			String s = lines[i].trim();
			String[] ss = s.split("[ |\t]+", 3);
			int thread = Integer.parseInt(ss[0]);
			int memory = Integer.parseInt(ss[1]);
			String args = ss[2];
			if (args.indexOf(arg) >= 0 && thread > minThreads)
				sb.append(String.format("thread=%,d\nmemory=%,d\n", thread,
						memory * 1024));
		}
		return sb.toString();
	}

	private static MessageDigest md5;

	public static byte[] md5(byte[] bs) {
		if (md5 == null) {
			try {
				md5 = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
				return null;
			}
		}
		synchronized (md5) {
			md5.reset();
			return md5.digest(bs);
		}
	}

	public static String md5(String s) {
		if (md5 == null) {
			try {
				md5 = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
				return null;
			}
		}
		synchronized (md5) {
			md5.reset();
			return bytesToHexString(md5.digest(s.getBytes()));
		}
	}

	public static String[] split(String str, char delim) {
		int len = str.length();
		java.util.Vector<String> vector = new Vector<String>();
		int start = 0;
		for (int i = 0; i < len; i++) {
			char c = str.charAt(i);
			if (c == delim) {
				vector.add(str.substring(start, i));
				start = i + 1;
			}
		}
		vector.add(str.substring(start));
		return vector.toArray(new String[vector.size()]);
	}

	public static int compareBytes(byte[] bs1, byte[] bs2) {
		int len = Math.min(bs1.length, bs2.length);
		for (int i = 0; i < len; i++) {
			if (bs1[i] > bs2[i])
				return 1;
			if (bs1[i] < bs2[i])
				return -1;
		}
		if (bs1.length > len)
			return 1;
		if (bs2.length > len)
			return -1;
		return 0;
	}

	public static boolean strEquals(String s1, String s2) {
		if ((s1 == null) != (s2 == null))
			return false;
		if (s1 == null)
			return true;
		return s1.equals(s2);
	}

	public static boolean bytesEquals(byte[] bs1, byte[] bs2) {
		if (bs1.length != bs2.length)
			return false;
		return bytesEquals(bs1, bs2, bs1.length);
	}

	public static int indexOf(byte[] bs, byte b, int fromIndex) {
		int count = bs.length;
		if (fromIndex < 0) {
			fromIndex = 0;
		} else if (fromIndex >= count) {
			return -1;
		}

		for (int i = fromIndex; i < count; i++) {
			if (bs[i] == b) {
				return i;
			}
		}
		return -1;
	}

	public static int lastIndexOf(byte[] bs, byte b, int fromIndex) {
		int count = bs.length;
		int i = (fromIndex >= count) ? count - 1 : fromIndex;

		for (; i >= 0; i--) {
			if (bs[i] == b) {
				return i;
			}
		}
		return -1;
	}

	public static byte[] sub(byte[] bs, int start) {
		return sub(bs, start, bs.length);
	}

	public static byte[] sub(byte[] bs, int start, int end) {
		byte[] b = new byte[end - start];
		System.arraycopy(bs, start, b, 0, end - start);
		return b;
	}

	public static int indexOf(byte[] text, byte[] keyword, int fromIndex) {
		int sourceCount = text.length;
		int targetCount = keyword.length;
		if (fromIndex >= sourceCount)
			return (targetCount == 0 ? sourceCount : -1);
		if (fromIndex < 0)
			fromIndex = 0;
		if (targetCount == 0)
			return fromIndex;

		byte first = keyword[0];
		int max = sourceCount - targetCount;

		for (int i = fromIndex; i <= max; i++) {
			/* Look for first character. */
			if (text[i] != first) {
				while (++i <= max && text[i] != first)
					;
			}

			/* Found first character, now look at the rest of v2 */
			if (i <= max) {
				int j = i + 1;
				int end = j + targetCount - 1;
				for (int k = 0 + 1; j < end && text[j] == keyword[k]; j++, k++)
					;

				if (j == end) {
					/* Found whole string. */
					return i;
				}
			}
		}
		return -1;
	}

	public static int bytesCompare(byte[] bs1, byte[] bs2) {
		int len1 = bs1.length;
		int len2 = bs2.length;
		int n = Math.min(len1, len2);

		int k = 0;
		while (k < n) {
			int c1 = bs1[k] & 0xFF;
			int c2 = bs2[k] & 0xFF;
			if (c1 != c2) {
				return c1 - c2;
			}
			k++;
		}
		return len1 - len2;
	}

	public static boolean bytesEquals(byte[] bs1, byte[] bs2, int len) {
		for (int i = 0; i < len; i++) {
			if (bs1[i] != bs2[i])
				return false;
		}
		return true;
	}

	public static String runShScript(String script) throws IOException {
		File tmpDir = new File("/tmp/cache");
		if (!tmpDir.exists())
			tmpDir = new File("/tmp");
		File tmpFile = File.createTempFile("wrtmp", ".sh", tmpDir);
		try {
			Rt.write(tmpFile, script);
			Process process = Runtime.getRuntime().exec(
					"/bin/sh " + tmpFile.getAbsolutePath());
			InputStream err = process.getErrorStream();
			return Rt.readAsString(process.getInputStream())
					+ Rt.readAsString(err);
		} finally {
			tmpFile.delete();
		}
	}

	public static String runCommand(String cmd) throws IOException {
		Process process = Runtime.getRuntime().exec(cmd);
		InputStream err = process.getErrorStream();
		return Rt.readAsString(process.getInputStream()) + Rt.readAsString(err);
	}

	public static void showInputStream(final InputStream is) {
		new Thread() {
			@Override
			public void run() {
				byte[] bs = new byte[1024];
				try {
					while (true) {
						int len = is.read(bs);
						if (len < 0)
							break;
						System.out.print(new String(bs, 0, len));
					}
				} catch (IOException e) {
					if (!"Stream Closed".equals(e.getMessage()))
						e.printStackTrace();
				}
			}
		}.start();
	}

	public static void showInputStream(final InputStream is,
			final StringBuilder sb) {
		new Thread() {
			@Override
			public void run() {
				byte[] bs = new byte[1024];
				try {
					while (true) {
						int len = is.read(bs);
						if (len < 0)
							break;
						String s = new String(bs, 0, len);
						System.out.print(s);
						synchronized (sb) {
							sb.append(s);
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}.start();
	}

	public static String runAndShowCommand(String[] cmd) throws IOException {
		Process process = Runtime.getRuntime().exec(cmd);
		StringBuilder sb = new StringBuilder();
		showInputStream(process.getInputStream(), sb);
		showInputStream(process.getErrorStream(), sb);
		try {
			process.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}

	public static String runAndShowCommand(String cmd) throws IOException {
		Process process = Runtime.getRuntime().exec(cmd);
		StringBuilder sb = new StringBuilder();
		showInputStream(process.getInputStream(), sb);
		showInputStream(process.getErrorStream(), sb);
		try {
			process.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}

	public static String runCommand(String[] cmd) throws IOException {
		Process process = Runtime.getRuntime().exec(cmd);
		InputStream err = process.getErrorStream();
		return Rt.readAsString(process.getInputStream()) + Rt.readAsString(err);
	}

	public static String[] getCommandLines(String cmd) throws IOException {
		Process process = Runtime.getRuntime().exec(cmd);
		return Rt.readAsLines(process.getInputStream());
	}

	public static String encodeString(String s) {
		StringBuilder sb = new StringBuilder();
		sb.append('"');
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			switch (c) {
			case '\\':
				sb.append("\\\\");
				break;
			case '\t':
				sb.append("\\t");
				break;
			case '\r':
				sb.append("\\r");
				break;
			case '\n':
				sb.append("\\n");
				break;
			case '\"':
				sb.append("\\\"");
				break;
			case '\'':
				sb.append("\\'");
				break;
			default:
				sb.append(c);
			}
		}
		sb.append('"');
		return sb.toString();
	}

	public static int getPid() throws IOException {
		Process process = Runtime.getRuntime().exec(
				new String[] { "/usr/bin/perl", "-e", "print getppid();" });
		String output = readAsString(process.getInputStream());
		return Integer.parseInt(output);
	}

	/**
	 * Get the process command line of a specific process
	 * 
	 * @param pid
	 * @return
	 * @throws IOException
	 */
	public static String getPidCmdLine(int pid) throws IOException {
		Process process = Runtime.getRuntime()
				.exec(
						new String[] { "ps", "-p", Integer.toString(pid), "-o",
								"args" });
		String[] output = readAsLines(process.getInputStream());
		if (output == null || output.length < 2)
			return null;
		return output[1];
	}

	public static String getShortenString(String s, int pre, int after) {
		int len = s.length();
		if (pre + after >= len)
			return s;
		return s.substring(0, pre) + " ... " + s.substring(len - after);
	}

	public static File[] sortFilesByName(File[] files) {
		Arrays.sort(files, new Comparator<File>() {
			@Override
			public int compare(File arg0, File arg1) {
				return arg0.getName().compareTo(arg1.getName());
			}
		});
		return files;
	}


	public static double p2 = Math.PI * 2;

	public static String escapeJavaString(String s) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			switch (c) {
			case '\t':
				sb.append("\\t");
				break;
			case '\r':
				sb.append("\\r");
				break;
			case '\n':
				sb.append("\\n");
				break;
			case '\'':
				sb.append("\\'");
				break;
			case '\"':
				sb.append("\\\"");
				break;
			case '\\':
				sb.append("\\\\");
				break;
			default:
				if (c < 32 || c > 126)
					sb.append("\\u" + String.format("%04x", (int) c));
				else
					sb.append(c);
				break;
			}
		}
		return sb.toString();
	}

	public static double format0_2p(double a) {
		while (a > p2)
			a -= p2;
		while (a < 0)
			a += p2;
		return a;
	}

	public static double formatnp_p(double a) {
		while (a > Math.PI)
			a -= p2;
		while (a < -Math.PI)
			a += p2;
		return a;
	}

	public static String swapString(String s) {
		char[] cs = s.toCharArray();
		for (int i = 0, j = cs.length - 1; i < j; i++, j--) {
			char t = cs[i];
			cs[i] = cs[j];
			cs[j] = t;
		}
		return new String(cs);
	}

	static Comparator<String> strComparator;

	public static int binarySearch(String[] ss, String s) {
		if (strComparator == null)
			strComparator = new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					return o1.compareTo(o2);
				}
			};
		return Arrays.binarySearch(ss, s);
	}

	public static String getFileNameWithoutExt(String name) {
		int t = name.lastIndexOf('.');
		if (t > 0)
			name = name.substring(0, t);
		return name;
	}

	public static boolean atHome() {
		try {
			return "wr".equals(InetAddress.getLocalHost().getHostName());
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static Properties readProperties(File file) throws IOException {
		Properties properties = new Properties();
		FileInputStream in = new FileInputStream(file);
		properties.load(in);
		in.close();
		return properties;
	}

	public static String[] searchPatterns(String text, String startPattern,
			String endPattern) {
		return searchPatterns(text, startPattern, endPattern, true, false);
	}

	public static String[] searchPatterns(String text, String startPattern,
			String endPattern, boolean includeStart, boolean includeEnd) {
		Vector<String> vector = new Vector<String>();
		Pattern p1 = Pattern.compile(startPattern);
		Pattern p2 = Pattern.compile(endPattern);
		Matcher m1 = p1.matcher(text);
		Matcher m2 = p2.matcher(text);
		int start = 0;
		while (true) {
			if (!m1.find(start))
				break;
			if (!m2.find(m1.end()))
				break;
			vector.add(text.substring(includeStart ? m1.start() : m1.end(),
					includeEnd ? m2.end() : m2.start()));
			start = m2.end();
		}
		return vector.toArray(new String[vector.size()]);
	}

}
