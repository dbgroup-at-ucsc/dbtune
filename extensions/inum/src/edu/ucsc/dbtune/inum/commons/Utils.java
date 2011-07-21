package edu.ucsc.dbtune.inum.commons;

import com.thoughtworks.xstream.XStream;

import edu.ucsc.dbtune.inum.Config;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: Dash
 * Date: Sep 15, 2008
 * Time: 4:57:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class Utils {
    public static final String NL = System.getProperty("line.separator");

    public static LinkedHashSet makeColumns(Iterable<String> iter) {
        LinkedHashSet set = new LinkedHashSet();
        for(String c: iter) {
            set.add(c);
        }
        return set;
    }

    public static LinkedHashSet makeColumns(String[] fields) {
        LinkedHashSet set = new LinkedHashSet();
        for(String c: fields) {
            set.add(c);
        }
        return set;
    }

    public static void saveObject(String file, Object o) {
        try {
            XStream xstream = new XStream();
            GZIPOutputStream oos = new GZIPOutputStream(new FileOutputStream(file));
            xstream.toXML(o, oos);
            oos.finish();
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Object loadObject(String file) {
        try {
            XStream xstream = new XStream();
            GZIPInputStream ois = new GZIPInputStream(new FileInputStream(file));
            Object o = xstream.fromXML(ois);
            ois.close();
            return o;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static <T> String join(final Iterable<T> objs, final String delimiter) {
        Iterator<T> iter = objs.iterator();
        if (!iter.hasNext())
            return "";
        StringBuffer buffer = new StringBuffer(String.valueOf(iter.next()));
        while (iter.hasNext())
            buffer.append(delimiter).append(String.valueOf(iter.next()));
        return buffer.toString();
    }

    public static List<Thread> findThreads(String prefix) {
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        if(group.getParent() != null) {
            group = group.getParent();
        }

        Thread[] threads = new Thread[group.activeCount()];
        List<Thread> threadList = new ArrayList();
        group.enumerate(threads, true);
        for (int i = 0; i < threads.length; i++) {
            Thread thr = threads[i];
            if (thr.getName().startsWith(prefix)) {
                threadList.add(thr);
            }
        }

        return threadList;
    }

    public static void main(String[] args) {
        List<Thread> threads = findThreads("main");
        System.out.println("threads = " + threads);
    }

    public static float getPagesFromMBs(double indexSize) {
        return (float) (indexSize * 1024f * 1024f / 8192);
    }

    public static double getMBsFromPages(float pages) {
        return (pages * 8182 / 1024 / 1024);
    }

    public static void moveTheFile(String path, String fname) {
        // deleting the file, if it exists
        File origfile = new File(path).getAbsoluteFile();
        File targetfile = new File(Config.WORKLOAD_DIR, fname).getAbsoluteFile();

        if(origfile.equals(targetfile)) return;

        (new File(Config.WORKLOAD_DIR, fname)).delete();

        File filesrc = new File(path);

        try {
            File file = new File(Config.WORKLOAD_DIR, fname);

            // Create file if it does not exist
            file.createNewFile();

            // now cop the info
            InputStream in = new FileInputStream(filesrc);
            OutputStream out = new FileOutputStream(file);

            // Transfer bytes from in to out
            byte[] buf = new byte[1024];
            int len;
            while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
            in.close();
            out.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}