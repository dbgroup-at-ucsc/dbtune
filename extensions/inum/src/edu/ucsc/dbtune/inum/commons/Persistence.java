package edu.ucsc.dbtune.inum.commons;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileInputStream;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Feb 22, 2008
 * Time: 3:37:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class Persistence {
    public static void saveToXML(String fileName, Object o) throws IOException {
        System.out.println("CostEstimator.saveMaterializedViewAccesses(" + fileName + ")");
        XStream xstream = new XStream();
        GZIPOutputStream oos = new GZIPOutputStream(new FileOutputStream(fileName));
        xstream.toXML(o, oos);
        oos.finish();
        oos.close();
    }

    public static Object loadFromXML(String filename) throws IOException {
        System.out.println("CostEstimator.loadConfigEnumerations(" + filename + ")");
        XStream xstream = new XStream(new DomDriver());

        GZIPInputStream ois = new GZIPInputStream(new FileInputStream(filename));
        try {
                return xstream.fromXML(ois);
        } finally {
            ois.close();
        }
    }
}
