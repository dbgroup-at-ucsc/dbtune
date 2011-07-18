package edu.ucsc.dbtune.tools.cmudb.commons;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Feb 29, 2008
 * Time: 4:15:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class Cloner {

    public static<T> T clone(T o) {
        T o1 = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(o);
            oos.close();
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            o1 = (T) ois.readObject();
            ois.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return o1;
    }
}