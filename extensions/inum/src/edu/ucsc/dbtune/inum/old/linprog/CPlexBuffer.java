package edu.ucsc.dbtune.inum.old.linprog;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Mar 16, 2007
 * Time: 6:04:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class CPlexBuffer {
    private PrintWriter obj, cons, bin;

    public CPlexBuffer(String prefix) throws IOException {
        obj  = new PrintWriter(new FileWriter(prefix+".obj"));
        cons = new PrintWriter(new FileWriter(prefix+".cons"));
        bin  = new PrintWriter(new FileWriter(prefix+".bin"));

        cons.println();
        cons.println("Subject To");

        bin.println();
        bin.println("Binary");
        obj.println("maximize");
        obj.print("obj: ");
    }

    public void close() {
        bin.println("End");
        obj.close();
        cons.close();
        bin.close();
    }


    public PrintWriter getObj() {
        return obj;
    }

    public PrintWriter getCons() {
        return cons;
    }

    public PrintWriter getBin() {
        return bin;
    }
}
