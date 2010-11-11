package edu.ucsc.satuning;

import edu.ucsc.satuning.console.Console;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
//todo(Huascar) this is not ready. It is just a functionality placeholder...
public class SatuningService {
    private final DBMS dbms;

    public SatuningService(){
        this(DBMS.PG);
    }

    public SatuningService(DBMS dbms){
        this.dbms = dbms;
    }

    void addError(Throwable cause){

    }

    public boolean buildAndRun(boolean logging, Mode mode) throws Exception {
        boolean allDone = false;
        try {
            if(logging){
                Console.getInstance().warn("hey");
                //dbms.runLogging(mode);
                allDone = true;
            } else {
                //dbms.run(mode);
                allDone = true;
            }
        } catch (Exception e){
            Console.getInstance().warn("hey");
        }
        return allDone;
    }
}
