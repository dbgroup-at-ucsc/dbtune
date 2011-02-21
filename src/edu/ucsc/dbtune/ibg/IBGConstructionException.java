package edu.ucsc.dbtune.ibg;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class IBGConstructionException extends RuntimeException {
    public IBGConstructionException(String s) {
        super(s);
    }

    public IBGConstructionException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
