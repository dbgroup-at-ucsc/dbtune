package edu.ucsc.dbtune.inum;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Feb 24, 2008
 * Time: 1:29:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class InumUtils {
    public static String getEnumerationFileName(String workloadFile) {
        return getWorkloadPath("ce."+workloadFile+".gz");
    }

    private static String getWorkloadPath(String workloadFile) {
        return new File(Config.WORKLOAD_DIR,workloadFile).getPath();
    }

    public static boolean isIndexAccessCostFileAvailable(String workloadFile){
      boolean result;
      try {
        result = new File(InumUtils.getIndexAccessCostFile(workloadFile)).exists();
      } catch (Throwable ignored){
        result = false;
      }
      return result;
    }

    public static boolean isEnumerationFileAvailable(String workloadFile){
      boolean result;
      try {
        result = new File(InumUtils.getEnumerationFileName(workloadFile)).exists();
      } catch (Throwable ignored){
        result = false;
      }
      return result;
    }

    public static String getIndexAccessCostFile(String workloadFile) {
        return getWorkloadPath("iac."+workloadFile+".gz");
    }

    public static String getIndexSizeFile(String workloadFile) {
        return getWorkloadPath("ias."+workloadFile+".gz");
    }

    public static String getMatViewAccessCostFile(String workloadFile) {
        return getWorkloadPath("mac."+workloadFile+".gz");
    }
}
