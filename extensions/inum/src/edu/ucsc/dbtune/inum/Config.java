package edu.ucsc.dbtune.inum;


import com.google.common.base.Joiner;
import edu.ucsc.dbtune.spi.Environment;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Dec 10, 2007
 * Time: 3:44:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class Config {
    public static final DbType TYPE = DbType.PGSQL;
    public static final String HOME         = System.getProperty("HOME", System.getProperty("user.dir"));
    public static final String WORKLOAD_DIR = Environment.getInstance().getInumCacheDeploymentDir();
    public static final String CONFIG_DIR   = Joiner.on(File.separator).join(File.separator,  HOME, "config", TYPE);
    private static Properties props;
    public static String NewLine            = System.getProperty("line.separator");

    public static Properties getDatabaseProperties() {
        if (props == null) {
            props = new Properties();
            String fileName = Joiner.on(File.separator).join(File.separator, CONFIG_DIR ,"db.properties");
            try {
                props.load(new FileInputStream(fileName));
                return props;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return props;
        }

        return props;
    }

    public static String getDatabaseName() {
        if(props == null) getDatabaseProperties();
        return props.getProperty("database");
    }

    public static String getUserName() {
        if(props == null) getDatabaseProperties();
        return props.getProperty("user");
    }

    public static File getWorkloadFile(String workload) {
        return new File(WORKLOAD_DIR, workload);
    }

    public static boolean generateAllCandidates() {
        return false;
    }

    public static boolean sampleConfigurations() {
        return false;
    }

    public static File getMetadataFile() {
        return new File(CONFIG_DIR, "metadata." + getDatabaseName() + ".conf");
    }
}
