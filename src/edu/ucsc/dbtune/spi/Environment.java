package edu.ucsc.dbtune.spi;

import com.germinus.easyconf.ComponentConfiguration;
import com.germinus.easyconf.EasyConf;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Environment {
    private final ComponentConfiguration config;
    Environment(){
        config = EasyConf.getConfiguration("conf.xml");
    }

}
