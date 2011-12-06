package edu.ucsc.dbtune.bip;


public interface LogListener {
    String BIP = "BIP";

    void onLogEvent(String component, String logEvent);
}

