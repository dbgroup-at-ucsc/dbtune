package edu.ucsc.dbtune.bip.util;

public interface LogListener {
    String BIP = "BIP";

    void onLogEvent(String component, String logEvent);
}