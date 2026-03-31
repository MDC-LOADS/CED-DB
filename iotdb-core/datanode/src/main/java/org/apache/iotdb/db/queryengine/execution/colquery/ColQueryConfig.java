package org.apache.iotdb.db.queryengine.execution.colquery;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public final class ColQueryConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(ColQueryConfig.class);
  private static final String CONF_FILE = "iotdb-colquery.properties";

  private static volatile ColQueryConfig INSTANCE;

  // Edge defaults
  private String localIp = "127.0.0.1";
  private String remoteIp = "127.0.0.1";
  private String bindIp = "0.0.0.0";
  private int localRpcPort = 9090;
  private int remoteRpcPort = 9091;
  private int localMppPort = 10740;
  private int remoteMppPort = 10744;
  private boolean iscolQuery = false;
  private int colQueryWait=0;
  private boolean rpcRetryEnabled = false;
  private int rpcRetryIntervalMs = 1000;

  private ColQueryConfig() { load(); }

  public static ColQueryConfig getInstance() {
    if (INSTANCE == null) {
      synchronized (ColQueryConfig.class) {
        if (INSTANCE == null) INSTANCE = new ColQueryConfig();
      }
    }
    return INSTANCE;
  }

  private void load() {
    Properties props = new Properties();
    boolean loaded = false;
    URL url = IoTDBDescriptor.getPropsUrl(CONF_FILE);
    if (url != null) {
      try (InputStream in = url.openStream()) {
        props.load(new InputStreamReader(in, StandardCharsets.UTF_8));
        loaded = true;
      } catch (Exception e) {
        LOGGER.warn("Failed to load {} from {}: {}", CONF_FILE, url, e.toString());
      }
    }
    if (!loaded) {
      try (InputStream in = ColQueryConfig.class.getResourceAsStream("/" + CONF_FILE)) {
        if (in != null) {
          props.load(new InputStreamReader(in, StandardCharsets.UTF_8));
          loaded = true;
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to load {} from classpath: {}", CONF_FILE, e.toString());
      }
    }
    if (!loaded) {
      LOGGER.info("No {} found; using defaults.", CONF_FILE);
      return;
    }

    this.localIp = props.getProperty("colquery.local.ip", localIp).trim();
    this.remoteIp = props.getProperty("colquery.remote.ip", remoteIp).trim();
    this.bindIp = props.getProperty("colquery.bind.ip", bindIp).trim();
    this.localRpcPort = parseInt(props.getProperty("colquery.local.rpc.port"), localRpcPort);
    this.remoteRpcPort = parseInt(props.getProperty("colquery.remote.rpc.port"), remoteRpcPort);
    this.localMppPort = parseInt(props.getProperty("colquery.local.mpp.port"), localMppPort);
    this.remoteMppPort = parseInt(props.getProperty("colquery.remote.mpp.port"), remoteMppPort);
    this.iscolQuery = Boolean.parseBoolean(props.getProperty("colquery.iscol.query"));
    this.colQueryWait = parseInt(props.getProperty("colquery.col.query.wait"), colQueryWait);
    this.rpcRetryEnabled =
        Boolean.parseBoolean(
            props.getProperty("colquery.rpc.retry.enabled", Boolean.toString(rpcRetryEnabled)));
    this.rpcRetryIntervalMs =
        parseInt(props.getProperty("colquery.rpc.retry.interval.ms"), rpcRetryIntervalMs);
  }

  private static int parseInt(String s, int def) {
    if (s == null) return def;
    try { return Integer.parseInt(s.trim()); } catch (Exception e) { return def; }
  }

  public String getLocalIp() { return localIp; }
  public String getRemoteIp() { return remoteIp; }
  public String getBindIp() { return bindIp; }
  public int getLocalRpcPort() { return localRpcPort; }
  public int getRemoteRpcPort() { return remoteRpcPort; }
  public int getLocalMppPort() { return localMppPort; }
  public int getRemoteMppPort() { return remoteMppPort; }
  public boolean isColQuery() { return iscolQuery; }
  public int getColQueryWait() { return colQueryWait; }
  public boolean isRpcRetryEnabled() { return rpcRetryEnabled; }
  public int getRpcRetryIntervalMs() { return rpcRetryIntervalMs; }
  public void addColQueryWait() {
    this.colQueryWait = this.colQueryWait + 1;
  }
}
