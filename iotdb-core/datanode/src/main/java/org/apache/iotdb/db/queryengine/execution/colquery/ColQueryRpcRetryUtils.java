package org.apache.iotdb.db.queryengine.execution.colquery;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ColQueryRpcRetryUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ColQueryRpcRetryUtils.class);

  @FunctionalInterface
  public interface RpcAction {
    void run() throws TException;
  }

  private ColQueryRpcRetryUtils() {}

  public static void execute(String rpcName, RpcAction action) throws TException {
    final ColQueryConfig config = ColQueryConfig.getInstance();
    try {
      action.run();
      return;
    } catch (TException firstFailure) {
      if (!config.isRpcRetryEnabled()) {
        throw firstFailure;
      }

      final int retryWaitMs = Math.max(0, config.getRpcRetryIntervalMs());
      LOGGER.warn(
          "RPC {} failed, retrying once after {} ms. cause={}",
          rpcName,
          retryWaitMs,
          firstFailure.toString());

      if (retryWaitMs > 0) {
        try {
          Thread.sleep(retryWaitMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.warn("RPC {} retry interrupted.", rpcName);
          throw firstFailure;
        }
      }

      action.run();
    }
  }
}
