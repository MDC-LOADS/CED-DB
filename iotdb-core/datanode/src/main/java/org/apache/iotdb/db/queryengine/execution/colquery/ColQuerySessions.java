package org.apache.iotdb.db.queryengine.execution.colquery;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Edge-side collaborative query session registry (per colQueryId). */
public final class ColQuerySessions {
  private static final Map<String, QueryStateManager> BY_EDGE_QUERY_ID = new ConcurrentHashMap<>();

  private ColQuerySessions() {}

  public static QueryStateManager create(String edgeColQueryId) {
    QueryStateManager s = new QueryStateManager();
    BY_EDGE_QUERY_ID.put(edgeColQueryId, s);
    return s;
  }

  public static QueryStateManager getByEdgeQueryId(String edgeColQueryId) {
    return BY_EDGE_QUERY_ID.get(edgeColQueryId);
  }

  public static void removeByEdgeQueryId(String edgeColQueryId) {
    QueryStateManager manager = BY_EDGE_QUERY_ID.remove(edgeColQueryId);
    if (manager != null) {
      manager.clearMetrics();
    }
  }
}
