package org.apache.iotdb.db.queryengine.execution.colquery;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry for collaborative query sessions on Cloud.
 * - Avoids single global QueryStateManager instance by keeping a map per query.
 * - Supports lookups by cloudQueryId (IoTDB) and edgeQueryId (from Edge E2C RPC).
 */
public final class ColQuerySessions {

  private static final Map<String, QueryStateManager> BY_CLOUD_QUERY_ID = new ConcurrentHashMap<>();
  private static final Map<String, QueryStateManager> BY_EDGE_QUERY_ID = new ConcurrentHashMap<>();

  private ColQuerySessions() {}

  public static QueryStateManager create(String edgeQueryId) {
    Objects.requireNonNull(edgeQueryId, "edgeQueryId");
    QueryStateManager session = new QueryStateManager();
    // pre-bind by edgeQueryId
    BY_EDGE_QUERY_ID.put(edgeQueryId, session);
    return session;
  }

  public static void bindCloudQueryId(String edgeQueryId, String cloudQueryId) {
    Objects.requireNonNull(edgeQueryId, "edgeQueryId");
    Objects.requireNonNull(cloudQueryId, "cloudQueryId");
    QueryStateManager s = BY_EDGE_QUERY_ID.get(edgeQueryId);
    if (s != null) {
      BY_CLOUD_QUERY_ID.put(cloudQueryId, s);
    }
  }

  public static QueryStateManager getByCloudQueryId(String cloudQueryId) {
    if (cloudQueryId == null) return null;
    return BY_CLOUD_QUERY_ID.get(cloudQueryId);
  }

  public static QueryStateManager getByEdgeQueryId(String edgeQueryId) {
    if (edgeQueryId == null) return null;
    return BY_EDGE_QUERY_ID.get(edgeQueryId);
  }

  public static boolean hasCloudSession(String cloudQueryId) {
    return BY_CLOUD_QUERY_ID.containsKey(cloudQueryId);
  }

  public static boolean hasEdgeSession(String edgeQueryId) {
    return BY_EDGE_QUERY_ID.containsKey(edgeQueryId);
  }

  public static void removeByCloudQueryId(String cloudQueryId) {
    QueryStateManager s = BY_CLOUD_QUERY_ID.remove(cloudQueryId);
    // Also remove from edge map if still present
    if (s != null) {
      // linear scan to remove
      BY_EDGE_QUERY_ID.values().removeIf(v -> v == s);
    }
  }

  public static void removeByEdgeQueryId(String edgeQueryId) {
    QueryStateManager s = BY_EDGE_QUERY_ID.remove(edgeQueryId);
    if (s != null) {
      BY_CLOUD_QUERY_ID.values().removeIf(v -> v == s);
    }
  }

  /**
   * Heuristic: find a session which is in START state and optionally matches SQL text.
   * This is used as a best-effort fallback when cloudQueryId binding is not ready yet.
   */
  public static QueryStateManager findPendingBySql(String sql) {
    Collection<QueryStateManager> all = BY_EDGE_QUERY_ID.values();
    QueryStateManager candidate = null;
    for (QueryStateManager s : all) {
      try {
        if (s.getStateMachine() != null
            && s.getStateMachine().getState() == ColQueryState.START) {
          if (sql == null || Objects.equals(sql, s.getSql())) {
            // Prefer exact SQL match
            if (sql != null && Objects.equals(sql, s.getSql())) {
              return s;
            }
            candidate = s; // remember a START session as fallback
          }
        }
      } catch (Throwable ignore) {
        // ignore broken session
      }
    }
    return candidate;
  }

  /** Return any pending (START) session not yet bound to a cloud query id. */
  public static QueryStateManager getAnyPending() {
    for (Map.Entry<String, QueryStateManager> e : BY_EDGE_QUERY_ID.entrySet()) {
      QueryStateManager s = e.getValue();
      try {
        if (s.getStateMachine() != null && s.getStateMachine().getState() == ColQueryState.START) {
          // Not strictly verifying cloud-binding here; caller should bind
          return s;
        }
      } catch (Throwable ignore) {
      }
    }
    return null;
  }
}
