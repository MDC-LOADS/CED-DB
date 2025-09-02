package org.apache.iotdb.db.queryengine.execution.colquery;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class OperatorClearManager {
    // planNodeId -> 是否被清空
    private final Map<String, Boolean> clearedMap = new ConcurrentHashMap<>();

    // 总算子数
    private final int totalOperators;

    // 已完成的算子数量
    private final AtomicInteger finishedCount = new AtomicInteger(0);

    public OperatorClearManager(List<String> planNodeIds) {
        this.totalOperators = planNodeIds.size();
        for (String id : planNodeIds) {
            clearedMap.put(id, false); // 初始化为未清空
        }
    }

    /** 清空某个算子 */
    public void clearOperator(String planNodeId) {
        Boolean prev = clearedMap.get(planNodeId);
        if (prev == null) {
            throw new IllegalArgumentException("未知的 planNodeId: " + planNodeId);
        }

        // 避免重复清空
        if (!prev) {
            clearedMap.put(planNodeId, true);
            int finished = finishedCount.incrementAndGet();
            System.out.println("算子 [" + planNodeId + "] 已清空，总完成数 = " + finished);

            if (finished == totalOperators) {
                System.out.println("✅ ALL FINISHED");
                QueryStateManager queryStateManager = QueryStateManager.getInstance();
                queryStateManager.getStateMachine().transitionToClosed();
            }
        }
    }

    /** 查询某个算子是否清空 */
    public boolean isCleared(String planNodeId) {
        return clearedMap.getOrDefault(planNodeId, false);
    }

    /** 打印当前状态 */
    public void printStatus() {
        System.out.println("Cleared状态: " + clearedMap);
        System.out.println("Finished: " + finishedCount.get() + "/" + totalOperators);
    }
}
