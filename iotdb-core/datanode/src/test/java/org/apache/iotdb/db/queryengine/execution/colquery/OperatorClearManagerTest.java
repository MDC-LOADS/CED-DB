package org.apache.iotdb.db.queryengine.execution.colquery;

import java.util.*;
import java.util.concurrent.*;

public class OperatorClearManagerTest {
    public static void main(String[] args) throws InterruptedException {
        // 假设有 5 个算子
        List<String> planNodeIds = Arrays.asList("scanA", "join1", "scanB", "scanC", "join2");
        OperatorClearManager manager = new OperatorClearManager(planNodeIds);

        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(3);

        // 提交清理任务（每个算子对应一个线程任务）
        for (String id : planNodeIds) {
            executor.submit(() -> {
                try {
                    // 模拟清理耗时
                    Thread.sleep((long) (Math.random() * 1000));
                    manager.clearOperator("",id);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // 关闭线程池
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // 打印最终状态
        manager.printStatus();
    }
}
