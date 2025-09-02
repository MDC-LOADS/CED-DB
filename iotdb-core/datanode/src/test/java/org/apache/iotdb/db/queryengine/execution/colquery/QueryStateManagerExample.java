/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.colquery;

import org.apache.iotdb.db.queryengine.execution.colquery.QueryStateManager.ScanStates;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Example usage of QueryStateManager (Singleton Pattern) */
public class QueryStateManagerExample {

  public static void main(String[] args) {
    // 1. 初始化单例状态管理器
    QueryStateManager stateManager = QueryStateManager.initialize();

    // 2. 设置状态机
    ExecutorService executor = Executors.newSingleThreadExecutor();
    ColQueryStateMachine stateMachine = new ColQueryStateMachine("query-001", executor);
    stateManager.setStateMachine(stateMachine);

    // 3. 添加scan算子状态
    String scanPath1 = "root.vehicle.d1.temperature";
    String scanPath2 = "root.vehicle.d2.speed";

    // 方式1：直接创建ScanStates对象
    ScanStates scanStates1 =
        new ScanStates(
            System.currentTimeMillis(), // 当前时间戳
            100L, // offset
            true, // isCouldEqual
            false, // isInnerJoin
            true // isFullOuterJoin
            );
    stateManager.setScanStates(scanPath1, scanStates1);

    // 方式2：逐个设置属性
    stateManager.updateScanTimestamp(scanPath2, System.currentTimeMillis() + 1000);
    stateManager.updateScanOffset(scanPath2, 200L);
    stateManager.updateScanCouldEqual(scanPath2, false);
    stateManager.updateScanInnerJoin(scanPath2, true);
    stateManager.updateScanFullOuterJoin(scanPath2, false);

    // 4. 设置左外连接相关状态
    stateManager.setHasLeftOuterJoin(true);
    // stateManager.setLeftOuterJoinCache(someTsBlock); // 设置TsBlock缓存

    // 5. 获取状态信息
    System.out.println("=== 查询状态摘要 ===");
    System.out.println(stateManager.getStateSummary());

    // 6. 获取特定scan算子的状态
    ScanStates retrievedStates = stateManager.getScanStates(scanPath1);
    if (retrievedStates != null) {
      System.out.println("Scan路径 " + scanPath1 + " 的状态:");
      System.out.println("  时间戳: " + retrievedStates.getScanTimestamp());
      System.out.println("  偏移量: " + retrievedStates.getOffset());
      System.out.println("  是否相等: " + retrievedStates.isCouldEqual());
      System.out.println("  是否内连接: " + retrievedStates.isInnerJoin());
      System.out.println("  是否全外连接: " + retrievedStates.isFullOuterJoin());
    }

    // 7. 状态机转换示例
    System.out.println("\n=== 状态机转换 ===");
    System.out.println("当前状态: " + stateManager.getStateMachine().getState());

    stateMachine.transitionToStart();
    System.out.println("转换到START: " + stateManager.getStateMachine().getState());

    stateMachine.transitionToPreColQuery();
    System.out.println("转换到PRE_COL_QUERY: " + stateManager.getStateMachine().getState());


    System.out.println("\nlocalhost ip:"+stateManager.getLocalhostIp());
    System.out.println("\nremote ip:"+stateManager.getRemoteIp());
    System.out.println("\nlocal port:"+stateManager.getLocalhostRpcPort());
    System.out.println("\nremote port:"+stateManager.getRemoteRpcPort());


    //8.测试

    // 9. 清理资源
    stateManager.reset();
    executor.shutdown();

    System.out.println("\n=== 清理后的状态 ===");
    System.out.println(stateManager.toString());

    // 清理单例以为下一个演示做准备
    QueryStateManager.reset();

    demonstrateThreadSafety();
  }

  /** 演示多线程安全使用 */
  public static void demonstrateThreadSafety() {
    QueryStateManager stateManager = QueryStateManager.reinitialize();

    // 模拟多个线程同时修改状态
    for (int i = 0; i < 5; i++) {
      final int threadId = i;
      new Thread(
              () -> {
                String scanPath = "root.db.device" + threadId + ".sensor";

                // 每个线程更新自己的scan状态
                stateManager.updateScanTimestamp(
                    scanPath, System.currentTimeMillis() + threadId * 1000);
                stateManager.updateScanOffset(scanPath, threadId * 100L);
                stateManager.updateScanInnerJoin(scanPath, threadId % 2 == 0);

                System.out.println("线程 " + threadId + " 更新了 " + scanPath);
              })
          .start();
    }

    // 等待一段时间让线程执行完成
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    System.out.println("\n=== 多线程更新后的状态 ===");
    System.out.println(stateManager.getStateSummary());

    // 清理单例
    QueryStateManager.reset();
  }
}
