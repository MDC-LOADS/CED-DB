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

package org.apache.iotdb.db.queryengine.execution.operator.process.join;

import org.apache.iotdb.db.queryengine.execution.colquery.QueryStateManager;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;

import org.apache.tsfile.enums.TSDataType;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

/**
 * 演示默认扫描路径初始化逻辑的直观示例
 *
 * <p>这个演示重点关注以下代码的执行效果：
 *
 * <pre>
 * // Initialize default scan paths if not provided
 * if (this.childScanPaths.isEmpty() && QueryStateManager.isInitialized()) {
 *   for (int i = 0; i < inputOperatorsCount; i++) {
 *     this.childScanPaths.add("child_" + i + "_" + operatorContext.getPlanNodeId());
 *   }
 * }
 * </pre>
 */
public class DefaultScanPathsDemoTest {

  @After
  public void tearDown() {
    // 清理单例状态，确保测试之间的隔离
    QueryStateManager.reset();
  }

  /** 🎯 核心演示：展示默认路径生成的条件和结果 */
  @Test
  public void demonstrateDefaultScanPathsLogic() {
    System.out.println("=== InnerTimeJoinOperator 默认扫描路径生成演示 ===\n");

    // ✅ 场景1：满足生成条件 - QueryStateManager单例已初始化，childScanPaths为null
    System.out.println("🟢 场景1: 满足默认路径生成条件");
    System.out.println("   条件: QueryStateManager.isInitialized() && childScanPaths.isEmpty()");

    QueryStateManager stateManager = QueryStateManager.initialize();
    OperatorContext context1 = createMockOperatorContext("JOIN_001");
    List<Operator> children1 = createMockChildren(3);

    InnerTimeJoinOperator operator1 =
        new InnerTimeJoinOperator(
            context1,
            children1,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            null // ✅ 为null，会变成空列表，isEmpty()=true
            );

    List<String> paths1 = operator1.getChildScanPaths();
    System.out.println("   结果: 生成了 " + paths1.size() + " 个默认路径");
    for (int i = 0; i < paths1.size(); i++) {
      System.out.println("     子算子[" + i + "]: " + paths1.get(i));
    }
    System.out.println("   路径格式: child_{索引}_{PlanNodeId}\n");

    // ❌ 场景2：不满足生成条件 - QueryStateManager未初始化
    System.out.println("🔴 场景2: 不满足默认路径生成条件 (QueryStateManager未初始化)");
    System.out.println("   条件: !QueryStateManager.isInitialized()");

    // 重置单例以模拟未初始化状态
    QueryStateManager.reset();

    OperatorContext context2 = createMockOperatorContext("JOIN_002");
    List<Operator> children2 = createMockChildren(3);

    InnerTimeJoinOperator operator2 =
        new InnerTimeJoinOperator(
            context2,
            children2,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            null);

    List<String> paths2 = operator2.getChildScanPaths();
    System.out.println("   结果: 生成了 " + paths2.size() + " 个路径 (没有生成默认路径)");
    System.out.println("   原因: QueryStateManager未初始化，条件检查失败\n");

    // ❌ 场景3：不满足生成条件 - childScanPaths非空
    System.out.println("🔴 场景3: 不满足默认路径生成条件 (提供了自定义路径)");
    System.out.println("   条件: childScanPaths不为空");

    // 重新初始化单例
    QueryStateManager.reinitialize();

    OperatorContext context3 = createMockOperatorContext("JOIN_003");
    List<Operator> children3 = createMockChildren(2);
    List<String> customPaths =
        Arrays.asList("root.factory.line1.temperature", "root.factory.line2.temperature");

    InnerTimeJoinOperator operator3 =
        new InnerTimeJoinOperator(
            context3,
            children3,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            customPaths // ❌ 不为空，isEmpty()=false
            );

    List<String> paths3 = operator3.getChildScanPaths();
    System.out.println("   结果: 使用了 " + paths3.size() + " 个自定义路径");
    for (int i = 0; i < paths3.size(); i++) {
      System.out.println("     子算子[" + i + "]: " + paths3.get(i));
    }
    System.out.println("   原因: childScanPaths不为空，条件检查失败\n");

    // ✅ 场景4：边界情况 - 空列表也会触发生成
    System.out.println("🟢 场景4: 边界情况 - 空列表触发默认路径生成");
    System.out.println(
        "   条件: QueryStateManager.isInitialized() && childScanPaths.isEmpty() (空ArrayList)");

    // 重新初始化单例
    QueryStateManager.reinitialize();

    OperatorContext context4 = createMockOperatorContext("JOIN_004");
    List<Operator> children4 = createMockChildren(4);
    List<String> emptyPaths = new ArrayList<>(); // 空列表，不是null

    InnerTimeJoinOperator operator4 =
        new InnerTimeJoinOperator(
            context4,
            children4,
            Arrays.asList(
                TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            emptyPaths // ✅ 空列表，isEmpty()=true
            );

    List<String> paths4 = operator4.getChildScanPaths();
    System.out.println("   结果: 生成了 " + paths4.size() + " 个默认路径");
    for (int i = 0; i < paths4.size(); i++) {
      System.out.println("     子算子[" + i + "]: " + paths4.get(i));
    }
    System.out.println("   说明: 空ArrayList的isEmpty()返回true，满足条件\n");

    // 📊 总结条件判断逻辑
    System.out.println("📊 条件判断逻辑总结:");
    System.out.println(
        "   if (this.childScanPaths.isEmpty() && QueryStateManager.isInitialized())");
    System.out.println("   │");
    System.out.println("   ├─ childScanPaths.isEmpty() 为true的情况:");
    System.out.println("   │  ├─ childScanPaths传入null（构造函数会创建空ArrayList）");
    System.out.println("   │  └─ childScanPaths传入空ArrayList");
    System.out.println("   │");
    System.out.println("   ├─ QueryStateManager.isInitialized() 为true的情况:");
    System.out.println("   │  └─ 在前置程序中调用QueryStateManager.initialize()");
    System.out.println("   │");
    System.out.println("   └─ 两个条件都满足时，执行默认路径生成循环:");
    System.out.println("      for (int i = 0; i < inputOperatorsCount; i++) {");
    System.out.println(
        "        this.childScanPaths.add(\"child_\" + i + \"_\" + operatorContext.getPlanNodeId());");
    System.out.println("      }");
  }

  /** 演示默认路径的实际应用场景 */
  @Test
  public void demonstrateRealWorldUsage() {
    System.out.println("\n=== 实际应用场景演示 ===\n");

    // 模拟真实的查询场景
    System.out.println("📋 场景: 查询多个传感器的数据进行内连接");
    System.out.println(
        "   SQL: SELECT d1.temperature, d2.pressure, d3.humidity FROM root.factory.**");
    System.out.println("        WHERE time >= '2024-01-01' AND time <= '2024-01-02'");
    System.out.println("   说明: 系统自动为每个传感器生成扫描路径用于状态跟踪\n");

    // 创建模拟的查询环境
    QueryStateManager globalStateManager = QueryStateManager.reinitialize();
    OperatorContext queryContext = createMockOperatorContext("SENSOR_JOIN_20240101");

    // 3个传感器对应3个子算子
    List<Operator> sensorOperators = createMockChildren(3);

    InnerTimeJoinOperator joinOperator =
        new InnerTimeJoinOperator(
            queryContext,
            sensorOperators,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            null // 让系统自动生成路径
            );

    List<String> generatedPaths = joinOperator.getChildScanPaths();

    System.out.println("🔍 自动生成的扫描路径:");
    String[] sensors = {"temperature", "pressure", "humidity"};
    for (int i = 0; i < generatedPaths.size(); i++) {
      System.out.println("   传感器" + (i + 1) + "(" + sensors[i] + "): " + generatedPaths.get(i));
    }

    System.out.println("\n💾 状态跟踪演示:");

    // 模拟扫描过程中的状态更新
    long baseTime = System.currentTimeMillis();
    for (int i = 0; i < generatedPaths.size(); i++) {
      String path = generatedPaths.get(i);
      String sensorType = sensors[i];

      // 模拟不同传感器的扫描状态
      globalStateManager.updateScanTimestamp(path, baseTime + i * 1000);
      globalStateManager.updateScanOffset(path, 100L + i * 50);
      globalStateManager.updateScanCouldEqual(path, i % 2 == 0); // 奇偶不同状态
      globalStateManager.updateScanInnerJoin(path, true);

      QueryStateManager.ScanStates states = globalStateManager.getScanStates(path);
      System.out.println("   " + sensorType + " 传感器状态:");
      System.out.println("     路径: " + path);
      System.out.println("     时间戳: " + states.getScanTimestamp());
      System.out.println("     偏移量: " + states.getOffset());
      System.out.println("     可相等: " + states.isCouldEqual());
      System.out.println("     内连接: " + states.isInnerJoin());
    }

    System.out.println("\n✨ 总结:");
    System.out.println("   默认路径生成机制的优势:");
    System.out.println("   ✓ 自动化: 无需手动为每个子算子指定路径");
    System.out.println("   ✓ 唯一性: 基于PlanNodeId确保路径唯一");
    System.out.println("   ✓ 可追踪: 提供统一的状态跟踪机制");
    System.out.println("   ✓ 灵活性: 支持自定义路径覆盖默认行为");
  }

  // ========== 辅助方法 ==========

  private OperatorContext createMockOperatorContext(String planNodeId) {
    OperatorContext context = Mockito.mock(OperatorContext.class);
    when(context.getPlanNodeId()).thenReturn(new PlanNodeId(planNodeId));
    return context;
  }

  private List<Operator> createMockChildren(int count) {
    List<Operator> children = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      children.add(Mockito.mock(Operator.class));
    }
    return children;
  }

  private Map<InputLocation, Integer> createMockOutputColumnMap() {
    Map<InputLocation, Integer> map = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      map.put(new InputLocation(i, 0), i);
    }
    return map;
  }
}
