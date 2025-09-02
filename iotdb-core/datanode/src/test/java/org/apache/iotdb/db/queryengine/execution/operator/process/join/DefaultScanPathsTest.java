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

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * 测试InnerTimeJoinOperator中默认扫描路径初始化逻辑的单元测试
 *
 * <p>重点测试这段代码的行为:
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
public class DefaultScanPathsTest {

  @After
  public void tearDown() {
    // 清理单例状态，确保测试之间的隔离
    QueryStateManager.reset();
  }

  /**
   * 测试场景1：当QueryStateManager单例已初始化且childScanPaths为null时，应该生成默认路径
   *
   * <p>这是最常见的触发默认路径生成的场景
   */
  @Test
  public void testDefaultScanPathsGenerationWithNullPaths() {
    // 初始化测试环境
    QueryStateManager stateManager = QueryStateManager.initialize();
    OperatorContext operatorContext = createMockOperatorContext("TestPlan_001");
    List<Operator> children = createMockChildren(3); // 3个子算子

    // 🎯 关键测试点：childScanPaths传入null，应该触发默认路径生成
    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            null // childScanPaths为null ✅
            );

    // 验证结果
    List<String> actualPaths = operator.getChildScanPaths();

    // 应该生成3个默认路径（对应3个子算子）
    assertEquals("应该生成3个默认扫描路径", 3, actualPaths.size());

    // 验证生成的路径格式：child_{index}_{planNodeId}
    assertEquals("第1个子算子的路径格式应该正确", "child_0_TestPlan_001", actualPaths.get(0));
    assertEquals("第2个子算子的路径格式应该正确", "child_1_TestPlan_001", actualPaths.get(1));
    assertEquals("第3个子算子的路径格式应该正确", "child_2_TestPlan_001", actualPaths.get(2));
  }

  /**
   * 测试场景2：当QueryStateManager单例已初始化且childScanPaths为空列表时，应该生成默认路径
   *
   * <p>isEmpty()检查会捕获这种情况
   */
  @Test
  public void testDefaultScanPathsGenerationWithEmptyList() {
    // 初始化测试环境
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext("TestPlan_002");
    List<Operator> children = createMockChildren(2); // 2个子算子

    // 🎯 关键测试点：childScanPaths传入空列表，应该触发默认路径生成
    List<String> emptyPaths = new ArrayList<>(); // 空列表，不是null

    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            emptyPaths // childScanPaths为空列表，isEmpty()=true ✅
            );

    // 验证结果
    List<String> actualPaths = operator.getChildScanPaths();

    // 应该生成2个默认路径
    assertEquals("应该生成2个默认扫描路径", 2, actualPaths.size());

    // 验证路径格式
    assertEquals("第1个子算子的路径格式应该正确", "child_0_TestPlan_002", actualPaths.get(0));
    assertEquals("第2个子算子的路径格式应该正确", "child_1_TestPlan_002", actualPaths.get(1));
  }

  /**
   * 测试场景3：当QueryStateManager单例未初始化时，即使childScanPaths为空也不应该生成默认路径
   *
   * <p>这个条件检查防止了在没有状态管理器时生成无用的路径
   */
  @Test
  public void testNoDefaultPathsWhenQueryStateManagerIsNull() {
    // 确保QueryStateManager未初始化
    QueryStateManager.reset();
    OperatorContext operatorContext = createMockOperatorContext("TestPlan_003");
    List<Operator> children = createMockChildren(3);

    // 🎯 关键测试点：QueryStateManager未初始化，不应该生成默认路径
    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            null // childScanPaths也为null
            );

    // 验证结果
    List<String> actualPaths = operator.getChildScanPaths();

    // 不应该生成任何路径
    assertEquals("不应该生成默认扫描路径", 0, actualPaths.size());
    assertTrue("路径列表应该为空", actualPaths.isEmpty());
  }

  /**
   * 测试场景4：当提供了自定义childScanPaths时，应该使用自定义路径而不生成默认路径
   *
   * <p>即使满足生成条件，也应该优先使用提供的自定义路径
   */
  @Test
  public void testUseCustomPathsWhenProvided() {
    // 初始化测试环境
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext("TestPlan_004");
    List<Operator> children = createMockChildren(2);

    // 🎯 关键测试点：提供自定义路径，不应该生成默认路径
    List<String> customPaths =
        Arrays.asList("root.factory.device1.temperature", "root.factory.device2.humidity");

    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            customPaths // 提供自定义路径，!isEmpty() ❌
            );

    // 验证结果
    List<String> actualPaths = operator.getChildScanPaths();

    // 应该使用自定义路径，不生成默认路径
    assertEquals("应该使用自定义路径", 2, actualPaths.size());
    assertEquals("应该使用第1个自定义路径", "root.factory.device1.temperature", actualPaths.get(0));
    assertEquals("应该使用第2个自定义路径", "root.factory.device2.humidity", actualPaths.get(1));

    // 确认没有生成默认格式的路径
    assertFalse("不应该包含默认格式的路径", actualPaths.get(0).startsWith("child_"));
    assertFalse("不应该包含默认格式的路径", actualPaths.get(1).startsWith("child_"));
  }

  /**
   * 测试场景5：验证默认路径在不同PlanNodeId下的唯一性
   *
   * <p>确保不同的算子实例生成的默认路径是唯一的
   */
  @Test
  public void testDefaultPathsUniquenessAcrossDifferentOperators() {
    // 初始化单例
    QueryStateManager.reinitialize();

    OperatorContext context1 = createMockOperatorContext("Plan_A");
    OperatorContext context2 = createMockOperatorContext("Plan_B");

    List<Operator> children = createMockChildren(2);

    // 创建两个算子，都会生成默认路径
    InnerTimeJoinOperator operator1 =
        new InnerTimeJoinOperator(
            context1,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            null);

    InnerTimeJoinOperator operator2 =
        new InnerTimeJoinOperator(
            context2,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            null);

    // 获取生成的路径
    List<String> paths1 = operator1.getChildScanPaths();
    List<String> paths2 = operator2.getChildScanPaths();

    // 验证路径的唯一性
    assertEquals("第一个算子应该生成2个路径", 2, paths1.size());
    assertEquals("第二个算子应该生成2个路径", 2, paths2.size());

    // 验证不同算子的路径是不同的（因为PlanNodeId不同）
    assertNotEquals("不同算子的第1个路径应该不同", paths1.get(0), paths2.get(0));
    assertNotEquals("不同算子的第2个路径应该不同", paths1.get(1), paths2.get(1));

    // 验证具体的路径格式
    assertEquals("第一个算子的路径包含正确的PlanNodeId", "child_0_Plan_A", paths1.get(0));
    assertEquals("第一个算子的路径包含正确的PlanNodeId", "child_1_Plan_A", paths1.get(1));
    assertEquals("第二个算子的路径包含正确的PlanNodeId", "child_0_Plan_B", paths2.get(0));
    assertEquals("第二个算子的路径包含正确的PlanNodeId", "child_1_Plan_B", paths2.get(1));
  }

  /**
   * 测试场景6：验证在大量子算子情况下的默认路径生成
   *
   * <p>确保算法能正确处理大量子算子的场景
   */
  @Test
  public void testDefaultPathsGenerationWithManyChildren() {
    // 初始化测试环境
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext("LargeJoin_001");

    // 创建10个子算子
    int childCount = 10;
    List<Operator> children = createMockChildren(childCount);
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < childCount; i++) {
      dataTypes.add(TSDataType.DOUBLE);
    }

    // 创建算子
    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            dataTypes,
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            null // 触发默认路径生成
            );

    // 验证结果
    List<String> actualPaths = operator.getChildScanPaths();

    // 应该生成10个路径
    assertEquals("应该生成10个默认扫描路径", childCount, actualPaths.size());

    // 验证每个路径的格式和唯一性
    for (int i = 0; i < childCount; i++) {
      String expectedPath = "child_" + i + "_LargeJoin_001";
      assertEquals("第" + (i + 1) + "个子算子的路径格式应该正确", expectedPath, actualPaths.get(i));
    }

    // 验证所有路径都是唯一的
    assertEquals("所有路径应该都是唯一的", childCount, actualPaths.stream().distinct().count());
  }

  // ========== 辅助方法 ==========

  /** 创建模拟的OperatorContext，指定PlanNodeId */
  private OperatorContext createMockOperatorContext(String planNodeId) {
    OperatorContext context = Mockito.mock(OperatorContext.class);
    when(context.getPlanNodeId()).thenReturn(new PlanNodeId(planNodeId));
    return context;
  }

  /** 创建指定数量的模拟子算子 */
  private List<Operator> createMockChildren(int count) {
    List<Operator> children = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      children.add(Mockito.mock(Operator.class));
    }
    return children;
  }

  /** 创建模拟的输出列映射 */
  private Map<InputLocation, Integer> createMockOutputColumnMap() {
    Map<InputLocation, Integer> map = new HashMap<>();
    // 简单映射：第i个子算子的第0列映射到输出的第i列
    for (int i = 0; i < 10; i++) {
      map.put(new InputLocation(i, 0), i);
    }
    return map;
  }
}
