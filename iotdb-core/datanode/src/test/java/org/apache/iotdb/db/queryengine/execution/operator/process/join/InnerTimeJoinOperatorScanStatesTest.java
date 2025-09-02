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
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * InnerTimeJoinOperator扫描状态更新功能的单元测试类
 *
 * <p>该测试类验证InnerTimeJoinOperator中updateScanStates函数的正确性， 包括处理空TsBlock和非空TsBlock的不同情况。
 */
public class InnerTimeJoinOperatorScanStatesTest {

  /** 测试结束后清理QueryStateManager单例状态 */
  @After
  public void tearDown() {
    QueryStateManager.reset();
  }

  /**
   * 测试当inputTsBlocks为空时的状态更新逻辑
   *
   * <p>预期行为： 1. 当子算子对应的inputTsBlocks[]为空(null)时 2. offset应该被设置为当前scanTimestamp的值 3.
   * isCouldEqual应该被设置为false 4. isInnerJoin应该被设置为true（因为这是内连接算子）
   */
  @Test
  public void testUpdateScanStatesWithEmptyInputTsBlocks() {
    // 初始化QueryStateManager单例
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext();

    // 定义两个扫描路径，模拟两个sensor的数据
    List<String> scanPaths =
        Arrays.asList(
            "root.vehicle.d1.temperature", // 温度传感器路径
            "root.vehicle.d2.speed" // 速度传感器路径
            );

    // 创建模拟的子算子
    Operator child1 = createMockOperator();
    Operator child2 = createMockOperator();
    List<Operator> children = Arrays.asList(child1, child2);

    // 创建InnerTimeJoinOperator（QueryStateManager现在是单例）
    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            scanPaths);

    // 为两个扫描路径初始化状态，设置初始的scanTimestamp
    QueryStateManager.ScanStates scanStates1 = new QueryStateManager.ScanStates();
    scanStates1.setScanTimestamp(1000L); // 温度传感器的初始时间戳
    stateManager.setScanStates(scanPaths.get(0), scanStates1);

    QueryStateManager.ScanStates scanStates2 = new QueryStateManager.ScanStates();
    scanStates2.setScanTimestamp(2000L); // 速度传感器的初始时间戳
    stateManager.setScanStates(scanPaths.get(1), scanStates2);

    // 执行测试：当inputTsBlocks为空时调用updateScanStates
    // 使用反射调用私有方法updateScanStates
    try {
      java.lang.reflect.Method updateMethod =
          InnerTimeJoinOperator.class.getDeclaredMethod("updateScanStates");
      updateMethod.setAccessible(true);
      updateMethod.invoke(operator);
    } catch (Exception e) {
      fail("调用updateScanStates失败: " + e.getMessage());
    }

    // 验证第一个传感器的状态更新结果
    QueryStateManager.ScanStates updatedStates1 = stateManager.getScanStates(scanPaths.get(0));
    assertEquals("当inputTsBlock为空时，offset应该被设置为scanTimestamp", 1000L, updatedStates1.getOffset());
    assertFalse("当inputTsBlock为空时，isCouldEqual应该为false", updatedStates1.isCouldEqual());
    assertTrue("内连接算子的isInnerJoin应该为true", updatedStates1.isInnerJoin());

    // 验证第二个传感器的状态更新结果
    QueryStateManager.ScanStates updatedStates2 = stateManager.getScanStates(scanPaths.get(1));
    assertEquals("当inputTsBlock为空时，offset应该被设置为scanTimestamp", 2000L, updatedStates2.getOffset());
    assertFalse("当inputTsBlock为空时，isCouldEqual应该为false", updatedStates2.isCouldEqual());
    assertTrue("内连接算子的isInnerJoin应该为true", updatedStates2.isInnerJoin());
  }

  /**
   * 测试当inputTsBlocks非空时的状态更新逻辑
   *
   * <p>预期行为： 1. 当子算子对应的inputTsBlocks[]不为空时 2. offset应该被设置为inputTsBlocks[inputIndex[i]]对应的时间戳 3.
   * isCouldEqual应该被设置为true 4. isInnerJoin应该被设置为true
   */
  @Test
  public void testUpdateScanStatesWithNonEmptyInputTsBlocks() throws Exception {
    // 初始化QueryStateManager单例
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext();

    // 定义扫描路径
    List<String> scanPaths =
        Arrays.asList(
            "root.vehicle.d1.temperature", // 温度传感器路径
            "root.vehicle.d2.speed" // 速度传感器路径
            );

    // 创建模拟的子算子
    Operator child1 = createMockOperator();
    Operator child2 = createMockOperator();
    List<Operator> children = Arrays.asList(child1, child2);

    // 创建InnerTimeJoinOperator（QueryStateManager现在是单例）
    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            scanPaths);

    // 创建包含时间戳数据的模拟TsBlocks
    // tsBlock1包含时间戳: [1000, 1001, 1002]
    TsBlock tsBlock1 = createMockTsBlock(new long[] {1000, 1001, 1002});
    // tsBlock2包含时间戳: [2000, 2001, 2002]
    TsBlock tsBlock2 = createMockTsBlock(new long[] {2000, 2001, 2002});

    // 使用反射设置inputTsBlocks数组
    java.lang.reflect.Field inputTsBlocksField =
        InnerTimeJoinOperator.class.getDeclaredField("inputTsBlocks");
    inputTsBlocksField.setAccessible(true);
    TsBlock[] inputTsBlocks = new TsBlock[2];
    inputTsBlocks[0] = tsBlock1; // 第一个子算子的TsBlock
    inputTsBlocks[1] = tsBlock2; // 第二个子算子的TsBlock
    inputTsBlocksField.set(operator, inputTsBlocks);

    // 使用反射设置inputIndex数组，指向不同的处理位置
    java.lang.reflect.Field inputIndexField =
        InnerTimeJoinOperator.class.getDeclaredField("inputIndex");
    inputIndexField.setAccessible(true);
    int[] inputIndex = new int[] {1, 2}; // 第一个算子指向索引1(时间戳1001)，第二个算子指向索引2(时间戳2002)
    inputIndexField.set(operator, inputIndex);

    // 初始化扫描状态，设置初始scanTimestamp
    QueryStateManager.ScanStates scanStates1 = new QueryStateManager.ScanStates();
    scanStates1.setScanTimestamp(999L); // 温度传感器的初始时间戳
    stateManager.setScanStates(scanPaths.get(0), scanStates1);

    QueryStateManager.ScanStates scanStates2 = new QueryStateManager.ScanStates();
    scanStates2.setScanTimestamp(1999L); // 速度传感器的初始时间戳
    stateManager.setScanStates(scanPaths.get(1), scanStates2);

    // 执行测试：当inputTsBlocks非空时调用updateScanStates
    java.lang.reflect.Method updateMethod =
        InnerTimeJoinOperator.class.getDeclaredMethod("updateScanStates");
    updateMethod.setAccessible(true);
    updateMethod.invoke(operator);

    // 验证第一个传感器的状态更新结果
    // 应该使用inputIndex[0]=1对应的时间戳1001
    QueryStateManager.ScanStates updatedStates1 = stateManager.getScanStates(scanPaths.get(0));
    assertEquals("offset应该被设置为inputIndex[0]对应的时间戳(1001)", 1001L, updatedStates1.getOffset());
    assertTrue("当inputTsBlock不为空时，isCouldEqual应该为true", updatedStates1.isCouldEqual());
    assertTrue("内连接算子的isInnerJoin应该为true", updatedStates1.isInnerJoin());

    // 验证第二个传感器的状态更新结果
    // 应该使用inputIndex[1]=2对应的时间戳2002
    QueryStateManager.ScanStates updatedStates2 = stateManager.getScanStates(scanPaths.get(1));
    assertEquals("offset应该被设置为inputIndex[1]对应的时间戳(2002)", 2002L, updatedStates2.getOffset());
    assertTrue("当inputTsBlock不为空时，isCouldEqual应该为true", updatedStates2.isCouldEqual());
    assertTrue("内连接算子的isInnerJoin应该为true", updatedStates2.isInnerJoin());
  }

  /**
   * 测试当inputTsBlocks部分为空、部分非空时的混合状态更新逻辑
   *
   * <p>预期行为： 1. 对于非空的inputTsBlocks[i]，offset设置为时间戳，isCouldEqual为true 2.
   * 对于空的inputTsBlocks[i]，offset设置为scanTimestamp，isCouldEqual为false 3. 这种混合情况在实际查询中很常见，需要正确处理
   */
  @Test
  public void testUpdateScanStatesWithMixedInputTsBlocks() throws Exception {
    // 初始化QueryStateManager单例
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext();

    // 定义扫描路径
    List<String> scanPaths =
        Arrays.asList(
            "root.vehicle.d1.temperature", // 温度传感器路径
            "root.vehicle.d2.speed" // 速度传感器路径
            );

    // 创建模拟的子算子
    Operator child1 = createMockOperator();
    Operator child2 = createMockOperator();
    List<Operator> children = Arrays.asList(child1, child2);

    // 创建InnerTimeJoinOperator（QueryStateManager现在是单例）
    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            scanPaths);

    // 创建混合状态：一个TsBlock有数据，另一个为空
    TsBlock tsBlock1 = createMockTsBlock(new long[] {1000, 1001, 1002}); // 温度数据存在

    // 使用反射设置混合状态的inputTsBlocks数组
    java.lang.reflect.Field inputTsBlocksField =
        InnerTimeJoinOperator.class.getDeclaredField("inputTsBlocks");
    inputTsBlocksField.setAccessible(true);
    TsBlock[] inputTsBlocks = new TsBlock[2];
    inputTsBlocks[0] = tsBlock1; // 第一个子算子有数据
    inputTsBlocks[1] = null; // 第二个子算子为空（可能数据还未到达）
    inputTsBlocksField.set(operator, inputTsBlocks);

    // 设置inputIndex数组
    java.lang.reflect.Field inputIndexField =
        InnerTimeJoinOperator.class.getDeclaredField("inputIndex");
    inputIndexField.setAccessible(true);
    int[] inputIndex = new int[] {1, 0}; // 第一个算子指向索引1，第二个算子索引为0但TsBlock为null
    inputIndexField.set(operator, inputIndex);

    // 初始化扫描状态
    QueryStateManager.ScanStates scanStates1 = new QueryStateManager.ScanStates();
    scanStates1.setScanTimestamp(999L); // 温度传感器的初始时间戳
    stateManager.setScanStates(scanPaths.get(0), scanStates1);

    QueryStateManager.ScanStates scanStates2 = new QueryStateManager.ScanStates();
    scanStates2.setScanTimestamp(1999L); // 速度传感器的初始时间戳
    stateManager.setScanStates(scanPaths.get(1), scanStates2);

    // 执行测试：调用updateScanStates处理混合状态
    java.lang.reflect.Method updateMethod =
        InnerTimeJoinOperator.class.getDeclaredMethod("updateScanStates");
    updateMethod.setAccessible(true);
    updateMethod.invoke(operator);

    // 验证第一个传感器（有数据）的状态更新结果
    // 应该使用inputIndex[0]=1对应的时间戳1001，isCouldEqual为true
    QueryStateManager.ScanStates updatedStates1 = stateManager.getScanStates(scanPaths.get(0));
    assertEquals("有数据的传感器offset应该被设置为inputIndex[0]对应的时间戳(1001)", 1001L, updatedStates1.getOffset());
    assertTrue("有数据的传感器isCouldEqual应该为true", updatedStates1.isCouldEqual());

    // 验证第二个传感器（无数据）的状态更新结果
    // 应该使用scanTimestamp，isCouldEqual为false
    QueryStateManager.ScanStates updatedStates2 = stateManager.getScanStates(scanPaths.get(1));
    assertEquals("无数据的传感器offset应该被设置为scanTimestamp", 1999L, updatedStates2.getOffset());
    assertFalse("无数据的传感器isCouldEqual应该为false", updatedStates2.isCouldEqual());
  }

  /**
   * 测试当QueryStateManager单例未初始化时的异常处理
   *
   * <p>预期行为： 1. 当QueryStateManager单例未初始化时，updateScanStates应该安全返回 2. 不应该抛出IllegalStateException或其他异常
   * 3. 这保证了向后兼容性和健壮性
   */
  @Test
  public void testUpdateScanStatesWithUninitializedQueryStateManager() throws Exception {
    // 确保QueryStateManager单例未初始化
    QueryStateManager.reset();

    OperatorContext operatorContext = createMockOperatorContext();

    Operator child1 = createMockOperator();
    Operator child2 = createMockOperator();
    List<Operator> children = Arrays.asList(child1, child2);

    // 创建算子，此时QueryStateManager单例未初始化
    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap());

    // 测试：当QueryStateManager单例未初始化时调用updateScanStates应该不会抛异常
    java.lang.reflect.Method updateMethod =
        InnerTimeJoinOperator.class.getDeclaredMethod("updateScanStates");
    updateMethod.setAccessible(true);

    // 应该安全执行，不抛任何异常
    updateMethod.invoke(operator);
  }

  /**
   * 测试QueryStateManager单例的获取功能
   *
   * <p>验证： 1. 当单例未初始化时，getQueryStateManager()应该返回null 2. 当单例已初始化时，应该能正确获取实例 3.
   * isStateTrackingAvailable()方法的正确性
   */
  @Test
  public void testGetQueryStateManagerSingleton() {
    // 确保单例未初始化
    QueryStateManager.reset();

    OperatorContext operatorContext = createMockOperatorContext();

    Operator child1 = createMockOperator();
    Operator child2 = createMockOperator();
    List<Operator> children = Arrays.asList(child1, child2);

    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap());

    // 测试：单例未初始化时应该返回null
    assertNull("单例未初始化时QueryStateManager应该为null", operator.getQueryStateManager());
    assertFalse("单例未初始化时状态跟踪应该不可用", operator.isStateTrackingAvailable());

    // 初始化单例
    QueryStateManager stateManager = QueryStateManager.initialize();

    // 测试：单例已初始化时应该返回实例
    assertEquals("单例已初始化时应该返回正确的实例", stateManager, operator.getQueryStateManager());
    assertTrue("单例已初始化时状态跟踪应该可用", operator.isStateTrackingAvailable());
  }

  /**
   * 测试子算子扫描路径的获取功能
   *
   * <p>验证： 1. 获取的路径应该与设置的路径相同 2. 返回的是路径列表的副本，修改不会影响原始数据 3. 数据封装和安全性的正确性
   */
  @Test
  public void testGetChildScanPaths() {
    // 初始化QueryStateManager单例（用于默认路径生成测试）
    QueryStateManager.reinitialize();

    OperatorContext operatorContext = createMockOperatorContext();
    List<String> originalScanPaths =
        Arrays.asList(
            "root.vehicle.d1.temperature", // 原始温度传感器路径
            "root.vehicle.d2.speed" // 原始速度传感器路径
            );

    Operator child1 = createMockOperator();
    Operator child2 = createMockOperator();
    List<Operator> children = Arrays.asList(child1, child2);

    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            originalScanPaths);

    // 测试：获取子算子扫描路径应该返回相同的路径
    List<String> retrievedPaths = operator.getChildScanPaths();
    assertEquals("返回的路径应该与原始路径相同", originalScanPaths, retrievedPaths);

    // 测试：修改返回的列表不应该影响原始数据（确保返回的是副本）
    retrievedPaths.add("new.path");
    List<String> retrievedPaths2 = operator.getChildScanPaths();
    assertEquals("原始路径应该保持不变", originalScanPaths, retrievedPaths2);
  }

  // ========== 辅助方法 ==========

  /**
   * 创建模拟的OperatorContext
   *
   * @return 配置好的mock OperatorContext
   */
  private OperatorContext createMockOperatorContext() {
    OperatorContext context = Mockito.mock(OperatorContext.class);
    when(context.getPlanNodeId()).thenReturn(new PlanNodeId("test_plan_node"));
    return context;
  }

  /**
   * 创建模拟的Operator
   *
   * @return mock Operator
   */
  private Operator createMockOperator() {
    return Mockito.mock(Operator.class);
  }

  /**
   * 创建模拟的输出列映射
   *
   * @return 包含基本映射关系的Map
   */
  private Map<InputLocation, Integer> createMockOutputColumnMap() {
    Map<InputLocation, Integer> map = new HashMap<>();
    map.put(new InputLocation(0, 0), 0); // 第一个子算子的第一列映射到输出的第0列
    map.put(new InputLocation(1, 0), 1); // 第二个子算子的第一列映射到输出的第1列
    return map;
  }

  /**
   * 创建包含指定时间戳的模拟TsBlock
   *
   * @param timestamps 要包含的时间戳数组
   * @return 构造好的TsBlock
   */
  private TsBlock createMockTsBlock(long[] timestamps) {
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.DOUBLE));
    for (long timestamp : timestamps) {
      builder.getTimeColumnBuilder().writeLong(timestamp);
      builder.getColumnBuilder(0).writeDouble(Math.random()); // 添加随机双精度数据
      builder.declarePosition();
    }
    return builder.build();
  }
}
