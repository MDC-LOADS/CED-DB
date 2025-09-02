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
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.ColumnMerger;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * FullOuterTimeJoinOperator扫描状态更新功能的单元测试类
 *
 * <p>该测试类验证FullOuterTimeJoinOperator中updateScanStates函数的正确性， 包括处理不同情况下的状态更新逻辑。
 */
public class FullOuterTimeJoinOperatorTest {

  /** 测试结束后清理QueryStateManager单例状态 */
  @After
  public void tearDown() {
    QueryStateManager.reset();
  }

  /**
   * 测试情况1：当子算子对应的inputTsBlocks[]为空，retainedTsBlock为空时的状态更新逻辑
   *
   * <p>预期行为：
   * 1. 当子算子对应的inputTsBlocks[]为空(null)或已消费完毕时
   * 2. 且retainedTsBlock也为空时
   * 3. scanOffset应该被设置为当前scanTimestamp的值
   * 4. isCouldEqual应该被设置为false
   * 5. isFullOuterJoin应该被设置为true（因为这是全外连接算子）
   */
  @Test
  public void testUpdateScanStatesWithEmptyInputAndEmptyRetainedTsBlocks() throws Exception {
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

    // 创建数据类型列表（两个子算子，每个算子一个INT32列）
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.INT32, TSDataType.INT32);

    // 创建FullOuterTimeJoinOperator实例，传入scanPaths
    FullOuterTimeJoinOperator operator =
        new FullOuterTimeJoinOperator(
            operatorContext,
            children,
            Ordering.ASC,
            dataTypes,
            createMockColumnMergers(dataTypes),
            new AscTimeComparator(),
            scanPaths);

    // 使用反射设置inputTsBlocks数组为空/null
    java.lang.reflect.Field inputTsBlocksField =
        FullOuterTimeJoinOperator.class.getSuperclass().getDeclaredField("inputTsBlocks");
    inputTsBlocksField.setAccessible(true);
    TsBlock[] inputTsBlocks = new TsBlock[2];
    inputTsBlocks[0] = null; // 第一个子算子的TsBlock为空
    inputTsBlocks[1] = null; // 第二个子算子的TsBlock为空
    inputTsBlocksField.set(operator, inputTsBlocks);

    // 使用反射设置inputIndex数组
    java.lang.reflect.Field inputIndexField =
        FullOuterTimeJoinOperator.class.getDeclaredField("inputIndex");
    inputIndexField.setAccessible(true);
    int[] inputIndex = new int[] {0, 0};
    inputIndexField.set(operator, inputIndex);

    // 为每个路径初始化ScanStates，设置初始的scanTimestamp
    long initialScanTimestamp = 1000L;
    for (String path : scanPaths) {
      QueryStateManager.ScanStates scanStates = new QueryStateManager.ScanStates();
      scanStates.setScanTimestamp(initialScanTimestamp);
      scanStates.setOffset(500L); // 设置一个不同的初始offset值
      stateManager.setScanStates(path, scanStates);
    }

    // 直接调用updateScanStates方法
    java.lang.reflect.Method updateMethod =
        FullOuterTimeJoinOperator.class.getDeclaredMethod("updateScanStates");
    updateMethod.setAccessible(true);
    updateMethod.invoke(operator);

    // 验证每个路径的ScanStates更新情况
    for (String path : scanPaths) {
      QueryStateManager.ScanStates scanStates = stateManager.getScanStates(path);
      assertNotNull("ScanStates不应该为null", scanStates);

      // 情况1验证：offset应该被设置为scanTimestamp
      assertEquals(
          "当inputTsBlocks为空时，offset应该被设置为scanTimestamp",
          initialScanTimestamp,
          scanStates.getOffset());

      // isCouldEqual应该为false
      assertFalse("当inputTsBlocks为空时，isCouldEqual应该为false", scanStates.isCouldEqual());

      // isFullOuterJoin应该为true
      assertTrue("应该标记为FullOuterJoin", scanStates.isFullOuterJoin());
    }
  }

  /**
   * 测试情况2：当子算子对应的inputTsBlocks[]为空，但retainedTsBlock不为空时的状态更新逻辑
   *
   * <p>预期行为：
   * 1. 当子算子对应的inputTsBlocks[]为空(null)或已消费完毕时
   * 2. 但retainedTsBlock不为空的情况下
   * 3. scanOffset应该被设置为retainedTsBlock中的最小时间戳
   * 4. isCouldEqual应该被设置为true
   * 5. isFullOuterJoin应该被设置为true
   */
  @Test
  public void testUpdateScanStatesWithEmptyInputAndNonEmptyRetained() throws Exception {
    // 初始化QueryStateManager单例
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext();

    // 定义扫描路径
    List<String> scanPaths = Arrays.asList("root.vehicle.d1.temperature");

    // 创建模拟的子算子
    Operator child1 = createMockOperator();
    List<Operator> children = Arrays.asList(child1);

    // 创建数据类型列表
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.INT32);

    // 创建FullOuterTimeJoinOperator实例
    FullOuterTimeJoinOperator operator =
        new FullOuterTimeJoinOperator(
            operatorContext,
            children,
            Ordering.ASC,
            dataTypes,
            createMockColumnMergers(dataTypes),
            new AscTimeComparator(),
            scanPaths);

    // 使用反射设置inputTsBlocks数组为空（情况2的条件）
    java.lang.reflect.Field inputTsBlocksField =
        FullOuterTimeJoinOperator.class.getSuperclass().getDeclaredField("inputTsBlocks");
    inputTsBlocksField.setAccessible(true);
    TsBlock[] inputTsBlocks = new TsBlock[1];
    inputTsBlocks[0] = null; // 第一个子算子的TsBlock为空
    inputTsBlocksField.set(operator, inputTsBlocks);

    // 使用反射设置inputIndex数组
    java.lang.reflect.Field inputIndexField =
        FullOuterTimeJoinOperator.class.getDeclaredField("inputIndex");
    inputIndexField.setAccessible(true);
    int[] inputIndex = new int[] {0}; // 指向索引0
    inputIndexField.set(operator, inputIndex);

    // 创建retainedTsBlock（情况2的条件）
    TsBlock retainedTsBlock = createTsBlock(new long[] {1800L, 1801L, 1802L}, new int[] {8, 9, 10});
    java.lang.reflect.Field retainedTsBlockField =
        FullOuterTimeJoinOperator.class.getSuperclass().getSuperclass().getDeclaredField("retainedTsBlock");
    retainedTsBlockField.setAccessible(true);
    retainedTsBlockField.set(operator, retainedTsBlock);

    // 期望的offset应该是retainedTsBlock的最小时间戳
    long expectedMinTime = 1800L;

    // 初始化ScanStates
    long initialScanTimestamp = 1000L;
    QueryStateManager.ScanStates scanStates = new QueryStateManager.ScanStates();
    scanStates.setScanTimestamp(initialScanTimestamp);
    scanStates.setOffset(500L); // 设置不同的初始offset
    stateManager.setScanStates(scanPaths.get(0), scanStates);

    // 直接调用updateScanStates方法
    java.lang.reflect.Method updateMethod =
        FullOuterTimeJoinOperator.class.getDeclaredMethod("updateScanStates");
    updateMethod.setAccessible(true);
    updateMethod.invoke(operator);

    // 验证ScanStates的更新
    QueryStateManager.ScanStates updatedStates = stateManager.getScanStates(scanPaths.get(0));
    assertNotNull("ScanStates不应该为null", updatedStates);

    // 情况2验证：当inputTsBlocks为空但retainedTsBlock不为空时，
    // offset应该被设置为retainedTsBlock中的最小时间戳
    assertEquals(
        "当inputTsBlocks为空且retainedTsBlock不为空时，offset应该为retainedTsBlock的最小时间戳",
        expectedMinTime,
        updatedStates.getOffset());

    // isCouldEqual应该为true
    assertTrue("当retainedTsBlock不为空时，isCouldEqual应该为true", updatedStates.isCouldEqual());

    // isFullOuterJoin应该为true
    assertTrue("应该标记为FullOuterJoin", updatedStates.isFullOuterJoin());
  }

  /**
   * 测试情况3：子算子对应inputTsBlocks[]不为空，retainedTsBlock为空时的状态更新逻辑
   *
   * <p>预期行为：
   * 1. 当子算子对应的inputTsBlocks[]不为空时
   * 2. 但retainedTsBlock为空的情况下
   * 3. scanOffset应该被设置为inputTsBlocks[i].getTimeByIndex(inputIndex[i])
   * 4. isCouldEqual应该被设置为true
   */
  @Test
  public void testUpdateScanStatesWithNonEmptyInputAndEmptyRetained() throws Exception {
    // 初始化QueryStateManager单例
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext();

    // 定义扫描路径
    List<String> scanPaths = Arrays.asList("root.vehicle.d1.temperature");

    // 创建模拟的子算子
    Operator child1 = createMockOperator();
    List<Operator> children = Arrays.asList(child1);

    // 创建数据类型列表
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.INT32);

    // 创建FullOuterTimeJoinOperator实例
    FullOuterTimeJoinOperator operator =
        new FullOuterTimeJoinOperator(
            operatorContext,
            children,
            Ordering.ASC,
            dataTypes,
            createMockColumnMergers(dataTypes),
            new AscTimeComparator(),
            scanPaths);

    // 创建有数据的TsBlock
    TsBlock inputBlock1 = createTsBlock(new long[] {2000L, 2001L, 2002L}, new int[] {1, 2, 3});

    // 使用反射设置inputTsBlocks数组（非空）
    java.lang.reflect.Field inputTsBlocksField =
        FullOuterTimeJoinOperator.class.getSuperclass().getDeclaredField("inputTsBlocks");
    inputTsBlocksField.setAccessible(true);
    TsBlock[] inputTsBlocks = new TsBlock[1];
    inputTsBlocks[0] = inputBlock1; // 第一个子算子有数据
    inputTsBlocksField.set(operator, inputTsBlocks);

    // 使用反射设置inputIndex数组
    java.lang.reflect.Field inputIndexField =
        FullOuterTimeJoinOperator.class.getDeclaredField("inputIndex");
    inputIndexField.setAccessible(true);
    int[] inputIndex = new int[] {1}; // 指向索引1，时间戳2001
    inputIndexField.set(operator, inputIndex);

    // 设置retainedTsBlock为null（情况3的条件）
    java.lang.reflect.Field retainedTsBlockField =
        FullOuterTimeJoinOperator.class.getSuperclass().getSuperclass().getDeclaredField("retainedTsBlock");
    retainedTsBlockField.setAccessible(true);
    retainedTsBlockField.set(operator, null);

    // 期望的offset应该是inputTsBlocks[0].getTimeByIndex(inputIndex[0]) = 2001L
    long expectedTime = 2001L;

    // 初始化ScanStates
    long initialScanTimestamp = 1000L;
    QueryStateManager.ScanStates scanStates = new QueryStateManager.ScanStates();
    scanStates.setScanTimestamp(initialScanTimestamp);
    scanStates.setOffset(500L); // 设置不同的初始offset
    stateManager.setScanStates(scanPaths.get(0), scanStates);

    // 直接调用updateScanStates方法
    java.lang.reflect.Method updateMethod =
        FullOuterTimeJoinOperator.class.getDeclaredMethod("updateScanStates");
    updateMethod.setAccessible(true);
    updateMethod.invoke(operator);

    // 验证ScanStates的更新
    QueryStateManager.ScanStates updatedStates = stateManager.getScanStates(scanPaths.get(0));
    assertNotNull("ScanStates不应该为null", updatedStates);

    // 情况3验证：当inputTsBlocks不为空但retainedTsBlock为空时，
    // offset应该被设置为inputTsBlocks[i].getTimeByIndex(inputIndex[i])
    assertEquals(
        "当inputTsBlocks不为空但retainedTsBlock为空时，offset应该为inputTsBlocks当前时间戳",
        expectedTime,
        updatedStates.getOffset());

    // isCouldEqual应该为true
    assertTrue("当inputTsBlocks不为空时，isCouldEqual应该为true", updatedStates.isCouldEqual());

    // isFullOuterJoin应该为true
    assertTrue("应该标记为FullOuterJoin", updatedStates.isFullOuterJoin());
  }

  /**
   * 测试情况4：子算子对应inputTsBlocks[]不为空，retainedTsBlock也不为空时的状态更新逻辑
   *
   * <p>预期行为：
   * 1. 当子算子对应的inputTsBlocks[]不为空时
   * 2. 且retainedTsBlock也不为空的情况下
   * 3. scanOffset应该被设置为retainedTsBlock中的最小时间戳
   * 4. isCouldEqual应该被设置为true
   */
  @Test
  public void testUpdateScanStatesWithNonEmptyInputAndNonEmptyRetained() throws Exception {
    // 初始化QueryStateManager单例
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext();

    // 定义扫描路径
    List<String> scanPaths = Arrays.asList("root.vehicle.d1.temperature");

    // 创建模拟的子算子
    Operator child1 = createMockOperator();
    List<Operator> children = Arrays.asList(child1);

    // 创建数据类型列表
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.INT32);

    // 创建FullOuterTimeJoinOperator实例
    FullOuterTimeJoinOperator operator =
        new FullOuterTimeJoinOperator(
            operatorContext,
            children,
            Ordering.ASC,
            dataTypes,
            createMockColumnMergers(dataTypes),
            new AscTimeComparator(),
            scanPaths);

    // 创建有数据的TsBlock
    TsBlock inputBlock1 = createTsBlock(new long[] {2000L, 2001L, 2002L}, new int[] {1, 2, 3});

    // 使用反射设置inputTsBlocks数组（非空）
    java.lang.reflect.Field inputTsBlocksField =
        FullOuterTimeJoinOperator.class.getSuperclass().getDeclaredField("inputTsBlocks");
    inputTsBlocksField.setAccessible(true);
    TsBlock[] inputTsBlocks = new TsBlock[1];
    inputTsBlocks[0] = inputBlock1; // 第一个子算子有数据
    inputTsBlocksField.set(operator, inputTsBlocks);

    // 使用反射设置inputIndex数组
    java.lang.reflect.Field inputIndexField =
        FullOuterTimeJoinOperator.class.getDeclaredField("inputIndex");
    inputIndexField.setAccessible(true);
    int[] inputIndex = new int[] {1}; // 指向索引1，时间戳2001
    inputIndexField.set(operator, inputIndex);

    // 创建retainedTsBlock（情况4的条件）
    TsBlock retainedTsBlock = createTsBlock(new long[] {1800L, 1801L, 1802L}, new int[] {8, 9, 10});
    java.lang.reflect.Field retainedTsBlockField =
        FullOuterTimeJoinOperator.class.getSuperclass().getSuperclass().getDeclaredField("retainedTsBlock");
    retainedTsBlockField.setAccessible(true);
    retainedTsBlockField.set(operator, retainedTsBlock);

    // 期望的offset应该是retainedTsBlock的最小时间戳
    long expectedMinTime = 1800L;

    // 初始化ScanStates
    long initialScanTimestamp = 1000L;
    QueryStateManager.ScanStates scanStates = new QueryStateManager.ScanStates();
    scanStates.setScanTimestamp(initialScanTimestamp);
    scanStates.setOffset(500L); // 设置不同的初始offset
    stateManager.setScanStates(scanPaths.get(0), scanStates);

    // 直接调用updateScanStates方法
    java.lang.reflect.Method updateMethod =
        FullOuterTimeJoinOperator.class.getDeclaredMethod("updateScanStates");
    updateMethod.setAccessible(true);
    updateMethod.invoke(operator);

    // 验证ScanStates的更新
    QueryStateManager.ScanStates updatedStates = stateManager.getScanStates(scanPaths.get(0));
    assertNotNull("ScanStates不应该为null", updatedStates);

    // 情况4验证：当inputTsBlocks和retainedTsBlock都不为空时，
    // offset应该被设置为retainedTsBlock中的最小时间戳
    assertEquals(
        "当inputTsBlocks和retainedTsBlock都不为空时，offset应该为retainedTsBlock的最小时间戳",
        expectedMinTime,
        updatedStates.getOffset());

    // isCouldEqual应该为true
    assertTrue("当inputTsBlocks不为空时，isCouldEqual应该为true", updatedStates.isCouldEqual());

    // isFullOuterJoin应该为true
    assertTrue("应该标记为FullOuterJoin", updatedStates.isFullOuterJoin());
  }

  /**
   * 测试情况5：验证SeriesScanUtil路径提取功能
   *
   * <p>预期行为：
   * 1. 当子算子包含SeriesScanUtil时
   * 2. 应该能够通过反射提取其中的seriesPath
   * 3. 如果没有提供childScanPaths，应该能够从子算子中自动提取
   */
  @Test
  public void testExtractSeriesPathFromChild() throws Exception {
    // 初始化QueryStateManager单例
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext();

    // 这个测试主要验证extractSeriesPathFromChild方法的功能
    // 由于SeriesScanUtil的复杂性，我们创建一个简化的模拟场景

    // 创建模拟的子算子
    Operator child1 = createMockOperator();
    List<Operator> children = Arrays.asList(child1);

    // 创建数据类型列表
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.INT32);

    // 创建FullOuterTimeJoinOperator实例，不提供scanPaths（让它自动提取）
    FullOuterTimeJoinOperator operator =
        new FullOuterTimeJoinOperator(
            operatorContext,
            children,
            Ordering.ASC,
            dataTypes,
            createMockColumnMergers(dataTypes),
            new AscTimeComparator());

    // 验证getChildScanPaths()方法
    List<String> childScanPaths = operator.getChildScanPaths();
    assertNotNull("childScanPaths不应该为null", childScanPaths);

    // 验证isStateTrackingAvailable()方法
    boolean isTrackingAvailable = operator.isStateTrackingAvailable();
    assertTrue("状态跟踪应该可用", isTrackingAvailable);

    // 验证getQueryStateManager()方法
    QueryStateManager retrievedStateManager = operator.getQueryStateManager();
    assertSame("应该返回同一个QueryStateManager实例", stateManager, retrievedStateManager);
  }

  /**
   * 测试当QueryStateManager未初始化时的防御性行为
   *
   * <p>预期行为：
   * 1. 当QueryStateManager未初始化时
   * 2. updateScanStates()方法应该安全地处理这种情况
   * 3. 不应该抛出异常
   */
  @Test
  public void testUpdateScanStatesWithoutQueryStateManager() throws Exception {
    // 确保QueryStateManager未初始化
    QueryStateManager.reset();
    assertFalse("QueryStateManager应该未初始化", QueryStateManager.isInitialized());

    OperatorContext operatorContext = createMockOperatorContext();

    // 创建模拟的子算子
    Operator child1 = createMockOperator();
    List<Operator> children = Arrays.asList(child1);

    // 创建数据类型列表
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.INT32);

    // 创建FullOuterTimeJoinOperator实例
    FullOuterTimeJoinOperator operator =
        new FullOuterTimeJoinOperator(
            operatorContext,
            children,
            Ordering.ASC,
            dataTypes,
            createMockColumnMergers(dataTypes),
            new AscTimeComparator());

    // 模拟子算子没有数据
    when(child1.hasNextWithTimer()).thenReturn(false);
    when(child1.nextWithTimer()).thenReturn(null);
    when(child1.isFinished()).thenReturn(true);

    // 调用next()，应该不抛出异常
    TsBlock result = operator.next();

    // 验证：没有抛出异常，说明防御性编程生效
    assertNull("当没有数据时应该返回null", result);
  }

  /**
   * 测试情况4：验证SeriesScanUtil路径提取功能（基础方法测试）
   *
   * <p>预期行为：
   * 1. 验证operator的基础方法功能
   * 2. 确保状态管理方法正常工作
   */
  @Test
  public void testBasicMethodFunctionality() throws Exception {
    // 初始化QueryStateManager单例
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext();

    // 创建模拟的子算子
    Operator child1 = createMockOperator();
    List<Operator> children = Arrays.asList(child1);

    // 创建数据类型列表
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.INT32);

    // 创建FullOuterTimeJoinOperator实例，不提供scanPaths（让它自动提取）
    FullOuterTimeJoinOperator operator =
        new FullOuterTimeJoinOperator(
            operatorContext,
            children,
            Ordering.ASC,
            dataTypes,
            createMockColumnMergers(dataTypes),
            new AscTimeComparator());

    // 验证getChildScanPaths()方法
    List<String> childScanPaths = operator.getChildScanPaths();
    assertNotNull("childScanPaths不应该为null", childScanPaths);

    // 验证isStateTrackingAvailable()方法
    boolean isTrackingAvailable = operator.isStateTrackingAvailable();
    assertTrue("状态跟踪应该可用", isTrackingAvailable);

    // 验证getQueryStateManager()方法
    QueryStateManager retrievedStateManager = operator.getQueryStateManager();
    assertSame("应该返回同一个QueryStateManager实例", stateManager, retrievedStateManager);
  }

  // ====================== 辅助方法 ======================

  /**
   * 创建模拟的OperatorContext
   *
   * @return 模拟的OperatorContext实例
   */
  private OperatorContext createMockOperatorContext() {
    OperatorContext context = Mockito.mock(OperatorContext.class);
    when(context.getMaxRunTime()).thenReturn(new Duration(1, TimeUnit.SECONDS));
    when(context.getPlanNodeId()).thenReturn(new PlanNodeId("test"));
    return context;
  }

  /**
   * 创建模拟的Operator
   *
   * @return 模拟的Operator实例
   */
  private Operator createMockOperator() {
    Operator operator = Mockito.mock(Operator.class);
    when(operator.isBlocked()).thenReturn(Mockito.mock(ListenableFuture.class));
    when(operator.calculateMaxPeekMemoryWithCounter()).thenReturn(0L);
    when(operator.calculateMaxReturnSize()).thenReturn(0L);
    when(operator.calculateRetainedSizeAfterCallingNext()).thenReturn(0L);
    return operator;
  }

  /**
   * 创建测试用的TsBlock
   *
   * @param timestamps 时间戳数组
   * @param values 数值数组
   * @return 创建的TsBlock
   */
  private TsBlock createTsBlock(long[] timestamps, int[] values) {
    if (timestamps.length != values.length) {
      throw new IllegalArgumentException("timestamps和values数组长度必须相同");
    }

    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.INT32));
    TimeColumnBuilder timeBuilder = builder.getTimeColumnBuilder();

    for (int i = 0; i < timestamps.length; i++) {
      timeBuilder.writeLong(timestamps[i]);
      builder.getColumnBuilder(0).writeInt(values[i]);
      builder.declarePosition();
    }

    return builder.build();
  }

  /**
   * 创建模拟的ColumnMerger列表
   *
   * @param dataTypes 数据类型列表
   * @return 模拟的ColumnMerger列表
   */
  private List<ColumnMerger> createMockColumnMergers(List<TSDataType> dataTypes) {
    List<ColumnMerger> mergers = new ArrayList<>();
    for (TSDataType dataType : dataTypes) {
      ColumnMerger merger = Mockito.mock(ColumnMerger.class);
      mergers.add(merger);
    }
    return mergers;
  }
}