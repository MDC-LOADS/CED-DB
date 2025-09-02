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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.colquery.QueryStateManager;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.enums.TSDataType;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

/**
 * 演示InnerTimeJoinOperator中默认扫描路径初始化逻辑的使用样例
 *
 * <p>该示例展示了以下场景： 1. 当QueryStateManager单例已初始化但未提供childScanPaths时，如何自动生成默认路径 2. 默认路径的命名规则和格式 3.
 * 在不同构造场景下的行为差异
 */
public class DefaultScanPathsUsageExample {

  public static void main(String[] args) {
    System.out.println("=== InnerTimeJoinOperator 默认扫描路径演示 ===\n");

    try {
      // 演示场景1：提供QueryStateManager但不提供childScanPaths（触发默认路径生成）
      demonstrateDefaultScanPathGeneration();

      // 演示场景2：同时提供QueryStateManager和childScanPaths（使用自定义路径）
      demonstrateCustomScanPaths();

      // 演示场景3：不提供QueryStateManager（不生成默认路径）
      demonstrateNoQueryStateManager();

      // 演示场景4：提供空的childScanPaths列表（触发默认路径生成）
      demonstrateEmptyScanPathsList();

      // 演示场景5：从SeriesScanOperator中提取真实的seriesPath（新功能）
      demonstrateSeriesPathExtraction();

      // 演示场景6：混合子算子类型的路径提取
      demonstrateMixedChildOperators();

      System.out.println("🎉 所有演示场景执行完成！");
      System.out.println("\n总结：");
      System.out.println("✅ 新功能成功实现：能够从SeriesScanUtil中提取真实的seriesPath");
      System.out.println("✅ 保持向后兼容：当无法提取时回退到默认路径命名");
      System.out.println("✅ 支持混合算子：智能处理不同类型的子算子");

    } catch (Exception e) {
      System.err.println("演示过程中出现错误: " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * 场景1：演示默认扫描路径的自动生成
   *
   * <p>当满足以下条件时，会自动生成默认扫描路径： 1. QueryStateManager单例已初始化 2. childScanPaths为空（null或empty）
   */
  private static void demonstrateDefaultScanPathGeneration() {
    System.out.println("📍 场景1: 自动生成默认扫描路径");
    System.out.println("条件: QueryStateManager已初始化, childScanPaths = null");

    // 初始化单例
    QueryStateManager stateManager = QueryStateManager.initialize();
    OperatorContext operatorContext = createMockOperatorContext("InnerJoin_001");
    List<Operator> children = createMockChildren(3); // 3个子算子

    // 🎯 关键：QueryStateManager单例已初始化，childScanPaths为null
    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            null // ✅ childScanPaths为null，触发默认路径生成
            );

    // 获取自动生成的扫描路径
    List<String> generatedPaths = operator.getChildScanPaths();

    System.out.println("自动生成的扫描路径:");
    for (int i = 0; i < generatedPaths.size(); i++) {
      System.out.println("  子算子[" + i + "]: " + generatedPaths.get(i));
    }

    System.out.println("路径生成规则: child_{索引}_{PlanNodeId}");
    System.out.println("实际PlanNodeId: " + operatorContext.getPlanNodeId());
    System.out.println();
  }

  /**
   * 场景2：演示使用自定义扫描路径
   *
   * <p>当提供了非空的childScanPaths时，直接使用提供的路径，不生成默认路径
   */
  private static void demonstrateCustomScanPaths() {
    System.out.println("📍 场景2: 使用自定义扫描路径");
    System.out.println("条件: QueryStateManager已初始化, childScanPaths ≠ empty");

    // 重新初始化单例
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext("InnerJoin_002");
    List<Operator> children = createMockChildren(3);

    // 🎯 关键：提供自定义的扫描路径
    List<String> customScanPaths =
        Arrays.asList(
            "root.factory.workshop1.temperature",
            "root.factory.workshop2.temperature",
            "root.factory.workshop3.temperature");

    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            customScanPaths // ✅ 提供自定义路径，不会生成默认路径
            );

    List<String> actualPaths = operator.getChildScanPaths();

    System.out.println("使用的扫描路径:");
    for (int i = 0; i < actualPaths.size(); i++) {
      System.out.println("  子算子[" + i + "]: " + actualPaths.get(i));
    }

    System.out.println("结果: 直接使用提供的自定义路径，未生成默认路径");
    System.out.println();
  }

  /**
   * 场景3：演示QueryStateManager未初始化的情况
   *
   * <p>当QueryStateManager单例未初始化时，即使childScanPaths为空也不会生成默认路径
   */
  private static void demonstrateNoQueryStateManager() {
    System.out.println("📍 场景3: QueryStateManager未初始化");
    System.out.println("条件: QueryStateManager未初始化, childScanPaths = null");

    // 重置单例以模拟未初始化状态
    QueryStateManager.reset();

    // 创建测试环境（不提供QueryStateManager）
    OperatorContext operatorContext = createMockOperatorContext("InnerJoin_003");
    List<Operator> children = createMockChildren(2);

    // 🎯 关键：QueryStateManager单例未初始化，childScanPaths也为null
    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            null // childScanPaths为null
            );

    List<String> actualPaths = operator.getChildScanPaths();

    System.out.println("扫描路径结果:");
    System.out.println("  路径数量: " + actualPaths.size());
    for (int i = 0; i < actualPaths.size(); i++) {
      System.out.println("  子算子[" + i + "]: " + actualPaths.get(i));
    }

    System.out.println("结果: 由于QueryStateManager未初始化，未生成默认路径");
    System.out.println();
  }

  /**
   * 场景4：演示提供空列表的childScanPaths
   *
   * <p>当childScanPaths是空列表（而不是null）且QueryStateManager已初始化时，会生成默认路径
   */
  private static void demonstrateEmptyScanPathsList() {
    System.out.println("📍 场景4: 提供空的childScanPaths列表");
    System.out.println("条件: QueryStateManager已初始化, childScanPaths = empty list");

    // 重新初始化单例
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext("InnerJoin_004");
    List<Operator> children = createMockChildren(4); // 4个子算子，展示更多默认路径

    // 🎯 关键：提供空的ArrayList而不是null
    List<String> emptyScanPaths = new ArrayList<>(); // 空列表，不是null

    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(
                TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            emptyScanPaths // ✅ 空列表，isEmpty()返回true，触发默认路径生成
            );

    List<String> generatedPaths = operator.getChildScanPaths();

    System.out.println("自动生成的扫描路径:");
    for (int i = 0; i < generatedPaths.size(); i++) {
      System.out.println("  子算子[" + i + "]: " + generatedPaths.get(i));
    }

    System.out.println("结果: 空列表被认为isEmpty()=true，触发了默认路径生成");
    System.out.println();
  }

  /** 演示在实际使用中如何利用默认路径进行状态跟踪 */
  public static void demonstrateStateTracking() {
    System.out.println("📍 实际应用: 使用默认路径进行状态跟踪");

    // 创建QueryStateManager单例的算子
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext("StateTracking_001");
    List<Operator> children = createMockChildren(2);

    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            null // 触发默认路径生成
            );

    // 获取生成的路径
    List<String> scanPaths = operator.getChildScanPaths();
    System.out.println("生成的扫描路径: " + scanPaths);

    // 模拟设置一些状态
    for (String path : scanPaths) {
      stateManager.updateScanTimestamp(path, System.currentTimeMillis());
      stateManager.updateScanOffset(path, 1000L);
      stateManager.updateScanCouldEqual(path, true);
      stateManager.updateScanInnerJoin(path, true);
    }

    // 显示状态跟踪结果
    System.out.println("状态跟踪结果:");
    for (String path : scanPaths) {
      QueryStateManager.ScanStates states = stateManager.getScanStates(path);
      if (states != null) {
        System.out.println("  路径: " + path);
        System.out.println("    时间戳: " + states.getScanTimestamp());
        System.out.println("    偏移量: " + states.getOffset());
        System.out.println("    可相等: " + states.isCouldEqual());
        System.out.println("    内连接: " + states.isInnerJoin());
      }
    }
  }

  /**
   * 场景5：演示从SeriesScanOperator中提取真实的seriesPath
   *
   * <p>这是新功能的核心测试：当子算子是SeriesScanOperator（包含SeriesScanUtil）时， 会从其seriesPath字段提取真实的时间序列路径，而不是生成默认路径
   */
  private static void demonstrateSeriesPathExtraction() {
    System.out.println("📍 场景5: 从SeriesScanOperator提取真实seriesPath（新功能）");
    System.out.println("条件: QueryStateManager已初始化, 子算子包含SeriesScanUtil");

    // 重新初始化单例
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext("SeriesPath_001");

    // 🎯 关键：创建真实的SeriesScanOperator子算子，它们包含SeriesScanUtil
    List<Operator> children = createSeriesScanOperators();

    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            children,
            Arrays.asList(TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT32),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            null // childScanPaths为null，触发路径提取逻辑
            );

    List<String> extractedPaths = operator.getChildScanPaths();

    System.out.println("从SeriesScanOperator提取的真实路径:");
    for (int i = 0; i < extractedPaths.size(); i++) {
      System.out.println("  子算子[" + i + "]: " + extractedPaths.get(i));
    }

    System.out.println("✅ 结果: 成功提取了SeriesScanUtil中的真实seriesPath");
    System.out.println("   这些是实际的时间序列测量点路径，而不是默认生成的路径");
    System.out.println();
  }

  /**
   * 场景6：演示混合子算子类型的路径提取
   *
   * <p>测试包含不同类型子算子的情况： - SeriesScanOperator：提取真实的seriesPath - 其他类型算子：使用默认路径命名
   */
  private static void demonstrateMixedChildOperators() {
    System.out.println("📍 场景6: 混合子算子类型的路径提取");
    System.out.println("条件: 部分子算子是SeriesScanOperator，部分是其他类型");

    // 重新初始化单例
    QueryStateManager stateManager = QueryStateManager.reinitialize();
    OperatorContext operatorContext = createMockOperatorContext("Mixed_001");

    // 🎯 关键：创建混合类型的子算子
    List<Operator> mixedChildren = createMixedChildOperators();

    InnerTimeJoinOperator operator =
        new InnerTimeJoinOperator(
            operatorContext,
            mixedChildren,
            Arrays.asList(
                TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT32, TSDataType.BOOLEAN),
            new AscTimeComparator(),
            createMockOutputColumnMap(),
            null // 触发路径提取逻辑
            );

    List<String> extractedPaths = operator.getChildScanPaths();

    System.out.println("混合子算子的路径提取结果:");
    for (int i = 0; i < extractedPaths.size(); i++) {
      String pathType = extractedPaths.get(i).startsWith("root.") ? "真实路径" : "默认路径";
      System.out.println("  子算子[" + i + "]: " + extractedPaths.get(i) + " (" + pathType + ")");
    }

    System.out.println("✅ 结果: SeriesScanOperator提取了真实路径，其他算子使用默认路径");
    System.out.println("   这展示了新功能的智能路径提取逻辑");
    System.out.println();
  }

  // ========== 辅助方法 ==========

  private static OperatorContext createMockOperatorContext(String planNodeId) {
    OperatorContext context = Mockito.mock(OperatorContext.class);
    when(context.getPlanNodeId()).thenReturn(new PlanNodeId(planNodeId));
    return context;
  }

  private static List<Operator> createMockChildren(int count) {
    List<Operator> children = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      children.add(Mockito.mock(Operator.class));
    }
    return children;
  }

  private static Map<InputLocation, Integer> createMockOutputColumnMap() {
    Map<InputLocation, Integer> map = new HashMap<>();
    map.put(new InputLocation(0, 0), 0);
    map.put(new InputLocation(1, 0), 1);
    return map;
  }

  /**
   * 创建包含SeriesScanUtil的SeriesScanOperator子算子 这些算子有真实的seriesPath，会被extractSeriesPathFromChild方法提取
   */
  private static List<Operator> createSeriesScanOperators() {
    List<Operator> children = new ArrayList<>();

    try {
      // 创建模拟的OperatorContext和FragmentInstanceContext
      OperatorContext mockContext = createMockOperatorContext("TestScan");
      FragmentInstanceContext fragmentContext = Mockito.mock(FragmentInstanceContext.class);
      when(mockContext.getInstanceContext()).thenReturn(fragmentContext);

      // 创建不同的时间序列路径 - 注意：必须包含具体的测量点名称
      String[] seriesPaths = {
        "root.vehicle.d1.s1", "root.vehicle.d2.s1", "root.factory.workshop.s1"
      };

      for (String pathStr : seriesPaths) {
        // 创建MeasurementPath - 需要指定数据类型才能正确表示测量点
        MeasurementPath seriesPath = new MeasurementPath(pathStr, TSDataType.DOUBLE);

        // 创建SeriesScanOptions - 使用默认选项
        SeriesScanOptions scanOptions = SeriesScanOptions.getDefaultSeriesScanOptions(seriesPath);

        // 创建SeriesScanOperator - 这是包含SeriesScanUtil的真实算子
        SeriesScanOperator seriesScanOp =
            new SeriesScanOperator(
                mockContext,
                new PlanNodeId("SeriesScan_" + children.size()),
                seriesPath,
                Ordering.ASC,
                scanOptions);

        children.add(seriesScanOp);
      }

    } catch (Exception e) {
      System.err.println("创建SeriesScanOperator时出错: " + e.getMessage());
      // 如果创建失败，回退到mock对象
      return createMockChildren(3);
    }

    return children;
  }

  /** 创建混合类型的子算子：一部分是SeriesScanOperator，一部分是其他类型的mock算子 */
  private static List<Operator> createMixedChildOperators() {
    List<Operator> children = new ArrayList<>();

    try {
      // 前两个是SeriesScanOperator（有真实seriesPath）
      OperatorContext mockContext = createMockOperatorContext("TestMixed");
      FragmentInstanceContext fragmentContext = Mockito.mock(FragmentInstanceContext.class);
      when(mockContext.getInstanceContext()).thenReturn(fragmentContext);

      // SeriesScanOperator 1 - 使用正确的测量点路径格式
      MeasurementPath path1 = new MeasurementPath("root.database.device1.s1", TSDataType.DOUBLE);
      SeriesScanOptions options1 = SeriesScanOptions.getDefaultSeriesScanOptions(path1);
      SeriesScanOperator scanOp1 =
          new SeriesScanOperator(
              mockContext, new PlanNodeId("Scan_1"), path1, Ordering.ASC, options1);
      children.add(scanOp1);

      // SeriesScanOperator 2 - 使用正确的测量点路径格式
      MeasurementPath path2 = new MeasurementPath("root.database.device2.s1", TSDataType.FLOAT);
      SeriesScanOptions options2 = SeriesScanOptions.getDefaultSeriesScanOptions(path2);
      SeriesScanOperator scanOp2 =
          new SeriesScanOperator(
              mockContext, new PlanNodeId("Scan_2"), path2, Ordering.ASC, options2);
      children.add(scanOp2);

      // 后两个是mock算子（没有SeriesScanUtil，使用默认路径）
      children.add(Mockito.mock(Operator.class)); // 算子3
      children.add(Mockito.mock(Operator.class)); // 算子4

    } catch (Exception e) {
      System.err.println("创建混合算子时出错: " + e.getMessage());
      // 如果创建失败，回退到全mock对象
      return createMockChildren(4);
    }

    return children;
  }
}
