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
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.DescTimeComparator;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LeftOuterTimeJoinOperatorTest {

  @Test
  public void testLeftOuterJoin1() {
    // left table
    // Time, s1
    // 4     4
    // 6     6
    // 9     9
    // ----------- TsBlock-1
    // 13    13
    // 17    17
    // ----------- TsBlock-2
    // 22    22
    // 25    25
    // ----------- TsBlock-3

    // right table
    // Time, s2
    // 1     10
    // 2     20
    // 3     30
    // ----------- TsBlock-1
    // 4     40
    // 5     50
    // 10    100
    // ----------- TsBlock-2
    // 13    130
    // 16    160
    // ----------- TsBlock-3
    // 26    260
    // 27    270
    // ----------- TsBlock-4

    // result table
    // Time, s1,    s2
    // 4      4     40
    // 6      6     null
    // 9      9     null
    // 13     13    130
    // 17     17    null
    // 22     22    null
    // 25     25    null

    OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
    Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1, TimeUnit.SECONDS));

    Operator leftChild =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {
                {4L, 6L, 9L},
                {13L, 17L},
                {22L, 25L}
              };

          private final int[][] valueArray =
              new int[][] {
                {4, 6, 9},
                {13, 17},
                {22, 25}
              };

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {
                  {false, false, false},
                  {false, false},
                  {false, false}
                }
              };

          private int index = 0;

          @Override
          public OperatorContext getOperatorContext() {
            return operatorContext;
          }

          @Override
          public TsBlock next() {
            TsBlockBuilder builder =
                new TsBlockBuilder(
                    timeArray[index].length, Collections.singletonList(TSDataType.INT32));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(0).appendNull();
              } else {
                builder.getColumnBuilder(0).writeInt(valueArray[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            index++;
            return builder.build();
          }

          @Override
          public boolean hasNext() {
            return index < 3;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
            return index >= 3;
          }

          @Override
          public long calculateMaxPeekMemory() {
            return 64 * 1024;
          }

          @Override
          public long calculateMaxReturnSize() {
            return 64 * 1024;
          }

          @Override
          public long calculateRetainedSizeAfterCallingNext() {
            return 0;
          }

          @Override
          public long ramBytesUsed() {
            return 0;
          }
        };

    Operator rightChild =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {
                {1L, 2L, 3L},
                {4L, 5L, 10L},
                {13L, 16L},
                {26L, 27L}
              };

          private final long[][] valueArray =
              new long[][] {
                {10L, 20L, 30L},
                {40L, 50L, 100L},
                {130L, 160L},
                {260L, 270L}
              };

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {
                  {false, false, false},
                  {false, false, false},
                  {false, false},
                  {false, false}
                }
              };

          private int index = 0;

          @Override
          public OperatorContext getOperatorContext() {
            return operatorContext;
          }

          @Override
          public TsBlock next() {
            TsBlockBuilder builder =
                new TsBlockBuilder(
                    timeArray[index].length, Collections.singletonList(TSDataType.INT64));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(0).appendNull();
              } else {
                builder.getColumnBuilder(0).writeLong(valueArray[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            index++;
            return builder.build();
          }

          @Override
          public boolean hasNext() {
            return index < 4;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
            return index >= 4;
          }

          @Override
          public long calculateMaxPeekMemory() {
            return 64 * 1024;
          }

          @Override
          public long calculateMaxReturnSize() {
            return 64 * 1024;
          }

          @Override
          public long calculateRetainedSizeAfterCallingNext() {
            return 0;
          }

          @Override
          public long ramBytesUsed() {
            return 0;
          }
        };

    LeftOuterTimeJoinOperator leftOuterTimeJoinOperator =
        new LeftOuterTimeJoinOperator(
            operatorContext,
            leftChild,
            1,
            rightChild,
            Arrays.asList(TSDataType.INT32, TSDataType.INT64),
            new AscTimeComparator());

    assertEquals(
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes() + 64 * 1024 * 2,
        leftOuterTimeJoinOperator.calculateMaxPeekMemory());
    assertEquals(
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes(),
        leftOuterTimeJoinOperator.calculateMaxReturnSize());
    assertEquals(64 * 1024 * 2, leftOuterTimeJoinOperator.calculateRetainedSizeAfterCallingNext());

    long[] timeArray = new long[] {4L, 6L, 9L, 13L, 17L, 22L, 25L};
    int[] column1Array = new int[] {4, 6, 9, 13, 17, 22, 25};
    boolean[] column1IsNull = new boolean[] {false, false, false, false, false, false, false};
    long[] column2Array = new long[] {40L, 0L, 0L, 130L, 0L, 0L, 0L};
    boolean[] column2IsNull = new boolean[] {false, true, true, false, true, true, true};

    try {
      int count = 0;
      ListenableFuture<?> listenableFuture = leftOuterTimeJoinOperator.isBlocked();
      listenableFuture.get();
      while (!leftOuterTimeJoinOperator.isFinished() && leftOuterTimeJoinOperator.hasNext()) {
        TsBlock tsBlock = leftOuterTimeJoinOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertEquals(timeArray[count], tsBlock.getTimeByIndex(i));
            assertEquals(column1IsNull[count], tsBlock.getColumn(0).isNull(i));
            if (!column1IsNull[count]) {
              assertEquals(column1Array[count], tsBlock.getColumn(0).getInt(i));
            }
            assertEquals(column2IsNull[count], tsBlock.getColumn(1).isNull(i));
            if (!column2IsNull[count]) {
              assertEquals(column2Array[count], tsBlock.getColumn(1).getLong(i));
            }
          }
        }
        listenableFuture = leftOuterTimeJoinOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(timeArray.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testLeftOuterJoin2() {
    // left table
    // Time, s1,     s2
    // 25    null    26
    // 22    22      null
    // ---------------------- TsBlock-1
    // null
    // ---------------------- TsBlock-2
    // 19    19      20
    // 18    18      null
    // 15    null    16
    // ---------------------- TsBlock-3
    // empty
    // ---------------------- TsBlock-4
    // 9     null    null
    // 7     7       null
    // 6     null    7
    // 3     3       4
    // ---------------------- TsBlock-5
    // empty
    // ---------------------- TsBlock-6

    // right table
    // Time, s3,      s4
    // 21    210.0    false
    // 20    200.0    null
    // ---------------------- TsBlock-1
    // empty
    // ---------------------- TsBlock-2
    // 19    190.0    true
    // 18    180.0    null
    // 15    null     false
    // 14    null     null
    // 8     80.0     true
    // 7     null     false
    // ---------------------- TsBlock-3
    // null
    // ---------------------- TsBlock-4
    // 5     50.0     true
    // ---------------------- TsBlock-5
    // 4     40.0     null
    // ---------------------- TsBlock-6
    // 3     30.0     false
    // ---------------------- TsBlock-7
    // 2     20.0     true
    // 1     10.0     false
    // ---------------------- TsBlock-8
    // empty
    // ---------------------- TsBlock-9

    // result table
    // Time, s1,     s2,     s3,     s4
    // 25    null    26      null    null
    // 22    22      null    null    null
    // 19    19      20      190.0   true
    // 18    18      null    180.0   null
    // 15    null    16      null    false
    // 9     null    null    null    null
    // 7     7       null    null    false
    // 6     null    7       null    null
    // 3     3       4       30.0    false

    OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
    Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1, TimeUnit.SECONDS));

    Operator leftChild =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {{25L, 22L}, null, {19L, 18L, 15L}, {}, {9L, 7L, 6L, 3L}, {}};

          private final int[][] value1Array =
              new int[][] {{0, 22}, null, {19, 18, 0}, {}, {0, 7, 0, 3}, {}};

          private final long[][] value2Array =
              new long[][] {{26L, 0L}, null, {20L, 0L, 16L}, {}, {0L, 0L, 7L, 4L}, {}};

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {{true, false}, null, {false, false, true}, {}, {true, false, true, false}, {}},
                {{false, true}, null, {false, true, false}, {}, {true, true, false, false}, {}}
              };

          private int index = 0;

          @Override
          public OperatorContext getOperatorContext() {
            return operatorContext;
          }

          @Override
          public TsBlock next() {
            if (timeArray[index] == null) {
              index++;
              return null;
            }
            TsBlockBuilder builder =
                new TsBlockBuilder(
                    timeArray[index].length, Arrays.asList(TSDataType.INT32, TSDataType.INT64));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(0).appendNull();
              } else {
                builder.getColumnBuilder(0).writeInt(value1Array[index][i]);
              }
              if (valueIsNull[1][index][i]) {
                builder.getColumnBuilder(1).appendNull();
              } else {
                builder.getColumnBuilder(1).writeLong(value2Array[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            index++;
            return builder.build();
          }

          @Override
          public boolean hasNext() {
            return index < 6;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
            return index >= 6;
          }

          @Override
          public long calculateMaxPeekMemory() {
            return 64 * 1024;
          }

          @Override
          public long calculateMaxReturnSize() {
            return 64 * 1024;
          }

          @Override
          public long calculateRetainedSizeAfterCallingNext() {
            return 0;
          }

          @Override
          public long ramBytesUsed() {
            return 0;
          }
        };

    Operator rightChild =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {
                {21L, 20L}, {}, {19L, 18L, 15L, 14L, 8L, 7L}, null, {5L}, {4L}, {3L}, {2L, 1L}, {}
              };

          private final float[][] value1Array =
              new float[][] {
                {210.0f, 200.0f},
                {},
                {190.0f, 180.0f, 0.0f, 0.0f, 80.0f, 0.0f},
                null,
                {50.0f},
                {40.0f},
                {30.0f},
                {20.0f, 10.0f},
                {}
              };

          private final boolean[][] value2Array =
              new boolean[][] {
                {false, false},
                {},
                {true, false, false, false, true, false},
                null,
                {true},
                {false},
                {false},
                {true, false},
                {}
              };

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {
                  {false, false},
                  {},
                  {false, false, true, true, false, true},
                  null,
                  {false},
                  {false},
                  {false},
                  {false, false},
                  {}
                },
                {
                  {false, true},
                  {},
                  {false, true, false, true, false, false},
                  null,
                  {false},
                  {true},
                  {false},
                  {false, false},
                  {}
                }
              };

          private int index = 0;

          @Override
          public OperatorContext getOperatorContext() {
            return operatorContext;
          }

          @Override
          public TsBlock next() {
            if (timeArray[index] == null) {
              index++;
              return null;
            }
            TsBlockBuilder builder =
                new TsBlockBuilder(
                    timeArray[index].length, Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(0).appendNull();
              } else {
                builder.getColumnBuilder(0).writeFloat(value1Array[index][i]);
              }
              if (valueIsNull[1][index][i]) {
                builder.getColumnBuilder(1).appendNull();
              } else {
                builder.getColumnBuilder(1).writeBoolean(value2Array[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            index++;
            return builder.build();
          }

          @Override
          public boolean hasNext() {
            return index < 9;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
            return index >= 9;
          }

          @Override
          public long calculateMaxPeekMemory() {
            return 64 * 1024;
          }

          @Override
          public long calculateMaxReturnSize() {
            return 64 * 1024;
          }

          @Override
          public long calculateRetainedSizeAfterCallingNext() {
            return 0;
          }

          @Override
          public long ramBytesUsed() {
            return 0;
          }
        };

    LeftOuterTimeJoinOperator leftOuterTimeJoinOperator =
        new LeftOuterTimeJoinOperator(
            operatorContext,
            leftChild,
            2,
            rightChild,
            Arrays.asList(TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.BOOLEAN),
            new DescTimeComparator());

    long[] timeArray = new long[] {25L, 22L, 19L, 18L, 15L, 9L, 7L, 6L, 3L};
    int[] column1Array = new int[] {0, 22, 19, 18, 0, 0, 7, 0, 3};
    boolean[] column1IsNull =
        new boolean[] {true, false, false, false, true, true, false, true, false};
    long[] column2Array = new long[] {26L, 0L, 20L, 0L, 16L, 0L, 0L, 7L, 4L};
    boolean[] column2IsNull =
        new boolean[] {false, true, false, true, false, true, true, false, false};
    float[] column3Array = new float[] {0.0f, 0.0f, 190.0f, 180.0f, 0.0f, 0.0f, 0.0f, 0.0f, 30.0f};
    boolean[] column3IsNull =
        new boolean[] {true, true, false, false, true, true, true, true, false};
    boolean[] column4Array =
        new boolean[] {false, false, true, false, false, false, false, false, false};
    boolean[] column4IsNull =
        new boolean[] {true, true, false, true, false, true, false, true, false};

    try {
      int count = 0;
      ListenableFuture<?> listenableFuture = leftOuterTimeJoinOperator.isBlocked();
      listenableFuture.get();
      while (!leftOuterTimeJoinOperator.isFinished() && leftOuterTimeJoinOperator.hasNext()) {
        TsBlock tsBlock = leftOuterTimeJoinOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertEquals(timeArray[count], tsBlock.getTimeByIndex(i));
            assertEquals(column1IsNull[count], tsBlock.getColumn(0).isNull(i));
            if (!column1IsNull[count]) {
              assertEquals(column1Array[count], tsBlock.getColumn(0).getInt(i));
            }
            assertEquals(column2IsNull[count], tsBlock.getColumn(1).isNull(i));
            if (!column2IsNull[count]) {
              assertEquals(column2Array[count], tsBlock.getColumn(1).getLong(i));
            }
            assertEquals(column3IsNull[count], tsBlock.getColumn(2).isNull(i));
            if (!column3IsNull[count]) {
              assertEquals(column3Array[count], tsBlock.getColumn(2).getFloat(i), 0.000001);
            }
            assertEquals(column4IsNull[count], tsBlock.getColumn(3).isNull(i));
            if (!column4IsNull[count]) {
              assertEquals(column4Array[count], tsBlock.getColumn(3).getBoolean(i));
            }
          }
        }
        listenableFuture = leftOuterTimeJoinOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(timeArray.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testLeftOuterJoin3() {
    // left table
    // Time, s1
    // 4     4
    // 6     6
    // 9     9
    // ----------- TsBlock-1
    // 13    13
    // 17    17
    // ----------- TsBlock-2
    // 22    22
    // 25    25
    // ----------- TsBlock-3
    // 100   100
    // 101   101
    // ----------- TsBlock-4
    // 110   110
    // 111   111
    // ----------- TsBlock-5

    // right table
    // Time, s2
    // 1     10
    // 2     20
    // 3     30
    // ----------- TsBlock-1
    // 4     40
    // 5     50
    // 10    100
    // ----------- TsBlock-2
    // 13    130
    // 16    160
    // ----------- TsBlock-3
    // 26    260
    // 27    270
    // ----------- TsBlock-4

    // result table
    // Time, s1,    s2
    // 4      4     40
    // 6      6     null
    // 9      9     null
    // 13     13    130
    // 17     17    null
    // 22     22    null
    // 25     25    null
    // 100   100    null
    // 101   101    null
    // 110   110    null
    // 111   111    null

    OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
    Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1, TimeUnit.SECONDS));

    Operator leftChild =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {
                {4L, 6L, 9L},
                {13L, 17L},
                {22L, 25L},
                {100L, 101L},
                {110L, 111L}
              };

          private final int[][] valueArray =
              new int[][] {
                {4, 6, 9},
                {13, 17},
                {22, 25},
                {100, 101},
                {110, 111}
              };

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {
                  {false, false, false},
                  {false, false},
                  {false, false},
                  {false, false},
                  {false, false}
                }
              };

          private int index = 0;

          @Override
          public OperatorContext getOperatorContext() {
            return operatorContext;
          }

          @Override
          public TsBlock next() {
            TsBlockBuilder builder =
                new TsBlockBuilder(
                    timeArray[index].length, Collections.singletonList(TSDataType.INT32));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(0).appendNull();
              } else {
                builder.getColumnBuilder(0).writeInt(valueArray[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            index++;
            return builder.build();
          }

          @Override
          public boolean hasNext() {
            return index < 5;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
            return index >= 5;
          }

          @Override
          public long calculateMaxPeekMemory() {
            return 64 * 1024;
          }

          @Override
          public long calculateMaxReturnSize() {
            return 64 * 1024;
          }

          @Override
          public long calculateRetainedSizeAfterCallingNext() {
            return 0;
          }

          @Override
          public long ramBytesUsed() {
            return 0;
          }
        };

    Operator rightChild =
        new Operator() {
          private final long[][] timeArray =
              new long[][] {
                {1L, 2L, 3L},
                {4L, 5L, 10L},
                {13L, 16L},
                {26L, 27L}
              };

          private final long[][] valueArray =
              new long[][] {
                {10L, 20L, 30L},
                {40L, 50L, 100L},
                {130L, 160L},
                {260L, 270L}
              };

          private final boolean[][][] valueIsNull =
              new boolean[][][] {
                {
                  {false, false, false},
                  {false, false, false},
                  {false, false},
                  {false, false}
                }
              };

          private int index = 0;

          @Override
          public OperatorContext getOperatorContext() {
            return operatorContext;
          }

          @Override
          public TsBlock next() {
            TsBlockBuilder builder =
                new TsBlockBuilder(
                    timeArray[index].length, Collections.singletonList(TSDataType.INT64));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
              if (valueIsNull[0][index][i]) {
                builder.getColumnBuilder(0).appendNull();
              } else {
                builder.getColumnBuilder(0).writeLong(valueArray[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            index++;
            return builder.build();
          }

          @Override
          public boolean hasNext() {
            return index < 4;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
            return index >= 4;
          }

          @Override
          public long calculateMaxPeekMemory() {
            return 64 * 1024;
          }

          @Override
          public long calculateMaxReturnSize() {
            return 64 * 1024;
          }

          @Override
          public long calculateRetainedSizeAfterCallingNext() {
            return 0;
          }

          @Override
          public long ramBytesUsed() {
            return 0;
          }
        };

    LeftOuterTimeJoinOperator leftOuterTimeJoinOperator =
        new LeftOuterTimeJoinOperator(
            operatorContext,
            leftChild,
            1,
            rightChild,
            Arrays.asList(TSDataType.INT32, TSDataType.INT64),
            new AscTimeComparator());

    assertEquals(
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes() + 64 * 1024 * 2,
        leftOuterTimeJoinOperator.calculateMaxPeekMemory());
    assertEquals(
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes(),
        leftOuterTimeJoinOperator.calculateMaxReturnSize());
    assertEquals(64 * 1024 * 2, leftOuterTimeJoinOperator.calculateRetainedSizeAfterCallingNext());

    long[] timeArray = new long[] {4L, 6L, 9L, 13L, 17L, 22L, 25L, 100L, 101L, 110L, 111L};
    int[] column1Array = new int[] {4, 6, 9, 13, 17, 22, 25, 100, 101, 110, 111};
    boolean[] column1IsNull =
        new boolean[] {false, false, false, false, false, false, false, false, false, false, false};
    long[] column2Array = new long[] {40L, 0L, 0L, 130L, 0L, 0L, 0L, 0L, 0L, 0L, 0L};
    boolean[] column2IsNull =
        new boolean[] {false, true, true, false, true, true, true, true, true, true, true};

    try {
      int count = 0;
      ListenableFuture<?> listenableFuture = leftOuterTimeJoinOperator.isBlocked();
      listenableFuture.get();
      while (!leftOuterTimeJoinOperator.isFinished() && leftOuterTimeJoinOperator.hasNext()) {
        TsBlock tsBlock = leftOuterTimeJoinOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++, count++) {
            assertEquals(timeArray[count], tsBlock.getTimeByIndex(i));
            assertEquals(column1IsNull[count], tsBlock.getColumn(0).isNull(i));
            if (!column1IsNull[count]) {
              assertEquals(column1Array[count], tsBlock.getColumn(0).getInt(i));
            }
            assertEquals(column2IsNull[count], tsBlock.getColumn(1).isNull(i));
            if (!column2IsNull[count]) {
              assertEquals(column2Array[count], tsBlock.getColumn(1).getLong(i));
            }
          }
        }
        listenableFuture = leftOuterTimeJoinOperator.isBlocked();
        listenableFuture.get();
      }
      assertEquals(timeArray.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

    /**
     * 测试 updateLeftOuterJoinCache() 方法的基本功能
     * 验证在左外连接操作过程中，QueryStateManager 的缓存状态是否被正确更新
     */
    @Test
    public void testUpdateLeftOuterJoinCache() {
        // 重置并初始化 QueryStateManager
        QueryStateManager.reset();
        QueryStateManager stateManager = QueryStateManager.initialize();

        OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
        Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1, TimeUnit.SECONDS));

        // 创建左子表操作器
        // 左表数据: TsBlock-1: (1,1), (2,2)  TsBlock-2: (10,10), (20,20)
        Operator leftChild =
                new Operator() {
                    private final long[][] timeArray = new long[][] {{1L, 2L}, {10L, 20L}};
                    private final int[][] valueArray = new int[][] {{1, 2}, {10, 20}};
                    private int index = 0;

                    @Override
                    public OperatorContext getOperatorContext() {
                        return operatorContext;
                    }

                    @Override
                    public TsBlock next() {
                        TsBlockBuilder builder =
                                new TsBlockBuilder(
                                        timeArray[index].length, Collections.singletonList(TSDataType.INT32));
                        for (int i = 0, size = timeArray[index].length; i < size; i++) {
                            builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
                            builder.getColumnBuilder(0).writeInt(valueArray[index][i]);
                        }
                        builder.declarePositions(timeArray[index].length);
                        index++;
                        return builder.build();
                    }

                    @Override
                    public boolean hasNext() {
                        return index < 2;
                    }

                    @Override
                    public void close() {}

                    @Override
                    public boolean isFinished() {
                        return index >= 2;
                    }

                    @Override
                    public long calculateMaxPeekMemory() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateMaxReturnSize() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateRetainedSizeAfterCallingNext() {
                        return 0;
                    }

                    @Override
                    public long ramBytesUsed() {
                        return 0;
                    }
                };

        // 创建右子表操作器
        // 右表数据: TsBlock-1: (1,10), (3,30)  TsBlock-2: (15,150)
        Operator rightChild =
                new Operator() {
                    private final long[][] timeArray = new long[][] {{1L, 3L}, {15L}};
                    private final long[][] valueArray = new long[][] {{10L, 30L}, {150L}};
                    private int index = 0;

                    @Override
                    public OperatorContext getOperatorContext() {
                        return operatorContext;
                    }

                    @Override
                    public TsBlock next() {
                        TsBlockBuilder builder =
                                new TsBlockBuilder(
                                        timeArray[index].length, Collections.singletonList(TSDataType.INT64));
                        for (int i = 0, size = timeArray[index].length; i < size; i++) {
                            builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
                            builder.getColumnBuilder(0).writeLong(valueArray[index][i]);
                        }
                        builder.declarePositions(timeArray[index].length);
                        index++;
                        return builder.build();
                    }

                    @Override
                    public boolean hasNext() {
                        return index < 2;
                    }

                    @Override
                    public void close() {}

                    @Override
                    public boolean isFinished() {
                        return index >= 2;
                    }

                    @Override
                    public long calculateMaxPeekMemory() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateMaxReturnSize() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateRetainedSizeAfterCallingNext() {
                        return 0;
                    }

                    @Override
                    public long ramBytesUsed() {
                        return 0;
                    }
                };

        // 创建左外连接操作器
        LeftOuterTimeJoinOperator leftOuterTimeJoinOperator =
                new LeftOuterTimeJoinOperator(
                        operatorContext,
                        leftChild,
                        1,
                        rightChild,
                        Arrays.asList(TSDataType.INT32, TSDataType.INT64),
                        new AscTimeComparator());

        try {
            // 验证初始状态：缓存标志为 false，缓存内容为 null
            assertFalse("初始状态 hasLeftOuterJoin 应为 false", stateManager.hasLeftOuterJoin());
            assertNull("初始状态 leftOuterJoinCache 应为 null", stateManager.getLeftOuterJoinCache());

            ListenableFuture<?> listenableFuture = leftOuterTimeJoinOperator.isBlocked();
            listenableFuture.get();

            // 处理第一批数据 - 应该产生结果并触发缓存更新
            TsBlock result1 = leftOuterTimeJoinOperator.next();
            assertNotNull("第一次处理结果不应为 null", result1);

            // 继续处理以消费数据并触发更多缓存场景
            while (leftOuterTimeJoinOperator.hasNext()) {
                listenableFuture = leftOuterTimeJoinOperator.isBlocked();
                listenableFuture.get();
                TsBlock result = leftOuterTimeJoinOperator.next();
                if (result != null && !result.isEmpty()) {
                    // 每次处理后都应该更新缓存
                }
            }

            // 在处理过程中的某个时点，缓存应该已被更新
            assertTrue("处理完成后 hasLeftOuterJoin 应为 true", stateManager.hasLeftOuterJoin());
            TsBlock cache = stateManager.getLeftOuterJoinCache();
            assertNotNull("处理完成后 leftOuterJoinCache 不应为 null", cache);

        } catch (Exception e) {
            e.printStackTrace();
            fail("测试失败，异常信息: " + e.getMessage());
        } finally {
            // 清理资源
            QueryStateManager.reset();
        }
    }

    /**
     * 测试当左表块被消费完毕，右表块仍有剩余数据时的缓存更新场景
     * 验证 updateLeftOuterJoinCache() 方法能正确缓存右表的剩余数据
     */
    @Test
    public void testUpdateLeftOuterJoinCacheWithLeftBlockFinished() {
        // 重置并初始化 QueryStateManager
        QueryStateManager.reset();
        QueryStateManager stateManager = QueryStateManager.initialize();

        OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
        Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1, TimeUnit.SECONDS));

        // 创建左子表操作器 - 数据量较少，会先被消费完
        // 左表数据: (1,1), (2,2)
        Operator leftChild =
                new Operator() {
                    private final long[][] timeArray = new long[][] {{1L, 2L}};
                    private final int[][] valueArray = new int[][] {{1, 2}};
                    private int index = 0;

                    @Override
                    public OperatorContext getOperatorContext() {
                        return operatorContext;
                    }

                    @Override
                    public TsBlock next() {
                        TsBlockBuilder builder =
                                new TsBlockBuilder(
                                        timeArray[index].length, Collections.singletonList(TSDataType.INT32));
                        for (int i = 0, size = timeArray[index].length; i < size; i++) {
                            builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
                            builder.getColumnBuilder(0).writeInt(valueArray[index][i]);
                        }
                        builder.declarePositions(timeArray[index].length);
                        index++;
                        return builder.build();
                    }

                    @Override
                    public boolean hasNext() {
                        return index < 1;
                    }

                    @Override
                    public void close() {}

                    @Override
                    public boolean isFinished() {
                        return index >= 1;
                    }

                    @Override
                    public long calculateMaxPeekMemory() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateMaxReturnSize() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateRetainedSizeAfterCallingNext() {
                        return 0;
                    }

                    @Override
                    public long ramBytesUsed() {
                        return 0;
                    }
                };

        // 创建右子表操作器 - 时间戳较大，数据量较多
        // 右表数据: (10,100), (20,200), (30,300)
        Operator rightChild =
                new Operator() {
                    private final long[][] timeArray = new long[][] {{10L, 20L, 30L}};
                    private final long[][] valueArray = new long[][] {{100L, 200L, 300L}};
                    private int index = 0;

                    @Override
                    public OperatorContext getOperatorContext() {
                        return operatorContext;
                    }

                    @Override
                    public TsBlock next() {
                        TsBlockBuilder builder =
                                new TsBlockBuilder(
                                        timeArray[index].length, Collections.singletonList(TSDataType.INT64));
                        for (int i = 0, size = timeArray[index].length; i < size; i++) {
                            builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
                            builder.getColumnBuilder(0).writeLong(valueArray[index][i]);
                        }
                        builder.declarePositions(timeArray[index].length);
                        index++;
                        return builder.build();
                    }

                    @Override
                    public boolean hasNext() {
                        return index < 1;
                    }

                    @Override
                    public void close() {}

                    @Override
                    public boolean isFinished() {
                        return index >= 1;
                    }

                    @Override
                    public long calculateMaxPeekMemory() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateMaxReturnSize() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateRetainedSizeAfterCallingNext() {
                        return 0;
                    }

                    @Override
                    public long ramBytesUsed() {
                        return 0;
                    }
                };

        // 创建左外连接操作器
        LeftOuterTimeJoinOperator leftOuterTimeJoinOperator =
                new LeftOuterTimeJoinOperator(
                        operatorContext,
                        leftChild,
                        1,
                        rightChild,
                        Arrays.asList(TSDataType.INT32, TSDataType.INT64),
                        new AscTimeComparator());

        try {
            // 验证初始状态
            assertFalse("初始状态 hasLeftOuterJoin 应为 false", stateManager.hasLeftOuterJoin());
            assertNull("初始状态 leftOuterJoinCache 应为 null", stateManager.getLeftOuterJoinCache());

            ListenableFuture<?> listenableFuture = leftOuterTimeJoinOperator.isBlocked();
            listenableFuture.get();

            // 第一次调用应该处理左表数据 (1, 2) 与右表数据 (10, 20, 30)
            // 由于左表时间戳远小于右表，左表数据将被处理完毕且右列填充 null
            TsBlock result = leftOuterTimeJoinOperator.next();
            assertNotNull("处理结果不应为 null", result);

            // 处理完成后，左表块已被消费但右表块仍有数据
            // 这应该触发对剩余右表块数据的缓存
            assertTrue("当左表块被消费且右表块有剩余数据时，hasLeftOuterJoin 应为 true",
                    stateManager.hasLeftOuterJoin());
            TsBlock cache = stateManager.getLeftOuterJoinCache();
            assertNotNull("当右表块有剩余数据时，leftOuterJoinCache 不应为 null", cache);
            assertTrue("缓存应该包含数据", cache.getPositionCount() > 0);

        } catch (Exception e) {
            e.printStackTrace();
            fail("测试失败，异常信息: " + e.getMessage());
        } finally {
            // 清理资源
            QueryStateManager.reset();
        }
    }

    /**
     * 测试当右表块被消费完毕或已完成，左表块仍有剩余数据时的缓存更新场景
     * 验证 updateLeftOuterJoinCache() 方法能正确缓存左表的剩余数据
     */
    @Test
    public void testUpdateLeftOuterJoinCacheWithRightBlockFinished() {
        // 重置并初始化 QueryStateManager
        QueryStateManager.reset();
        QueryStateManager stateManager = QueryStateManager.initialize();

        OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
        Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1, TimeUnit.SECONDS));

        // 创建左子表操作器 - 时间戳较大，数据量较多
        // 左表数据: (10,10), (20,20), (30,30)
        Operator leftChild =
                new Operator() {
                    private final long[][] timeArray = new long[][] {{10L, 20L, 30L}};
                    private final int[][] valueArray = new int[][] {{10, 20, 30}};
                    private int index = 0;

                    @Override
                    public OperatorContext getOperatorContext() {
                        return operatorContext;
                    }

                    @Override
                    public TsBlock next() {
                        TsBlockBuilder builder =
                                new TsBlockBuilder(
                                        timeArray[index].length, Collections.singletonList(TSDataType.INT32));
                        for (int i = 0, size = timeArray[index].length; i < size; i++) {
                            builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
                            builder.getColumnBuilder(0).writeInt(valueArray[index][i]);
                        }
                        builder.declarePositions(timeArray[index].length);
                        index++;
                        return builder.build();
                    }

                    @Override
                    public boolean hasNext() {
                        return index < 1;
                    }

                    @Override
                    public void close() {}

                    @Override
                    public boolean isFinished() {
                        return index >= 1;
                    }

                    @Override
                    public long calculateMaxPeekMemory() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateMaxReturnSize() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateRetainedSizeAfterCallingNext() {
                        return 0;
                    }

                    @Override
                    public long ramBytesUsed() {
                        return 0;
                    }
                };

        // 创建右子表操作器 - 时间戳较小，数据量较少，会先被消费完
        // 右表数据: (1,10), (2,20)
        Operator rightChild =
                new Operator() {
                    private final long[][] timeArray = new long[][] {{1L, 2L}};
                    private final long[][] valueArray = new long[][] {{10L, 20L}};
                    private int index = 0;

                    @Override
                    public OperatorContext getOperatorContext() {
                        return operatorContext;
                    }

                    @Override
                    public TsBlock next() {
                        TsBlockBuilder builder =
                                new TsBlockBuilder(
                                        timeArray[index].length, Collections.singletonList(TSDataType.INT64));
                        for (int i = 0, size = timeArray[index].length; i < size; i++) {
                            builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
                            builder.getColumnBuilder(0).writeLong(valueArray[index][i]);
                        }
                        builder.declarePositions(timeArray[index].length);
                        index++;
                        return builder.build();
                    }

                    @Override
                    public boolean hasNext() {
                        return index < 1;
                    }

                    @Override
                    public void close() {}

                    @Override
                    public boolean isFinished() {
                        return index >= 1;
                    }

                    @Override
                    public long calculateMaxPeekMemory() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateMaxReturnSize() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateRetainedSizeAfterCallingNext() {
                        return 0;
                    }

                    @Override
                    public long ramBytesUsed() {
                        return 0;
                    }
                };

        // 创建左外连接操作器
        LeftOuterTimeJoinOperator leftOuterTimeJoinOperator =
                new LeftOuterTimeJoinOperator(
                        operatorContext,
                        leftChild,
                        1,
                        rightChild,
                        Arrays.asList(TSDataType.INT32, TSDataType.INT64),
                        new AscTimeComparator());

        try {
            // 验证初始状态
            assertFalse("初始状态 hasLeftOuterJoin 应为 false", stateManager.hasLeftOuterJoin());
            assertNull("初始状态 leftOuterJoinCache 应为 null", stateManager.getLeftOuterJoinCache());

            ListenableFuture<?> listenableFuture = leftOuterTimeJoinOperator.isBlocked();
            listenableFuture.get();

            // 第一次调用应该处理右表数据 (1, 2) 与左表数据 (10, 20, 30)
            // 由于右表时间戳远小于左表，右表数据将先被消费完毕
            // 左表数据仍然存在，应该被缓存
            TsBlock result = leftOuterTimeJoinOperator.next();
            assertNotNull("处理结果不应为 null", result);

            // 处理完成后，右表块已被消费但左表块仍有数据
            // 这应该触发对剩余左表块数据的缓存
            assertTrue("当右表块被消费且左表块有剩余数据时，hasLeftOuterJoin 应为 true",
                    stateManager.hasLeftOuterJoin());
            TsBlock cache = stateManager.getLeftOuterJoinCache();
            assertNotNull("当左表块有剩余数据时，leftOuterJoinCache 不应为 null", cache);
            assertTrue("缓存应该包含数据", cache.getPositionCount() > 0);

        } catch (Exception e) {
            e.printStackTrace();
            fail("测试失败，异常信息: " + e.getMessage());
        } finally {
            // 清理资源
            QueryStateManager.reset();
        }
    }

    /**
     * 测试在 QueryStateManager 未初始化的情况下，updateLeftOuterJoinCache() 方法的防御性处理
     * 验证当没有全局状态管理器时，操作器仍能正常工作而不会抛出异常
     */
    @Test
    public void testUpdateLeftOuterJoinCacheWithoutQueryStateManager() {
        // 重置 QueryStateManager 但不重新初始化，模拟未初始化状态
        QueryStateManager.reset();

        OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
        Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1, TimeUnit.SECONDS));

        // 创建左子表操作器
        // 左表数据: (1,1), (2,2)
        Operator leftChild =
                new Operator() {
                    private final long[][] timeArray = new long[][] {{1L, 2L}};
                    private final int[][] valueArray = new int[][] {{1, 2}};
                    private int index = 0;

                    @Override
                    public OperatorContext getOperatorContext() {
                        return operatorContext;
                    }

                    @Override
                    public TsBlock next() {
                        TsBlockBuilder builder =
                                new TsBlockBuilder(
                                        timeArray[index].length, Collections.singletonList(TSDataType.INT32));
                        for (int i = 0, size = timeArray[index].length; i < size; i++) {
                            builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
                            builder.getColumnBuilder(0).writeInt(valueArray[index][i]);
                        }
                        builder.declarePositions(timeArray[index].length);
                        index++;
                        return builder.build();
                    }

                    @Override
                    public boolean hasNext() {
                        return index < 1;
                    }

                    @Override
                    public void close() {}

                    @Override
                    public boolean isFinished() {
                        return index >= 1;
                    }

                    @Override
                    public long calculateMaxPeekMemory() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateMaxReturnSize() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateRetainedSizeAfterCallingNext() {
                        return 0;
                    }

                    @Override
                    public long ramBytesUsed() {
                        return 0;
                    }
                };

        // 创建右子表操作器
        // 右表数据: (3,30), (4,40)
        Operator rightChild =
                new Operator() {
                    private final long[][] timeArray = new long[][] {{3L, 4L}};
                    private final long[][] valueArray = new long[][] {{30L, 40L}};
                    private int index = 0;

                    @Override
                    public OperatorContext getOperatorContext() {
                        return operatorContext;
                    }

                    @Override
                    public TsBlock next() {
                        TsBlockBuilder builder =
                                new TsBlockBuilder(
                                        timeArray[index].length, Collections.singletonList(TSDataType.INT64));
                        for (int i = 0, size = timeArray[index].length; i < size; i++) {
                            builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
                            builder.getColumnBuilder(0).writeLong(valueArray[index][i]);
                        }
                        builder.declarePositions(timeArray[index].length);
                        index++;
                        return builder.build();
                    }

                    @Override
                    public boolean hasNext() {
                        return index < 1;
                    }

                    @Override
                    public void close() {}

                    @Override
                    public boolean isFinished() {
                        return index >= 1;
                    }

                    @Override
                    public long calculateMaxPeekMemory() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateMaxReturnSize() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateRetainedSizeAfterCallingNext() {
                        return 0;
                    }

                    @Override
                    public long ramBytesUsed() {
                        return 0;
                    }
                };

        // 创建左外连接操作器
        LeftOuterTimeJoinOperator leftOuterTimeJoinOperator =
                new LeftOuterTimeJoinOperator(
                        operatorContext,
                        leftChild,
                        1,
                        rightChild,
                        Arrays.asList(TSDataType.INT32, TSDataType.INT64),
                        new AscTimeComparator());

        try {
            // 验证 QueryStateManager 确实未初始化
            assertFalse("QueryStateManager 应该处于未初始化状态", QueryStateManager.isInitialized());

            ListenableFuture<?> listenableFuture = leftOuterTimeJoinOperator.isBlocked();
            listenableFuture.get();

            // 即使 QueryStateManager 未初始化，操作器也应该能正常工作
            TsBlock result = leftOuterTimeJoinOperator.next();
            assertNotNull("即使没有 QueryStateManager，处理结果也不应为 null", result);

        } catch (Exception e) {
            e.printStackTrace();
            fail("当 QueryStateManager 未初始化时，测试不应该失败: " + e.getMessage());
        }
    }

    /**
     * 测试 extractDataFromThreshold() 方法的功能
     * 验证从 TsBlock 中提取时间戳 >= 阈值的数据是否正确
     *
     * 测试策略：创建一个场景，让左表有多个批次的数据，右表数据较少，
     * 这样在处理第一批次后，左表还有剩余数据需要缓存
     */
    @Test
    public void testExtractDataFromThreshold() {
        QueryStateManager.reset();
        QueryStateManager stateManager = QueryStateManager.initialize();

        OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
        Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1, TimeUnit.SECONDS));

        // 创建左子表操作器 - 有两个批次的数据
        Operator leftChild =
                new Operator() {
                    private final long[][] timeArray = new long[][] {{10L, 20L}, {30L, 40L, 50L}};
                    private final int[][] valueArray = new int[][] {{10, 20}, {30, 40, 50}};
                    private int index = 0;

                    @Override
                    public OperatorContext getOperatorContext() {
                        return operatorContext;
                    }

                    @Override
                    public TsBlock next() {
                        TsBlockBuilder builder =
                                new TsBlockBuilder(
                                        timeArray[index].length, Collections.singletonList(TSDataType.INT32));
                        for (int i = 0, size = timeArray[index].length; i < size; i++) {
                            builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
                            builder.getColumnBuilder(0).writeInt(valueArray[index][i]);
                        }
                        builder.declarePositions(timeArray[index].length);
                        index++;
                        return builder.build();
                    }

                    @Override
                    public boolean hasNext() {
                        return index < 2;
                    }

                    @Override
                    public void close() {}

                    @Override
                    public boolean isFinished() {
                        return index >= 2;
                    }

                    @Override
                    public long calculateMaxPeekMemory() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateMaxReturnSize() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateRetainedSizeAfterCallingNext() {
                        return 0;
                    }

                    @Override
                    public long ramBytesUsed() {
                        return 0;
                    }
                };

        // 创建右子表操作器 - 只有一个小数据，很快就会被处理完
        Operator rightChild =
                new Operator() {
                    private final long[][] timeArray = new long[][] {{15L}};
                    private final long[][] valueArray = new long[][] {{150L}};
                    private int index = 0;

                    @Override
                    public OperatorContext getOperatorContext() {
                        return operatorContext;
                    }

                    @Override
                    public TsBlock next() {
                        TsBlockBuilder builder =
                                new TsBlockBuilder(
                                        timeArray[index].length, Collections.singletonList(TSDataType.INT64));
                        for (int i = 0, size = timeArray[index].length; i < size; i++) {
                            builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
                            builder.getColumnBuilder(0).writeLong(valueArray[index][i]);
                        }
                        builder.declarePositions(timeArray[index].length);
                        index++;
                        return builder.build();
                    }

                    @Override
                    public boolean hasNext() {
                        return index < 1;
                    }

                    @Override
                    public void close() {}

                    @Override
                    public boolean isFinished() {
                        return index >= 1;
                    }

                    @Override
                    public long calculateMaxPeekMemory() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateMaxReturnSize() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateRetainedSizeAfterCallingNext() {
                        return 0;
                    }

                    @Override
                    public long ramBytesUsed() {
                        return 0;
                    }
                };

        LeftOuterTimeJoinOperator leftOuterTimeJoinOperator =
                new LeftOuterTimeJoinOperator(
                        operatorContext,
                        leftChild,
                        1,
                        rightChild,
                        Arrays.asList(TSDataType.INT32, TSDataType.INT64),
                        new AscTimeComparator());

        try {
            ListenableFuture<?> listenableFuture = leftOuterTimeJoinOperator.isBlocked();
            listenableFuture.get();

            // 处理第一批数据
            TsBlock result1 = leftOuterTimeJoinOperator.next();
            assertNotNull("第一次处理结果不应为 null", result1);

            // 继续处理以消费更多数据，触发缓存更新
            while (leftOuterTimeJoinOperator.hasNext()) {
                listenableFuture = leftOuterTimeJoinOperator.isBlocked();
                listenableFuture.get();
                TsBlock result = leftOuterTimeJoinOperator.next();
                // 每次处理都可能触发缓存更新
            }

            // 在某个处理过程中，应该会触发缓存（当右表完成但左表还有数据时）
            assertTrue("处理完成后 hasLeftOuterJoin 应为 true", stateManager.hasLeftOuterJoin());
            TsBlock cache = stateManager.getLeftOuterJoinCache();
            assertNotNull("处理完成后 leftOuterJoinCache 不应为 null", cache);

            // 验证缓存中的数据结构正确性
            assertTrue("缓存应该包含数据", cache.getPositionCount() > 0);

            // 检查缓存中的时间戳是否按预期排序（应该是剩余数据）
            for (int i = 0; i < cache.getPositionCount(); i++) {
                long timestamp = cache.getTimeByIndex(i);
                // 缓存的数据应该是未处理的部分
                assertTrue("缓存中的时间戳应该合理", timestamp >= 10L);

                // 验证时间戳的单调性（升序）
                if (i > 0) {
                    long previousTimestamp = cache.getTimeByIndex(i - 1);
                    assertTrue("缓存中的时间戳应该是单调递增的", timestamp > previousTimestamp);
                }
            }

            // 验证缓存中的值列数量正确
            assertEquals("缓存中的列数应该正确", 1, cache.getValueColumnCount());

        } catch (Exception e) {
            e.printStackTrace();
            fail("测试失败，异常信息: " + e.getMessage());
        } finally {
            // 清理资源
            QueryStateManager.reset();
        }
    }

    /**
     * 测试 extractDataFromThreshold() 方法处理边界情况
     * 包括空 TsBlock、无效索引、无数据满足阈值条件等场景
     */
    @Test
    public void testExtractDataFromThresholdBoundaryConditions() {
        QueryStateManager.reset();
        QueryStateManager stateManager = QueryStateManager.initialize();

        OperatorContext operatorContext = Mockito.mock(OperatorContext.class);
        Mockito.when(operatorContext.getMaxRunTime()).thenReturn(new Duration(1, TimeUnit.SECONDS));

        // 测试场景1：左表有数据但阈值很大，应该没有数据满足条件
        Operator leftChild1 =
                new Operator() {
                    private final long[][] timeArray = new long[][] {{1L, 2L, 3L}};
                    private final int[][] valueArray = new int[][] {{1, 2, 3}};
                    private int index = 0;

                    @Override
                    public OperatorContext getOperatorContext() {
                        return operatorContext;
                    }

                    @Override
                    public TsBlock next() {
                        TsBlockBuilder builder =
                                new TsBlockBuilder(
                                        timeArray[index].length, Collections.singletonList(TSDataType.INT32));
                        for (int i = 0, size = timeArray[index].length; i < size; i++) {
                            builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
                            builder.getColumnBuilder(0).writeInt(valueArray[index][i]);
                        }
                        builder.declarePositions(timeArray[index].length);
                        index++;
                        return builder.build();
                    }

                    @Override
                    public boolean hasNext() {
                        return index < 1;
                    }

                    @Override
                    public void close() {}

                    @Override
                    public boolean isFinished() {
                        return index >= 1;
                    }

                    @Override
                    public long calculateMaxPeekMemory() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateMaxReturnSize() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateRetainedSizeAfterCallingNext() {
                        return 0;
                    }

                    @Override
                    public long ramBytesUsed() {
                        return 0;
                    }
                };

        // 右表数据的时间戳都比左表大很多，这样左表会被完全处理但右表数据会被缓存
        Operator rightChild1 =
                new Operator() {
                    private final long[][] timeArray = new long[][] {{100L, 200L}};
                    private final long[][] valueArray = new long[][] {{100L, 200L}};
                    private int index = 0;

                    @Override
                    public OperatorContext getOperatorContext() {
                        return operatorContext;
                    }

                    @Override
                    public TsBlock next() {
                        TsBlockBuilder builder =
                                new TsBlockBuilder(
                                        timeArray[index].length, Collections.singletonList(TSDataType.INT64));
                        for (int i = 0, size = timeArray[index].length; i < size; i++) {
                            builder.getTimeColumnBuilder().writeLong(timeArray[index][i]);
                            builder.getColumnBuilder(0).writeLong(valueArray[index][i]);
                        }
                        builder.declarePositions(timeArray[index].length);
                        index++;
                        return builder.build();
                    }

                    @Override
                    public boolean hasNext() {
                        return index < 1;
                    }

                    @Override
                    public void close() {}

                    @Override
                    public boolean isFinished() {
                        return index >= 1;
                    }

                    @Override
                    public long calculateMaxPeekMemory() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateMaxReturnSize() {
                        return 64 * 1024;
                    }

                    @Override
                    public long calculateRetainedSizeAfterCallingNext() {
                        return 0;
                    }

                    @Override
                    public long ramBytesUsed() {
                        return 0;
                    }
                };

        LeftOuterTimeJoinOperator leftOuterTimeJoinOperator1 =
                new LeftOuterTimeJoinOperator(
                        operatorContext,
                        leftChild1,
                        1,
                        rightChild1,
                        Arrays.asList(TSDataType.INT32, TSDataType.INT64),
                        new AscTimeComparator());

        try {
            // 验证初始状态
            assertFalse("初始状态 hasLeftOuterJoin 应为 false", stateManager.hasLeftOuterJoin());
            assertNull("初始状态 leftOuterJoinCache 应为 null", stateManager.getLeftOuterJoinCache());

            ListenableFuture<?> listenableFuture = leftOuterTimeJoinOperator1.isBlocked();
            listenableFuture.get();

            // 处理数据 - 左表数据 (1,2,3) 会被处理，右表数据 (100,200) 应该被缓存
            TsBlock result = leftOuterTimeJoinOperator1.next();
            assertNotNull("处理结果不应为 null", result);

            // 验证缓存状态
            assertTrue("处理后 hasLeftOuterJoin 应为 true", stateManager.hasLeftOuterJoin());
            TsBlock cache = stateManager.getLeftOuterJoinCache();
            assertNotNull("处理后 leftOuterJoinCache 不应为 null", cache);

            // 验证缓存的具体内容 - 应该包含右表的所有数据
            assertEquals("缓存应该包含2条记录", 2, cache.getPositionCount());
            assertEquals("第一条记录的时间戳应为100", 100L, cache.getTimeByIndex(0));
            assertEquals("第二条记录的时间戳应为200", 200L, cache.getTimeByIndex(1));

            // 验证缓存中的值列数据类型正确
            assertEquals("缓存中应该有1个值列", 1, cache.getValueColumnCount());
            assertEquals("第一条记录的值应为100", 100L, cache.getColumn(0).getLong(0));
            assertEquals("第二条记录的值应为200", 200L, cache.getColumn(0).getLong(1));

        } catch (Exception e) {
            e.printStackTrace();
            fail("边界条件测试失败，异常信息: " + e.getMessage());
        } finally {
            // 清理资源
            QueryStateManager.reset();
        }
    }

}
