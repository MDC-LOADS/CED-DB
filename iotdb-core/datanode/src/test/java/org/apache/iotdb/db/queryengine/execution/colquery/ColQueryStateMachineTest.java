package org.apache.iotdb.db.queryengine.execution.colquery;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.queryengine.execution.StateMachine;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState.ABORT;
import static org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState.CLOSED;
import static org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState.COL_QUERY;
import static org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState.PRE_CLOSED;
import static org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState.PRE_COL_QUERY;
import static org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState.START;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ColQueryStateMachineTest {

  private ExecutorService executor;
  private ColQueryStateMachine stateMachine;

  @Before
  public void setUp() {
    executor = Executors.newCachedThreadPool();
    stateMachine = new ColQueryStateMachine("test-colquery-001", executor);
  }

  @After
  public void tearDown() {
    executor.shutdown();
  }

  /** 测试状态机的初始状态 验证状态机初始化后处于CLOSED状态 */
  @Test
  public void testInitialState() {
    assertEquals(CLOSED, stateMachine.getState());
  }

  /**
   * 测试正常的状态转换流程 验证完整的状态转换序列：CLOSED -> START -> PRE_COL_QUERY -> COL_QUERY -> PRE_CLOSED -> CLOSED
   */
  @Test
  public void testNormalStateTransition() {
    // CLOSED -> START
    stateMachine.transitionToStart();
    assertEquals(START, stateMachine.getState());

    // START -> PRE_COL_QUERY
    stateMachine.transitionToPreColQuery();
    assertEquals(PRE_COL_QUERY, stateMachine.getState());

    // PRE_COL_QUERY -> COL_QUERY
    stateMachine.transitionToColQuery();
    assertEquals(COL_QUERY, stateMachine.getState());

    // COL_QUERY -> PRE_CLOSED
    stateMachine.transitionToPreClosed();
    assertEquals(PRE_CLOSED, stateMachine.getState());

    // PRE_CLOSED -> CLOSED
    stateMachine.transitionToClosed();
    assertEquals(CLOSED, stateMachine.getState());
  }

  /** 测试无效的状态转换 验证状态机会拒绝不符合转换规则的状态变更 */
  @Test
  public void testInvalidTransitions() {
    // 尝试从CLOSED直接跳到PRE_COL_QUERY应该失败
    stateMachine.transitionToPreColQuery();
    assertEquals(CLOSED, stateMachine.getState());

    // 尝试从START直接跳到COL_QUERY应该失败
    stateMachine.transitionToStart();
    stateMachine.transitionToColQuery();
    assertEquals(START, stateMachine.getState());
  }

  /** 测试从任意状态转换到ABORT状态 验证状态机可以从任何状态安全地转换到中止状态 */
  @Test
  public void testAbortFromAnyState() {
    // 从CLOSED状态中止
    stateMachine.transitionToAbort();
    assertEquals(ABORT, stateMachine.getState());

    // 创建新的状态机测试从其他状态中止
    stateMachine = new ColQueryStateMachine("test-colquery-002", executor);

    // 到达COL_QUERY状态然后中止
    stateMachine.transitionToStart();
    stateMachine.transitionToPreColQuery();
    stateMachine.transitionToColQuery();
    stateMachine.transitionToAbort();
    assertEquals(ABORT, stateMachine.getState());
  }

  /** 测试带异常信息的ABORT转换 验证状态机能正确存储和返回异常信息 */
  @Test
  public void testAbortWithException() {
    Exception testException = new RuntimeException("Test exception");
    stateMachine.transitionToAbort(testException);

    assertEquals(ABORT, stateMachine.getState());
    assertEquals(testException, stateMachine.getFailureException());
    assertEquals("Test exception", stateMachine.getFailureMessage());
  }

  /** 测试带TSStatus的ABORT转换 验证状态机能正确存储和返回TSStatus信息 */
  @Test
  public void testAbortWithTSStatus() {
    TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    status.setMessage("Test failure");

    stateMachine.transitionToAbort(status);

    assertEquals(ABORT, stateMachine.getState());
    assertEquals(status, stateMachine.getFailureStatus());
  }

  /** 测试同时带异常和TSStatus的ABORT转换 验证状态机能同时处理异常信息和状态码 */
  @Test
  public void testAbortWithExceptionAndStatus() {
    Exception testException = new RuntimeException("Test exception");
    TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    status.setMessage("Test failure");

    stateMachine.transitionToAbort(testException, status);

    assertEquals(ABORT, stateMachine.getState());
    assertEquals(testException, stateMachine.getFailureException());
    assertEquals(status, stateMachine.getFailureStatus());
    assertEquals("Test exception", stateMachine.getFailureMessage());
  }

  /** 测试状态变更监听器 验证监听器能够正确接收状态变更通知 注意：添加监听器时会立即收到当前状态通知，然后才是状态变更通知 */
  @Test
  public void testStateChangeListener() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2); // 等待两次调用：初始状态 + 状态变更
    AtomicReference<ColQueryState> firstState = new AtomicReference<>();
    AtomicReference<ColQueryState> secondState = new AtomicReference<>();
    final AtomicInteger callCount = new AtomicInteger(0);

    stateMachine.addStateChangeListener(
        new StateMachine.StateChangeListener<ColQueryState>() {
          @Override
          public void stateChanged(ColQueryState newState) {
            int count = callCount.getAndIncrement();
            if (count == 0) {
              firstState.set(newState); // 第一次调用：当前状态 CLOSED
            } else if (count == 1) {
              secondState.set(newState); // 第二次调用：新状态 START
            }
            latch.countDown();
          }
        });

    stateMachine.transitionToStart();

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(CLOSED, firstState.get()); // 添加监听器时的当前状态
    assertEquals(START, secondState.get()); // 状态转换后的新状态
  }

  /** 测试终态判断 验证各个状态的isDone()方法返回正确的值 */
  @Test
  public void testTerminalStates() {
    assertFalse(CLOSED.isDone());
    assertTrue(ABORT.isDone());
    assertFalse(START.isDone());
    assertFalse(PRE_COL_QUERY.isDone());
    assertFalse(COL_QUERY.isDone());
    assertFalse(PRE_CLOSED.isDone());
  }

  /** 测试从终态无法转换到其他状态 验证一旦进入ABORT状态，就无法再进行任何状态转换 */
  @Test
  public void testCannotTransitionFromTerminalState() {
    // 转换到ABORT状态
    stateMachine.transitionToAbort();
    assertEquals(ABORT, stateMachine.getState());

    // 尝试从ABORT状态转换到其他状态应该失败
    stateMachine.transitionToStart();
    assertEquals(ABORT, stateMachine.getState());

    stateMachine.transitionToClosed();
    assertEquals(ABORT, stateMachine.getState());
  }

  /** 测试从 START 状态直接转换到 CLOSED 状态 验证状态机支持快捷路径，不必完成所有中间步骤 */
  @Test
  public void testDirectTransitionFromStartToClosed() {
    stateMachine.transitionToStart();
    assertEquals(START, stateMachine.getState());

    // START可以直接转换到CLOSED
    stateMachine.transitionToClosed();
    assertEquals(CLOSED, stateMachine.getState());
  }

  /** 测试 CLOSED 状态可以重新开始查询周期 验证状态机支持在完成一个周期后重新启动新的查询 */
  @Test
  public void testClosedStateCanRestartCycle() {
    // 执行完整周期
    stateMachine.transitionToStart();
    stateMachine.transitionToPreColQuery();
    stateMachine.transitionToColQuery();
    stateMachine.transitionToPreClosed();
    stateMachine.transitionToClosed();
    assertEquals(CLOSED, stateMachine.getState());

    // CLOSED状态应该能够重新开始新周期
    stateMachine.transitionToStart();
    assertEquals(START, stateMachine.getState());

    // 再次完成周期
    stateMachine.transitionToPreColQuery();
    stateMachine.transitionToColQuery();
    stateMachine.transitionToPreClosed();
    stateMachine.transitionToClosed();
    assertEquals(CLOSED, stateMachine.getState());
  }

  /** 测试完整的工作流程和状态监听 验证完整的状态转换序列和监听器功能 注意：监听器会收到6次调用（1次初始状态 + 5次状态转换） 由于异步执行，我们记录所有状态变化而不依赖于顺序 */
  @Test
  public void testCompleteWorkflow() throws InterruptedException {
    CountDownLatch completionLatch = new CountDownLatch(6); // 1次初始状态 + 5次状态转换
    List<ColQueryState> receivedStates = Collections.synchronizedList(new ArrayList<>());

    stateMachine.addStateChangeListener(
        new StateMachine.StateChangeListener<ColQueryState>() {
          @Override
          public void stateChanged(ColQueryState newState) {
            receivedStates.add(newState);
            completionLatch.countDown();
          }
        });

    // 执行完整的工作流程
    stateMachine.transitionToStart();
    stateMachine.transitionToPreColQuery();
    stateMachine.transitionToColQuery();
    stateMachine.transitionToPreClosed();
    stateMachine.transitionToClosed();

    assertTrue(completionLatch.await(5, TimeUnit.SECONDS));

    // 验证我们接收到了所有预期的状态（不依赖于顺序）
    assertEquals(6, receivedStates.size());
    assertTrue("应该包含初始状态 CLOSED", receivedStates.contains(CLOSED));
    assertTrue("应该包含 START", receivedStates.contains(START));
    assertTrue("应该包含 PRE_COL_QUERY", receivedStates.contains(PRE_COL_QUERY));
    assertTrue("应该包含 COL_QUERY", receivedStates.contains(COL_QUERY));
    assertTrue("应该包含 PRE_CLOSED", receivedStates.contains(PRE_CLOSED));
    // CLOSED 会出现两次：初始状态一次，最终状态一次
    long closedCount = receivedStates.stream().filter(state -> state == CLOSED).count();
    assertEquals("CLOSED 状态应该出现2次", 2, closedCount);

    // 验证最终状态
    assertEquals(CLOSED, stateMachine.getState());
  }

  /** 测试无异常情况下的失败消息 验证在没有具体异常信息时返回默认消息 */
  @Test
  public void testFailureMessageWithoutException() {
    stateMachine.transitionToAbort();
    assertEquals(
        "no detailed failure reason in ColQueryStateMachine", stateMachine.getFailureMessage());
  }

  /** 测试通过TSStatus获取失败异常 验证当只有TSStatus时，能够正确转换为异常对象 */
  @Test
  public void testGetFailureExceptionWithTSStatus() {
    TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    status.setMessage("Test failure message");

    stateMachine.transitionToAbort(status);

    Throwable exception = stateMachine.getFailureException();
    assertNotNull(exception);
    assertTrue(exception.getMessage().contains("Test failure message"));
  }

  /** 测试状态转换序列的正确性 验证按照预期序列执行的状态转换都能成功 */
  @Test
  public void testStateTransitionSequence() {
    ColQueryState[] expectedSequence = {START, PRE_COL_QUERY, COL_QUERY, PRE_CLOSED, CLOSED};

    ColQueryState currentState = stateMachine.getState();
    assertEquals(CLOSED, currentState);

    for (ColQueryState expectedState : expectedSequence) {
      switch (expectedState) {
        case START:
          stateMachine.transitionToStart();
          break;
        case PRE_COL_QUERY:
          stateMachine.transitionToPreColQuery();
          break;
        case COL_QUERY:
          stateMachine.transitionToColQuery();
          break;
        case PRE_CLOSED:
          stateMachine.transitionToPreClosed();
          break;
        case CLOSED:
          stateMachine.transitionToClosed();
          break;
      }
      assertEquals(expectedState, stateMachine.getState());
    }
  }
}
