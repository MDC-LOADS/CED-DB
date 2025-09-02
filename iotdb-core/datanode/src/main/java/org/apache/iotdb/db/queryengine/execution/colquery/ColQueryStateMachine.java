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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.queryengine.execution.StateMachine;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState.ABORT;
import static org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState.CLOSED;
import static org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState.COL_QUERY;
import static org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState.PRE_CLOSED;
import static org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState.PRE_COL_QUERY;
import static org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState.START;

/**
 * State machine for collaborative query. It stores the states for the collaborative query process.
 * Others can register listeners when the state changes of the collaborative query.
 *
 * <p>The state transitions follow: CLOSED -> START -> PRE_COL_QUERY -> COL_QUERY -> PRE_CLOSED ->
 * CLOSED With ABORT state available from any state.
 */
public class ColQueryStateMachine {
  private final StateMachine<ColQueryState> colQueryState;

  private Executor stateMachineExecutor;
  private Throwable failureException;
  private TSStatus failureStatus;

  public ColQueryStateMachine(String colQueryId, ExecutorService executor) {
    this.stateMachineExecutor = executor;
    this.colQueryState =
        new StateMachine<>(
            colQueryId, this.stateMachineExecutor, CLOSED, ColQueryState.TERMINAL_INSTANCE_STATES);
  }

  public void addStateChangeListener(
      StateMachine.StateChangeListener<ColQueryState> stateChangeListener) {
    colQueryState.addStateChangeListener(stateChangeListener);
  }

  public ListenableFuture<ColQueryState> getStateChange(ColQueryState currentState) {
    return colQueryState.getStateChange(currentState);
  }

  public ColQueryState getState() {
    return colQueryState.get();
  }

  public void transitionToStart() {
    colQueryState.setIf(START, currentState -> currentState == CLOSED);
  }

  public void transitionToPreColQuery() {
    colQueryState.setIf(PRE_COL_QUERY, currentState -> currentState == START);
  }

  public void transitionToColQuery() {
    colQueryState.setIf(COL_QUERY, currentState -> currentState == PRE_COL_QUERY);
  }

  public void transitionToPreClosed() {
    colQueryState.setIf(PRE_CLOSED, currentState -> currentState == COL_QUERY);
  }

  public void transitionToClosed() {
    colQueryState.setIf(
        CLOSED, currentState -> currentState == PRE_CLOSED || currentState == START);
  }

  public void transitionToAbort() {
    transitionToDoneState(ABORT);
  }

  public void transitionToAbort(Throwable throwable) {
    this.failureException = throwable;
    transitionToDoneState(ABORT);
  }

  public void transitionToAbort(TSStatus failureStatus) {
    this.failureStatus = failureStatus;
    transitionToDoneState(ABORT);
  }

  public void transitionToAbort(Throwable throwable, TSStatus failureStatus) {
    this.failureException = throwable;
    this.failureStatus = failureStatus;
    transitionToDoneState(ABORT);
  }

  private void transitionToDoneState(ColQueryState doneState) {
    requireNonNull(doneState, "doneState is null");
    checkArgument(doneState.isDone(), "doneState %s is not a done state", doneState);

    colQueryState.setIf(doneState, currentState -> !currentState.isDone());
  }

  public String getFailureMessage() {
    if (failureException != null) {
      return failureException.getMessage();
    }
    return "no detailed failure reason in ColQueryStateMachine";
  }

  public Throwable getFailureException() {
    if (failureException == null && failureStatus != null) {
      return new IoTDBException(failureStatus.getMessage(), failureStatus.code);
    } else {
      return failureException;
    }
  }

  public TSStatus getFailureStatus() {
    return failureStatus;
  }
}
