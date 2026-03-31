package org.apache.iotdb.db.queryengine.execution.colquery;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ColQueryRpcRetryUtilsTest {

  private boolean oldRetryEnabled;
  private int oldRetryIntervalMs;

  @Before
  public void setUp() throws Exception {
    ColQueryConfig config = ColQueryConfig.getInstance();
    oldRetryEnabled = config.isRpcRetryEnabled();
    oldRetryIntervalMs = config.getRpcRetryIntervalMs();
  }

  @After
  public void tearDown() throws Exception {
    setRetryConfig(oldRetryEnabled, oldRetryIntervalMs);
  }

  @Test
  public void testNoRetryWhenDisabled() throws Exception {
    setRetryConfig(false, 0);
    AtomicInteger attempts = new AtomicInteger(0);

    try {
      ColQueryRpcRetryUtils.execute(
          "testNoRetryWhenDisabled",
          () -> {
            attempts.incrementAndGet();
            throw new TException("first failure");
          });
      fail("Expected TException");
    } catch (TException e) {
      assertEquals(1, attempts.get());
    }
  }

  @Test
  public void testRetryOnceWhenEnabledAndSecondAttemptSucceeds() throws Exception {
    setRetryConfig(true, 0);
    AtomicInteger attempts = new AtomicInteger(0);

    ColQueryRpcRetryUtils.execute(
        "testRetryOnceWhenEnabledAndSecondAttemptSucceeds",
        () -> {
          int current = attempts.incrementAndGet();
          if (current == 1) {
            throw new TException("first failure");
          }
        });

    assertEquals(2, attempts.get());
  }

  @Test
  public void testStillFailAfterRetryWhenEnabled() throws Exception {
    setRetryConfig(true, 0);
    AtomicInteger attempts = new AtomicInteger(0);

    try {
      ColQueryRpcRetryUtils.execute(
          "testStillFailAfterRetryWhenEnabled",
          () -> {
            attempts.incrementAndGet();
            throw new TException("always fail");
          });
      fail("Expected TException");
    } catch (TException e) {
      assertEquals(2, attempts.get());
    }
  }

  private void setRetryConfig(boolean enabled, int intervalMs) throws Exception {
    ColQueryConfig config = ColQueryConfig.getInstance();

    Field enabledField = ColQueryConfig.class.getDeclaredField("rpcRetryEnabled");
    enabledField.setAccessible(true);
    enabledField.setBoolean(config, enabled);

    Field intervalField = ColQueryConfig.class.getDeclaredField("rpcRetryIntervalMs");
    intervalField.setAccessible(true);
    intervalField.setInt(config, intervalMs);
  }
}
