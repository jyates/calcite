/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.jdbc.cooperative;

import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.AbstractEnumerable2;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;

import com.google.common.collect.Iterators;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test that {@link AndCooperativeIterationPolicy} will combine policies together
 */
public class AndCooperativeIterationPolicyTest {

  @Test(expected = IllegalArgumentException.class)
  public void testNoPoliciesFails() throws Exception {
    buildPolicyFor("AND()");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMalformedPolicyNames() throws Exception {
    buildPolicyFor("AND(NOOP)");
  }

  @Test
  public void supportOnePolicyForAnd() throws Exception {
    verifyRowCountPolicy(buildPolicyFor("AND(RowCount)"));
  }

  @Test
  public void simpleDoublePolicy() throws Exception {
    verifyRowCountPolicy(buildPolicyFor("AND(RowCount,NoOp"));
  }

  private void verifyRowCountPolicy(AndCooperativeIterationPolicy policy) throws SQLException {
    AtomicBoolean closed = setStatementWithClose(policy);

    Enumerable<String> source = new AbstractEnumerable2<String>() {
      @Override
      public Iterator<String> iterator() {
        //infinite stream of elements
        return Iterators.cycle("elem");
      }
    };

    // check the row count policy
    Enumerable<String> e = policy.apply(source);
    Enumerator<String> er = e.enumerator();
    for (int i = 0; i < 10; i++) {
      assertTrue(er.moveNext());
    }
    // RowCount defaults to checking after every 10 rows
    // after which it will check the closed state, and then prevent any further iteration.
    closed.set(true);
    assertTrue(er.moveNext());
    assertFalse(er.moveNext());
  }


  private AtomicBoolean setStatementWithClose(CooperativeIterationPolicy policy)
      throws SQLException {
    AvaticaStatement stmt = Mockito.mock(AvaticaStatement.class);
    final AtomicBoolean stmtClosed = new AtomicBoolean(false);
    Mockito.when(stmt.isClosed()).then(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
        return stmtClosed.get();
      }
    });
    policy.setStatement(stmt);
    return stmtClosed;
  }

  @Test
  public void testRowAndTimer() throws Exception {
    verifyTimerAndRowCount("AND(Timer,RowCount)");
  }

  @Test
  public void testThreePolicies() throws Exception {
    verifyTimerAndRowCount("AND(Timer,RowCount,NoOp)");
  }

  private void verifyTimerAndRowCount(String policyString) throws Exception {
    AndCooperativeIterationPolicy policy = buildPolicyFor(policyString);
    final AvaticaStatement stmt = Mockito.mock(AvaticaStatement.class);
    final CountDownLatch closedCheck = new CountDownLatch(3);
    Mockito.when(stmt.isClosed()).then(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
        closedCheck.countDown();
        return false;
      }
    });
    policy.setStatement(stmt);

    // fancy enumerator where we can control progress with a simple lock mechanism
    final ReentrantLock lock = new ReentrantLock(true);
    final Enumerable<String> e = new AbstractEnumerable<String>() {
      @Override
      public Enumerator<String> enumerator() {
        return new Enumerator<String>() {
          @Override
          public String current() {
            return "next value";
          }

          @Override
          public boolean moveNext() {
            //wait for the lock
            try {
              lock.lock();
              return true;
            } finally {
              lock.unlock();
            }
          }

          @Override
          public void reset() {
            // noop
          }

          @Override
          public void close() {
            // noop
          }
        };
      }
    };

    // First check that we do the statement check
    final Enumerator<String> wrapped = policy.apply(e).enumerator();
    final CountDownLatch rowCountDone = new CountDownLatch(1);
    final CountDownLatch reset = new CountDownLatch(1);
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        // loop the first 10 times until we see the row count check
        for (int i = 0; i < 11; i++) {
          assertTrue(wrapped.moveNext());
        }
        rowCountDone.countDown();

        try {
          reset.await();
        } catch (InterruptedException e1) {
          throw new RuntimeException(e1);
        }
        // run until we are complete
        while (wrapped.moveNext()) {
          try {
            Thread.currentThread().sleep(5);
          } catch (InterruptedException e1) {
            throw new RuntimeException(e1);
          }
        }
      }
    });
    // prevent the underlying iterator from sending any messages
    lock.lock();
    // start the reader thread
    t.start();

    // wait for 3 checks of the statement. The first two are each policy entering the while loop,
    // third the timer checking the state after timing out
    closedCheck.await();


    // wait for the row count close check to trigger - 3x for the above, 10x for each call to the
    // timer's moveNext, 1x for the row count check
    Mockito.reset(stmt);
    final CountDownLatch rowCountClosedCheck = new CountDownLatch(3 + 10);
    Mockito.when(stmt.isClosed()).then(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
        rowCountClosedCheck.countDown();
        return false;
      }
    });

    // unlock the enumerator to start ending rows up
    lock.unlock();
    // wait for all the checks
    rowCountClosedCheck.await();
    rowCountDone.await();

    // just use the timeout to close the thread
    Mockito.reset(stmt);
    Mockito.when(stmt.isClosed()).then(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
        return true;
      }
    });
    reset.countDown();
    t.join();
  }

  private AndCooperativeIterationPolicy buildPolicyFor(String policy) {
    CalciteConnectionConfig config = Mockito.mock(CalciteConnectionConfig.class);
    Mockito.when(config.cooperativePolicy()).thenReturn(policy);
    return (AndCooperativeIterationPolicy) AndCooperativeIterationPolicy.BUILDER.apply(config);
  }
}
