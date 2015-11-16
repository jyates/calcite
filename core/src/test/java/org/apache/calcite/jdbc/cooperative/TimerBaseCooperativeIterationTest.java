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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test that the {@link TimerBasedCooperativeIteration} will check the 'stop' value periodically,
 * regardless of the state of the underlying enumerator
 */
public class TimerBaseCooperativeIterationTest {
  private static final Log LOG = LogFactory.getLog(TimerBaseCooperativeIterationTest.class);

  @Test
  public void testChecksClosedWithEmptyEnumerable() throws Exception {
    CalciteConnectionConfig config = Mockito.mock(CalciteConnectionConfig.class);
    TimerBasedCooperativeIteration iter =
      (TimerBasedCooperativeIteration) TimerBasedCooperativeIteration.BUILDER.apply(config);
    final AtomicBoolean stmtClosed = new AtomicBoolean(false);
    final AvaticaStatement stmt = Mockito.mock(AvaticaStatement.class);
    Mockito.when(stmt.isClosed()).then(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
        return stmtClosed.get();
      }
    });
    iter.setStatement(stmt);
    CountDownLatch next = new CountDownLatch(1);
    final AtomicBoolean finished = new AtomicBoolean(false);
    Enumerable e = iter.apply(new HangingNextEnumerable(next));
    final Enumerator er = e.enumerator();

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        // this should block until we close the iterator
        er.moveNext();
        finished.set(true);
      }
    });
    t.start();

    // wait for next to be called before closing the enumerator
    next.await();
    assertFalse(finished.get());
    stmtClosed.set(true);

    // move next should return
    t.join();
  }

  /**
   * If the delegate enumerator is empty the timer will eventually receive a 'poison pill' in its
   * delegate queue, allowing it to respond negatively to and moveNext calls.
   *
   * @throws Exception
   */
  @Test
  public void testPoisonPill() throws Exception {
    CalciteConnectionConfig config = Mockito.mock(CalciteConnectionConfig.class);
    TimerBasedCooperativeIteration iter =
      (TimerBasedCooperativeIteration) TimerBasedCooperativeIteration.BUILDER.apply(config);
    final AvaticaStatement stmt = Mockito.mock(AvaticaStatement.class);
    Enumerable e = new AbstractEnumerable2() {
      @Override
      public Iterator iterator() {
        return Lists.newArrayList("a", "b").iterator();
      }
    };
    iter.setStatement(stmt);
    Enumerator er = iter.apply(e).enumerator();
    // iterates 'a' and 'b'
    hasNext(er, "a");
    hasNext(er, "b");
    assertFalse(er.moveNext());
  }

  @Test
  public void testIgnoresSpuriousInterruption() throws Exception {
    CalciteConnectionConfig config = Mockito.mock(CalciteConnectionConfig.class);
    TimerBasedCooperativeIteration iter =
      (TimerBasedCooperativeIteration) TimerBasedCooperativeIteration.BUILDER.apply(config);
    final AtomicBoolean stmtClosed = new AtomicBoolean(false);
    final AvaticaStatement stmt = Mockito.mock(AvaticaStatement.class);
    Mockito.when(stmt.isClosed()).then(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
        return stmtClosed.get();
      }
    });
    iter.setStatement(stmt);
    CountDownLatch next = new CountDownLatch(1);
    final AtomicBoolean finished = new AtomicBoolean(false);
    Enumerable e = iter.apply(new HangingNextEnumerable(next));
    final Enumerator er = e.enumerator();

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        // this should block until we close the iterator
        er.moveNext();
        finished.set(true);
      }
    });
    t.start();

    // wait for next to be called before closing the enumerator
    next.await();
    assertFalse(finished.get());

    // interrupt the thread, which should just bump the loop to do another pull, but continue
    // waiting
    t.interrupt();
    Thread.sleep(500);
    assertFalse(finished.get());

    // do the actual close
    stmtClosed.set(true);
    // bump the running thread to immediately check the closed state
    t.interrupt();

    // move next should return
    t.join();
  }

  private void hasNext(Enumerator er, Object next) {
    assertTrue(er.moveNext());
    assertEquals(next, er.current());
  }

  /**
   * An enumerable whose enumerator hangs calls to moveNext indefinetly. The passed latch is
   * counted down when there is a call to moveNext
   *
   * @param <T>
   */
  private class HangingNextEnumerable<T> extends AbstractEnumerable<T> {

    private final CountDownLatch latch;

    public HangingNextEnumerable(CountDownLatch nextCalled) {
      this.latch = nextCalled;
    }

    @Override
    public Enumerator<T> enumerator() {
      return new Enumerator<T>() {
        @Override
        public T current() {
          return null;
        }

        @Override
        public boolean moveNext() {
          latch.countDown();
          try {
            this.wait();
          } catch (InterruptedException e) {
            LOG.info("Interrupted while waiting for 'next' that will never occur");
          }
          return false;
        }

        @Override
        public void reset() {

        }

        @Override
        public void close() {
        }
      };
    }
  }
}
