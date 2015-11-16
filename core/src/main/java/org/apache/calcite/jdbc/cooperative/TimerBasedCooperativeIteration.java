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
import org.apache.calcite.linq4j.DelegatingEnumerator;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;

import com.google.common.base.Function;

import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 *
 */
public class TimerBasedCooperativeIteration extends BaseCooperativeIterationPolicy {

  /**
   * Marker values used to populate the queues indicating state-change requests
   */
  private static final Object POISON_PILL = "END_OF_ENUMERABLE_VALUES";
  private static final Object NEXT = "MOVE_NEXT_REQUESTED";
  private static ExecutorService pool;
  public static final Function<CalciteConnectionConfig, CooperativeIterationPolicy> BUILDER =
    new Function<CalciteConnectionConfig, CooperativeIterationPolicy>() {

      @Nullable
      @Override
      public CooperativeIterationPolicy apply(CalciteConnectionConfig input) {
        synchronized (TimerBasedCooperativeIteration.class) {
          if (pool == null) {
            pool = Executors.newFixedThreadPool(10,
              new DaemonThreadFactory("Time-Out-Cooperative Enumerator"));
          }
          return new TimerBasedCooperativeIteration(100);
        }
      }
    };
  private final long timeout;

  private TimerBasedCooperativeIteration(long millis) {
    this.timeout = millis;
  }

  @Override
  public <T> Enumerable<T> apply(final Enumerable<T> source) {
    return new AbstractEnumerable<T>() {
      @Override
      public Enumerator<T> enumerator() {
        TimerBasedCheckingEnumerator<T> enumerator =
          new TimerBasedCheckingEnumerator<>(source.enumerator(), stmt, timeout);
        pool.submit(enumerator);
        return enumerator;
      }
    };
  }

  /**
   * Enumerator wrapper that communicates over 'channels' (unbounded blocking queues). The source
   * enumerator is  queried while the parent {@link AvaticaStatement} is not closed and there is
   * a move next request (object in the moveNextRequest queue). For each move next request there
   * is an element added to the output queue; the element is either the next value from the
   * delegate enumerator OR a 'poison pill'. The output queue is queried in calls to {@link
   * #moveNext()} for the next element, blocking until one is ready. If the 'poison pill' is
   * received, then this enumerator is marked 'done' and {@link #moveNext()} returns false.
   * Otherwise, the next element is assumed to be the next element from the source enumerator and
   * it is cast to the expected type and returned.
   * <p>
   * Because enumerators are blocking, we have to wrap the actual 'next' action in another thread.
   * Communication via channels allows for easy understanding and separation of concerns for the
   * running and the polling threads, with minimal shared state.
   * </p>
   *
   * @param <T> generic type of the values to receive from this enumerator
   */
  private class TimerBasedCheckingEnumerator<T> extends DelegatingEnumerator<T>
    implements Runnable {
    private final AvaticaStatement stmt;
    private final long timeout;
    private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Object> moveNextRequests = new LinkedBlockingQueue<>();
    private T current;
    private boolean done = false;

    public TimerBasedCheckingEnumerator(Enumerator<T> source, AvaticaStatement stmt, long timeout) {
      super(source);
      this.stmt = stmt;
      this.timeout = timeout;
    }

    @Override
    public T current() {
      return this.current;
    }

    @Override
    public boolean moveNext() {
      try {
        while (!stmt.isClosed() && !done) {
          try {
            moveNextRequests.put(NEXT);
            Object next = this.queue.poll(timeout, TimeUnit.MILLISECONDS);
            if (next == null) {
              continue;
            } else if (next == POISON_PILL) {
              this.current = null;
              this.done = true;
              return false;
            } else {
              current = (T) next;
              return true;
            }
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      } catch (SQLException e) {
        throw new RuntimeException("Exception while checking whether the parent statement is "
                                   + "closed or not", e);
      }
      return false;
    }

    @Override
    public void run() {
      try {
        while (!stmt.isClosed()) {
          try {
            // block until the next moveNext request
            this.moveNextRequests.take();
            // move to the next element in the queue. If there is one, we just return the 'new'
            // current element.If there isn't one, we put a poison pill on the queue and quit
            if (this.delegate.moveNext()) {
              this.queue.put(delegate.current());
            } else {
              this.queue.put(POISON_PILL);
              return;
            }
          } catch (InterruptedException e) {
            // we only quit if the statement is closed, so we go around the loop again
          }
        }

        // just in case we find the close before reaching the end, we put the poison pill on to
        // ensure no one tries to read more
        this.queue.put(POISON_PILL);
      } catch (SQLException e) {
        throw new RuntimeException("Exception while checking whether the parent statement is "
                                   + "closed or not", e);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while putting poison pill on queue after parent "
                                   + "statement is closed", e);
      }
    }
  }
}
