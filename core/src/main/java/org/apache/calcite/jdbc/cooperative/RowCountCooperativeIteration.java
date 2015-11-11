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
import com.google.common.base.Preconditions;

import java.sql.SQLException;
import javax.annotation.Nullable;

/**
 * A cooperative policy that just checks the state every <tt>n</tt> rows, where <tt>n</tt> is
 * configurable
 */
public class RowCountCooperativeIteration implements CooperativeIterationPolicy {

  public static final Function<CalciteConnectionConfig, CooperativeIterationPolicy> BUILDER =
    new Function<CalciteConnectionConfig, CooperativeIterationPolicy>() {

      @Nullable
      @Override
      public CooperativeIterationPolicy apply(CalciteConnectionConfig input) {
        Preconditions.checkNotNull(input);
        //place holder until we can get from config
        return new RowCountCooperativeIteration(10);
      }
    };

  private final int count;
  private AvaticaStatement stmt;

  public RowCountCooperativeIteration(int count) {
    this.count = count;
  }

  @Override
  public void setStatement(AvaticaStatement stmt) {
    this.stmt = stmt;
  }

  @Override
  public <T> Enumerable<T> apply(final Enumerable<T> source) {
    return new AbstractEnumerable<T>() {
      @Override
      public Enumerator<T> enumerator() {
        return new DelegatingEnumerator<T>(source.enumerator()) {
          private int currentCount = 0;

          @Override
          public boolean moveNext() {
            if (currentCount++ > count) {
              currentCount = 0;
              try {
                if (stmt.isClosed()) {
                  return false;
                }
              } catch (SQLException e) {
                throw new RuntimeException("Unexpected exception when checking statement state!",
                  e);
              }
            }
            return delegate.moveNext();
          }
        };
      }
    };
  }
}
