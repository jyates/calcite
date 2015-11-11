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
import org.apache.calcite.linq4j.Enumerable;

import com.google.common.base.Function;

import javax.annotation.Nullable;

/**
 * A {@link CooperativeIterationPolicy} that doesn't do any special cooperation - the source
 * iterator is returned without modification
 */
public class NoOpCooperativeIteration implements CooperativeIterationPolicy {

  public static final NoOpCooperativeIteration IMPL = new NoOpCooperativeIteration();
  public static final Function<CalciteConnectionConfig, CooperativeIterationPolicy> BUILDER =
    new Function<CalciteConnectionConfig, CooperativeIterationPolicy>() {
      @Nullable
      @Override
      public CooperativeIterationPolicy apply(CalciteConnectionConfig input) {
        return IMPL;
      }
    };

  @Override
  public <T> Enumerable<T> apply(Enumerable<T> source) {
    return source;
  }

  @Override
  public void setStatement(AvaticaStatement statement) {
    // noop
  }
}
