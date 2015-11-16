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

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.base.Function;

/**
 * Factory to generate a {@link CooperativeIterationPolicy} based on connection properties
 */
public class CooperativePolicyFactory {

  private static final CooperativePolicy DEFAULT = CooperativePolicy.RowCount;

  /**
   * Allowed policies
   */
  enum CooperativePolicy {
    NoOp(NoOpCooperativeIteration.BUILDER),
    RowCount(RowCountCooperativeIteration.BUILDER),
    Timer(TimerBasedCooperativeIteration.BUILDER),
    AND(AndCooperativeIterationPolicy.BUILDER);

    private final Function<CalciteConnectionConfig, CooperativeIterationPolicy> builder;

    CooperativePolicy(Function<CalciteConnectionConfig, CooperativeIterationPolicy> builder) {
      this.builder = builder;
    }

    public static CooperativePolicy getPolicy(String type) {
      if (type.startsWith("AND")) {
        return AND;
      }
      return CooperativePolicy.valueOf(type);
    }

    public CooperativeIterationPolicy build(CalciteConnectionConfig conn) {
      return builder.apply(conn);
    }
  }

  public CooperativeIterationPolicy createPolicy(CalciteConnection connection) {
    String policyName = connection.config().cooperativePolicy();
    CooperativePolicy builder = CooperativePolicy.getPolicy(policyName);
    if (builder == null) {
      builder = DEFAULT;
    }
//    Preconditions.checkNotNull("No policy found for name: " + policyName);
    return builder.build(connection.config());
  }

  // Used by the apply policy method when you wrap the base expression
  public static <T> Enumerable<T> applyPolicy(DataContext context, Enumerable<T> delegate) {
    CooperativeIterationPolicy policy =
      (CooperativeIterationPolicy) context.get("COOPERATIVE_EXEC_POLICY");
    // this happens when we have a simple context, like when doing an explain plan
    if (policy == null) {
      policy = NoOpCooperativeIteration.IMPL;
    }
    return policy.apply(delegate);
  }

  public static Expression wrapBaseExpression(Expression baseEnumerable) {
    return Expressions.call(BuiltInMethod.COOPERATIVE_POLICY_APPLY.method, DataContext.ROOT,
      baseEnumerable);
  }
}
