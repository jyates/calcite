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
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A cooperative policy that supports multiple policies at the same time. The format looks like:
 * <pre>
 *   AND(POLICY1,POLICY2...)
 * </pre>
 * The order the policies are specified is the order they will be evaluated. For instance, if you
 * specified AND(Timed, RowCount) the timer would kick in every 100ms to check the closed state
 * first. Then every 10 rows, the row counter would check the closed state every 10 rows.
 * However, if you flipped the order, you still get equivalent logic; now the timer is the second
 * check, so it only triggers every move-next call orif the wait for the next row from the row
 * counter wrapper takes longer than 100ms.
 * <p>
 * Currently, each sub-policy would use the default values for cooperative action. It also
 * doesn't support sub-multi-part policies (e.g AND(POLICY1, AND(POLICY2,POLICY3) - in this case,
 * you should just use AND(POLICY1,POLICY2,POLICY3)).
 * </p>
 */
public class AndCooperativeIterationPolicy implements CooperativeIterationPolicy {
  public static final Function<CalciteConnectionConfig, CooperativeIterationPolicy> BUILDER =
    new Function<CalciteConnectionConfig, CooperativeIterationPolicy>() {

      @Nullable
      @Override
      public CooperativeIterationPolicy apply(CalciteConnectionConfig input) {
        String policy = input.cooperativePolicy();
        assert policy.startsWith("AND");
        // remove the wrappers
        policy = policy.replaceFirst("AND[(]", "");
        String[] parts = policy.split(",");
        Preconditions.checkArgument(parts.length > 0, "Must have at least one policy when using "
                                                      + "AND operation");
        parts[parts.length - 1] = parts[parts.length - 1].replace(")", "");

        // for each part, get the sub-policy to apply
        List<CooperativeIterationPolicy> policies = new ArrayList<>();
        for (String part : parts) {
          policies.add(
            CooperativePolicyFactory.CooperativePolicy.getPolicy(part.trim()).build(input));
        }


        return new AndCooperativeIterationPolicy(policies);
      }
    };
  private final List<CooperativeIterationPolicy> policies;

  public AndCooperativeIterationPolicy(List<CooperativeIterationPolicy> policies) {
    this.policies = policies;
  }

  @Override
  public <T> Enumerable<T> apply(Enumerable<T> source) {
    Enumerable ret = source;
    for (CooperativeIterationPolicy policy : policies) {
      ret = policy.apply(ret);
    }
    return ret;
  }

  @Override
  public void setStatement(AvaticaStatement statement) {
    for (CooperativeIterationPolicy policy : policies) {
      policy.setStatement(statement);
    }
  }
}
