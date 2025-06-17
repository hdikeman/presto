/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushConstantThroughAggregation
        extends BaseRuleTest
{
    @Test
    public void testPushNullThroughSum()
    {
        tester().assertThat(new PushConstantThroughAggregation(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression input = p.variable("input", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    return p.project(
                            p.assignment(p.variable("output", BIGINT), p.variable("sum_result", BIGINT)),
                            p.aggregation((builder) -> builder
                                    .addAggregation(
                                            p.variable("sum_result", BIGINT),
                                            p.rowExpression("sum(input)"))
                                    .globalGrouping()
                                    .step(SINGLE)
                                    .source(
                                            p.project(
                                                    p.assignment(input, p.rowExpression("CAST(NULL AS BIGINT)")),
                                                    p.values(a)))));
                })
                .matches(
                        project(
                                ImmutableMap.of("output", expression("CAST(NULL AS BIGINT)")),
                                aggregation(
                                        ImmutableMap.of(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of("input", expression("CAST(NULL AS BIGINT)")),
                                                values("a")))));
    }

    @Test
    public void testPushNullThroughMin()
    {
        tester().assertThat(new PushConstantThroughAggregation(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression input = p.variable("input", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    return p.project(
                            p.assignment(p.variable("output", BIGINT), p.variable("min_result", BIGINT)),
                            p.aggregation((builder) -> builder
                                    .addAggregation(
                                            p.variable("min_result", BIGINT),
                                            p.rowExpression("min(input)"))
                                    .globalGrouping()
                                    .step(SINGLE)
                                    .source(
                                            p.project(
                                                    p.assignment(input, p.rowExpression("CAST(NULL AS BIGINT)")),
                                                    p.values(a)))));
                })
                .matches(
                        project(
                                ImmutableMap.of("output", expression("CAST(NULL AS BIGINT)")),
                                aggregation(
                                        ImmutableMap.of(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of("input", expression("CAST(NULL AS BIGINT)")),
                                                values("a")))));
    }

    @Test
    public void testPushNullThroughMax()
    {
        tester().assertThat(new PushConstantThroughAggregation(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression input = p.variable("input", DOUBLE);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    return p.project(
                            p.assignment(p.variable("output", DOUBLE), p.variable("max_result", DOUBLE)),
                            p.aggregation((builder) -> builder
                                    .addAggregation(
                                            p.variable("max_result", DOUBLE),
                                            p.rowExpression("max(input)"))
                                    .globalGrouping()
                                    .step(SINGLE)
                                    .source(
                                            p.project(
                                                    p.assignment(input, p.rowExpression("CAST(NULL AS DOUBLE)")),
                                                    p.values(a)))));
                })
                .matches(
                        project(
                                ImmutableMap.of("output", expression("CAST(NULL AS DOUBLE)")),
                                aggregation(
                                        ImmutableMap.of(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of("input", expression("CAST(NULL AS DOUBLE)")),
                                                values("a")))));
    }

    @Test
    public void testPushNullThroughCount()
    {
        tester().assertThat(new PushConstantThroughAggregation(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression input = p.variable("input", VARCHAR);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    return p.project(
                            p.assignment(p.variable("output", BIGINT), p.variable("count_result", BIGINT)),
                            p.aggregation((builder) -> builder
                                    .addAggregation(
                                            p.variable("count_result", BIGINT),
                                            p.rowExpression("count(input)"))
                                    .globalGrouping()
                                    .step(SINGLE)
                                    .source(
                                            p.project(
                                                    p.assignment(input, p.rowExpression("CAST(NULL AS VARCHAR)")),
                                                    p.values(a)))));
                })
                .matches(
                        project(
                                ImmutableMap.of("output", expression("CAST(NULL AS BIGINT)")),
                                aggregation(
                                        ImmutableMap.of(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of("input", expression("CAST(NULL AS VARCHAR)")),
                                                values("a")))));
    }

    @Test
    public void testPushNullThroughAverage()
    {
        tester().assertThat(new PushConstantThroughAggregation(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression input = p.variable("input", DOUBLE);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    return p.project(
                            p.assignment(p.variable("output", DOUBLE), p.variable("avg_result", DOUBLE)),
                            p.aggregation((builder) -> builder
                                    .addAggregation(
                                            p.variable("avg_result", DOUBLE),
                                            p.rowExpression("avg(input)"))
                                    .globalGrouping()
                                    .step(SINGLE)
                                    .source(
                                            p.project(
                                                    p.assignment(input, p.rowExpression("CAST(NULL AS DOUBLE)")),
                                                    p.values(a)))));
                })
                .matches(
                        project(
                                ImmutableMap.of("output", expression("CAST(NULL AS DOUBLE)")),
                                aggregation(
                                        ImmutableMap.of(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of("input", expression("CAST(NULL AS DOUBLE)")),
                                                values("a")))));
    }

    @Test
    public void testPushMultipleNullAggregations()
    {
        tester().assertThat(new PushConstantThroughAggregation(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression input1 = p.variable("input1", BIGINT);
                    VariableReferenceExpression input2 = p.variable("input2", DOUBLE);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    return p.project(
                            p.assignment(
                                    p.variable("output1", BIGINT), p.variable("sum_result", BIGINT),
                                    p.variable("output2", DOUBLE), p.variable("avg_result", DOUBLE)),
                            p.aggregation((builder) -> builder
                                    .addAggregation(
                                            p.variable("sum_result", BIGINT),
                                            p.rowExpression("sum(input1)"))
                                    .addAggregation(
                                            p.variable("avg_result", DOUBLE),
                                            p.rowExpression("avg(input2)"))
                                    .globalGrouping()
                                    .step(SINGLE)
                                    .source(
                                            p.project(
                                                    p.assignment(
                                                            input1, p.rowExpression("CAST(NULL AS BIGINT)"),
                                                            input2, p.rowExpression("CAST(NULL AS DOUBLE)")),
                                                    p.values(a)))));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "output1", expression("CAST(NULL AS BIGINT)"),
                                        "output2", expression("CAST(NULL AS DOUBLE)")),
                                aggregation(
                                        ImmutableMap.of(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of(
                                                        "input1", expression("CAST(NULL AS BIGINT)"),
                                                        "input2", expression("CAST(NULL AS DOUBLE)")),
                                                values("a")))));
    }

    @Test
    public void testDontPushNonNullConstant()
    {
        tester().assertThat(new PushConstantThroughAggregation(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression input = p.variable("input", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    return p.project(
                            p.assignment(p.variable("output", BIGINT), p.variable("sum_result", BIGINT)),
                            p.aggregation((builder) -> builder
                                    .addAggregation(
                                            p.variable("sum_result", BIGINT),
                                            p.rowExpression("sum(input)"))
                                    .globalGrouping()
                                    .step(SINGLE)
                                    .source(
                                            p.project(
                                                    p.assignment(input, p.rowExpression("42")),
                                                    p.values(a)))));
                }).doesNotFire();
    }

    @Test
    public void testDontPushVariable()
    {
        tester().assertThat(new PushConstantThroughAggregation(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression input = p.variable("input", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    return p.project(
                            p.assignment(p.variable("output", BIGINT), p.variable("sum_result", BIGINT)),
                            p.aggregation((builder) -> builder
                                    .addAggregation(
                                            p.variable("sum_result", BIGINT),
                                            p.rowExpression("sum(input)"))
                                    .globalGrouping()
                                    .step(SINGLE)
                                    .source(
                                            p.project(
                                                    p.assignment(input, p.variable("a", BIGINT)),
                                                    p.values(a)))));
                }).doesNotFire();
    }

    @Test
    public void testDontPushUnsupportedAggregation()
    {
        tester().assertThat(new PushConstantThroughAggregation(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression input = p.variable("input", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    return p.project(
                            p.assignment(p.variable("output", BIGINT), p.variable("result", BIGINT)),
                            p.aggregation((builder) -> builder
                                    .addAggregation(
                                            p.variable("result", BIGINT),
                                            p.rowExpression("arbitrary(input)"))
                                    .globalGrouping()
                                    .step(SINGLE)
                                    .source(
                                            p.project(
                                                    p.assignment(input, p.rowExpression("CAST(NULL AS BIGINT)")),
                                                    p.values(a)))));
                }).doesNotFire();
    }
}
