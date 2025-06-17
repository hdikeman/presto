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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.Expressions.isNull;
import static java.util.Objects.requireNonNull;

/**
 * Pull constants through aggregation nodes and remove the respective
 * aggregation. If no aggregations are remaining in the node, the child
 * and parent project nodes are merged entirely.
 * <p>
 * From:
 * </pre>
 * - Project
 *   output := result
 *     - Aggregation
 *       [...]
 *       result := SUM(input)
 *       [...]
 *         - Project
 *           [...]
 *           input := NULL
 *           [...]
 * </pre>
 * To:
 * <pre>
 * - Project
 *   output := NULL
 *     - Aggregation
 *       [...]
 *         - Project
 *           [...]
 * </pre>
 */
public class PushConstantThroughAggregation
        implements Rule<ProjectNode>
{
    private static final Capture<AggregationNode> SOURCE = Capture.newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(aggregation().capturedAs(SOURCE)));

    private final StandardFunctionResolution functionResolution;

    public PushConstantThroughAggregation(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public boolean isEnabled(Session session)
    {
        // TODO add session feature flag
        return true;
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        Assignments outputs = node.getAssignments();
        Assignments.Builder outputsV2 = Assignments.builder().putAll(outputs);
        boolean changed = false;

        AggregationNode child = captures.get(SOURCE);
        if (!(node.getSource() instanceof ProjectNode)) {
            // only handle the simple Project -> Aggregation -> Project case now
            return Result.empty();
        }
        ProjectNode grandchild = (ProjectNode) child.getSource();
        Assignments inputs = grandchild.getAssignments();

        Assignments.Builder inputsV2 = Assignments.builder().putAll(inputs);
        Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregationsV2 = new LinkedHashMap<>(
                child.getAggregations());
        for (Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : child.getAggregations().entrySet()) {
            VariableReferenceExpression variable = entry.getKey();
            AggregationNode.Aggregation aggregation = entry.getValue();

            if (!nullInput(aggregation, inputs) || !replaceable(aggregation)) {
                continue;
            }
            changed = true;

            // 1. find all references to aggregation output in `node`
            // 2. replace with actual NULL constants (type based on aggregation output type)
            // outputsV2.put(
            // aggregationsV2.remove(variable);
        }

        // get child project of aggregation
        // loop over aggregation
        //      check inputs of aggregation
        //
        //      if variable -> continue
        //
        //      if constant ->
        //          1. tear out aggregation dependent on it
        //          2. replace assignment in base project with result

        if (!changed) {
            return Result.empty();
        }

        ProjectNode grandchildV2 = new ProjectNode(
                grandchild.getSourceLocation(),
                grandchild.getId(),
                grandchild.getSource(),
                inputsV2.build(),
                grandchild.getLocality());

        AggregationNode childV2 = new AggregationNode(
                child.getSourceLocation(),
                child.getId(),
                grandchildV2,
                aggregationsV2,
                child.getGroupingSets(),
                ImmutableList.of(),
                child.getStep(),
                child.getHashVariable(),
                child.getGroupIdVariable(),
                child.getAggregationId());

        ProjectNode nodeV2 = new ProjectNode(
                node.getSourceLocation(),
                node.getId(),
                childV2,
                outputsV2.build(),
                node.getLocality());

        return Result.ofPlanNode(nodeV2);
    }

    private boolean replaceable(AggregationNode.Aggregation aggregation)
    {
        FunctionHandle handle = aggregation.getFunctionHandle();
        return functionResolution.isMaxFunction(handle) ||
                functionResolution.isMinFunction(handle) ||
                functionResolution.isAverageFunction(handle) || // TODO add
                functionResolution.isSumFunction(handle) || // TODO add
                functionResolution.isCountFunction(handle) ||
                functionResolution.isMaxFunction(handle) ||
                functionResolution.isApproximateCountDistinctFunction(handle);
    }

    private boolean nullInput(AggregationNode.Aggregation aggregation, Assignments inputs)
    {
        RowExpression argument = aggregation.getArguments().get(0);
        RowExpression input = null;

        if (argument instanceof VariableReferenceExpression) {
            input = inputs.get((VariableReferenceExpression) argument);
        }

        return isNull(input);
    }
}
