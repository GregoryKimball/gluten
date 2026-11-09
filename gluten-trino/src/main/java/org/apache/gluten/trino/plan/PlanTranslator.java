/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.trino.plan;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import io.substrait.proto.AdvancedExtension;
import io.substrait.proto.AggregateFunction;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.AggregationPhase;
import io.substrait.proto.Expression;
import io.substrait.proto.FetchMode;
import io.substrait.proto.FetchRel;
import io.substrait.proto.FilterRel;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.JoinRel;
import io.substrait.proto.NestedLoopJoinRel;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.RelRoot;
import io.substrait.proto.SortField;
import io.substrait.proto.SortRel;
import io.substrait.proto.TopNRel;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.spi.type.Type;
import io.trino.sql.DynamicFilters;
import io.trino.sql.ir.IrUtils;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Translates a Trino plan fragment into a Gluten Substrait plan. Table scans read Hive Parquet
 * files through splits supplied at run time; remote sources become Gluten iterator inputs fed
 * with pages from Trino's exchange. The fragment's output stays in Trino, which partitions and
 * serializes pages for downstream tasks.
 */
public final class PlanTranslator {
  private static final Set<String> SPLITTABLE_AGGREGATES = Set.of("sum", "count", "min", "max", "avg");

  private final FunctionCollector functions = new FunctionCollector();
  private final List<RemoteInput> inputs = new ArrayList<>();
  private Optional<TableScanNode> scan = Optional.empty();

  /** A remote source feeding the native task as Gluten input iterator {@code index}. */
  public record RemoteInput(int index, RemoteSourceNode node, List<Type> types) {}

  /** The translated fragment. */
  public record Translation(
      byte[] plan, Optional<TableScanNode> scan, List<RemoteInput> inputs, List<Type> outputTypes) {}

  private record Translated(Rel rel, List<Symbol> layout) {}

  private PlanTranslator() {}

  public static Translation translate(PlanFragment fragment) {
    PlanTranslator translator = new PlanTranslator();
    List<Symbol> outputLayout = fragment.getOutputPartitioningScheme().getOutputLayout();
    Translated root = translator.project(translator.visit(fragment.getRoot()), outputLayout);

    List<String> names = new ArrayList<>();
    for (int i = 0; i < outputLayout.size(); i++) {
      names.add("c" + i);
    }
    Plan.Builder plan = Plan.newBuilder();
    translator.functions.addTo(plan);
    plan.addRelations(
        PlanRel.newBuilder().setRoot(RelRoot.newBuilder().setInput(root.rel()).addAllNames(names)));
    return new Translation(
        plan.build().toByteArray(),
        translator.scan,
        ImmutableList.copyOf(translator.inputs),
        outputLayout.stream().map(Symbol::type).collect(ImmutableList.toImmutableList()));
  }

  private Translated visit(PlanNode node) {
    return switch (node) {
      case TableScanNode tableScan -> visitTableScan(tableScan, Optional.empty());
      case FilterNode filter -> visitFilter(filter);
      case ProjectNode project -> visitProject(project);
      case JoinNode join -> visitJoin(join);
      case SemiJoinNode semiJoin -> visitSemiJoin(semiJoin);
      case AggregationNode aggregation -> visitAggregation(aggregation);
      case ExchangeNode exchange -> visitLocalExchange(exchange);
      case RemoteSourceNode remoteSource -> visitRemoteSource(remoteSource);
      case OutputNode output -> visit(output.getSource());
      case SortNode sortNode -> sort(visit(sortNode.getSource()), sortNode.getOrderingScheme());
      case TopNNode topN -> visitTopN(topN);
      case LimitNode limit -> visitLimit(limit);
      case ValuesNode values -> visitValues(values);
      default -> throw new UnsupportedPlanException("plan node " + node.getClass().getSimpleName());
    };
  }

  private Translated visitTableScan(TableScanNode node, Optional<io.trino.sql.ir.Expression> filter) {
    if (scan.isPresent()) {
      throw new UnsupportedPlanException("more than one table scan in a fragment");
    }
    scan = Optional.of(node);
    HiveSplits.checkTable(node.getTable().connectorHandle());
    List<Symbol> outputs = node.getOutputSymbols();
    List<String> names = new ArrayList<>();
    List<Type> types = new ArrayList<>();
    for (Symbol symbol : outputs) {
      names.add(HiveSplits.columnName(node.getAssignments().get(symbol)));
      types.add(symbol.type());
    }
    ReadRel.Builder read = ReadRel.newBuilder().setBaseSchema(TypeTranslator.namedStruct(names, types));
    filter.ifPresent(
        predicate -> read.setFilter(new ExpressionTranslator(functions, outputs).translate(predicate)));
    return new Translated(Rel.newBuilder().setRead(read).build(), outputs);
  }

  private Translated visitFilter(FilterNode node) {
    // Dynamic filters are dropped: Velox pushes join build-side values into the probe scan of the
    // same task itself.
    List<io.trino.sql.ir.Expression> conjuncts =
        DynamicFilters.extractDynamicFilters(node.getPredicate()).staticConjuncts();
    Optional<io.trino.sql.ir.Expression> predicate =
        conjuncts.isEmpty() ? Optional.empty() : Optional.of(IrUtils.combineConjuncts(conjuncts));
    if (node.getSource() instanceof TableScanNode tableScan) {
      return visitTableScan(tableScan, predicate);
    }
    Translated source = visit(node.getSource());
    if (predicate.isEmpty()) {
      return source;
    }
    Expression condition = new ExpressionTranslator(functions, source.layout()).translate(predicate.get());
    Rel rel = Rel.newBuilder()
        .setFilter(FilterRel.newBuilder().setInput(source.rel()).setCondition(condition))
        .build();
    return new Translated(rel, source.layout());
  }

  private Translated visitProject(ProjectNode node) {
    Translated source = visit(node.getSource());
    ExpressionTranslator translator = new ExpressionTranslator(functions, source.layout());
    ProjectRel.Builder project = ProjectRel.newBuilder().setInput(source.rel());
    RelCommon.Emit.Builder emit = RelCommon.Emit.newBuilder();
    int next = source.layout().size();
    for (Symbol output : node.getOutputSymbols()) {
      io.trino.sql.ir.Expression expression = node.getAssignments().get(output);
      if (expression instanceof io.trino.sql.ir.Reference reference) {
        emit.addOutputMapping(translator.indexOf(Symbol.from(reference)));
      } else {
        project.addExpressions(translator.translate(expression));
        emit.addOutputMapping(next++);
      }
    }
    project.setCommon(RelCommon.newBuilder().setEmit(emit));
    return new Translated(Rel.newBuilder().setProject(project).build(), node.getOutputSymbols());
  }

  private Translated visitJoin(JoinNode node) {
    Translated left = visit(node.getLeft());
    Translated right = visit(node.getRight());
    List<Symbol> combined = ImmutableList.<Symbol>builder().addAll(left.layout()).addAll(right.layout()).build();
    ExpressionTranslator translator = new ExpressionTranslator(functions, combined);

    if (node.getCriteria().isEmpty()) {
      NestedLoopJoinRel.Builder join = NestedLoopJoinRel.newBuilder()
          .setType(switch (node.getType()) {
            case INNER -> NestedLoopJoinRel.JoinType.JOIN_TYPE_INNER;
            case LEFT -> NestedLoopJoinRel.JoinType.JOIN_TYPE_LEFT;
            default -> throw new UnsupportedPlanException("join " + node.getType() + " without keys");
          })
          .setLeft(left.rel())
          .setRight(right.rel());
      node.getFilter().ifPresent(filter -> join.setExpression(translator.translate(filter)));
      return project(new Translated(Rel.newBuilder().setNestedLoopJoin(join).build(), combined), node.getOutputSymbols());
    }

    // Gluten reads join keys from a conjunction of eq calls on field references; only the names
    // are inspected, so no function prefix is needed.
    Expression keys = null;
    for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
      int leftIndex = left.layout().indexOf(clause.getLeft());
      int rightIndex = left.layout().size() + right.layout().indexOf(clause.getRight());
      Expression equal = keyCall("eq", clause.getLeft().type(),
          ExpressionTranslator.fieldReference(leftIndex), ExpressionTranslator.fieldReference(rightIndex));
      keys = keys == null ? equal : keyCall("and", null, keys, equal);
    }
    JoinRel.Builder join = JoinRel.newBuilder()
        .setType(switch (node.getType()) {
          case INNER -> JoinRel.JoinType.JOIN_TYPE_INNER;
          case LEFT -> JoinRel.JoinType.JOIN_TYPE_LEFT;
          case RIGHT -> JoinRel.JoinType.JOIN_TYPE_RIGHT;
          case FULL -> JoinRel.JoinType.JOIN_TYPE_OUTER;
        })
        .setLeft(left.rel())
        .setRight(right.rel())
        .setExpression(keys);
    node.getFilter().ifPresent(filter -> join.setPostJoinFilter(translator.translate(filter)));
    return project(new Translated(Rel.newBuilder().setJoin(join).build(), combined), node.getOutputSymbols());
  }

  // Trino's semi join appends a three-valued IN result to the source rows, which is Velox's
  // null-aware left semi project join.
  private Translated visitSemiJoin(SemiJoinNode node) {
    Translated source = visit(node.getSource());
    Translated filtering = visit(node.getFilteringSource());
    int sourceIndex = source.layout().indexOf(node.getSourceJoinSymbol());
    int filteringIndex = source.layout().size() + filtering.layout().indexOf(node.getFilteringSourceJoinSymbol());
    JoinRel join = JoinRel.newBuilder()
        .setType(JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI)
        .setLeft(source.rel())
        .setRight(filtering.rel())
        .setExpression(keyCall("eq", node.getSourceJoinSymbol().type(),
            ExpressionTranslator.fieldReference(sourceIndex), ExpressionTranslator.fieldReference(filteringIndex)))
        .setAdvancedExtension(AdvancedExtension.newBuilder()
            .addOptimization(Any.pack(StringValue.of("isExistenceJoin=1\nisNullAwareAntiJoin=1\n"))))
        .build();
    List<Symbol> layout = ImmutableList.<Symbol>builder()
        .addAll(source.layout())
        .add(node.getSemiJoinOutput())
        .build();
    return project(new Translated(Rel.newBuilder().setJoin(join).build(), layout), node.getOutputSymbols());
  }

  private Expression keyCall(String name, Type argumentType, Expression left, Expression right) {
    String signature = argumentType == null
        ? name + ":bool_bool"
        : name + ":" + TypeTranslator.signatureName(argumentType) + "_" + TypeTranslator.signatureName(argumentType);
    return Expression.newBuilder()
        .setScalarFunction(Expression.ScalarFunction.newBuilder()
            .setFunctionReference(functions.anchor(signature))
            .setOutputType(TypeTranslator.toSubstrait(io.trino.spi.type.BooleanType.BOOLEAN))
            .addArguments(FunctionArgument.newBuilder().setValue(left))
            .addArguments(FunctionArgument.newBuilder().setValue(right)))
        .build();
  }

  private Translated visitAggregation(AggregationNode node) {
    if (node.getGroupingSetCount() != 1 || node.getStep() == AggregationNode.Step.INTERMEDIATE
        || !node.getPreGroupedSymbols().isEmpty() || node.getGroupIdSymbol().isPresent()) {
      throw new UnsupportedPlanException("aggregation " + node.getStep());
    }
    Translated source = visit(node.getSource());
    ExpressionTranslator translator = new ExpressionTranslator(functions, source.layout());
    AggregateRel.Builder aggregate = AggregateRel.newBuilder().setInput(source.rel());
    AggregateRel.Grouping.Builder grouping = AggregateRel.Grouping.newBuilder();
    List<Symbol> layout = new ArrayList<>(node.getGroupingKeys());
    for (int i = 0; i < node.getGroupingKeys().size(); i++) {
      aggregate.addGroupingExpressions(
          ExpressionTranslator.fieldReference(translator.indexOf(node.getGroupingKeys().get(i))));
      grouping.addExpressionReferences(i);
    }
    aggregate.addGroupings(grouping);

    AggregationPhase phase = switch (node.getStep()) {
      case PARTIAL -> AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE;
      case FINAL -> AggregationPhase.AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT;
      case SINGLE -> AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT;
      case INTERMEDIATE -> throw new IllegalStateException();
    };
    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
      AggregationNode.Aggregation aggregation = entry.getValue();
      String name = aggregation.getResolvedFunction().name().functionName();
      if (!GlobalFunctionCatalog.isBuiltinFunctionName(aggregation.getResolvedFunction().name())
          || aggregation.isDistinct() || aggregation.getFilter().isPresent()
          || aggregation.getOrderingScheme().isPresent() || aggregation.getMask().isPresent()) {
        throw new UnsupportedPlanException("aggregate " + name);
      }
      List<Type> rawTypes = aggregation.getResolvedFunction().signature().getArgumentTypes();
      if (!SPLITTABLE_AGGREGATES.contains(name)) {
        throw new UnsupportedPlanException("aggregate " + name + rawTypes + " at step " + node.getStep());
      }
      StringBuilder signature = new StringBuilder(ExpressionTranslator.PRESTO_PREFIX).append(name).append(':');
      for (int i = 0; i < rawTypes.size(); i++) {
        signature.append(i > 0 ? "_" : "").append(TypeTranslator.signatureName(rawTypes.get(i)));
      }
      AggregateFunction.Builder function = AggregateFunction.newBuilder()
          .setFunctionReference(functions.anchor(signature.toString()))
          .setPhase(phase)
          .setOutputType(TypeTranslator.toSubstrait(entry.getKey().type()));
      for (io.trino.sql.ir.Expression argument : aggregation.getArguments()) {
        function.addArguments(FunctionArgument.newBuilder().setValue(translator.translate(argument)));
      }
      aggregate.addMeasures(AggregateRel.Measure.newBuilder().setMeasure(function));
      layout.add(entry.getKey());
    }
    return new Translated(Rel.newBuilder().setAggregate(aggregate).build(), layout);
  }

  private Translated visitLocalExchange(ExchangeNode node) {
    if (node.getScope() != ExchangeNode.Scope.LOCAL || node.getSources().size() != 1) {
      throw new UnsupportedPlanException("exchange " + node.getScope() + " with " + node.getSources().size() + " sources");
    }
    // A single native driver needs no local exchange, and a local merge of its one sorted stream
    // keeps the order; the exchange only renames the source's symbols.
    Translated source = visit(node.getSources().get(0));
    List<Symbol> renamed = new ArrayList<>(source.layout());
    List<Symbol> inputs = node.getInputs().get(0);
    for (int i = 0; i < node.getOutputSymbols().size(); i++) {
      renamed.set(source.layout().indexOf(inputs.get(i)), node.getOutputSymbols().get(i));
    }
    return project(new Translated(source.rel(), renamed), node.getOutputSymbols());
  }

  private Translated visitValues(ValuesNode node) {
    List<Symbol> outputs = node.getOutputSymbols();
    ReadRel.VirtualTable.Builder table = ReadRel.VirtualTable.newBuilder();
    if (node.getRows().isEmpty()) {
      for (int i = 0; i < node.getRowCount(); i++) {
        table.addExpressions(Expression.Nested.Struct.getDefaultInstance());
      }
    } else {
      for (io.trino.sql.ir.Expression row : node.getRows().get()) {
        if (!(row instanceof io.trino.sql.ir.Row items)) {
          throw new UnsupportedPlanException("values row " + row);
        }
        Expression.Nested.Struct.Builder struct = Expression.Nested.Struct.newBuilder();
        for (io.trino.sql.ir.Expression item : items.items()) {
          if (!(item instanceof io.trino.sql.ir.Constant constant)) {
            throw new UnsupportedPlanException("values with non-constant item " + item);
          }
          struct.addFields(ExpressionTranslator.literal(constant.type(), constant.value()));
        }
        table.addExpressions(struct);
      }
    }
    List<String> names = new ArrayList<>();
    for (int i = 0; i < outputs.size(); i++) {
      names.add("c" + i);
    }
    ReadRel read = ReadRel.newBuilder()
        .setBaseSchema(TypeTranslator.namedStruct(names, outputs.stream().map(Symbol::type).toList()))
        .setVirtualTable(table)
        .build();
    return new Translated(Rel.newBuilder().setRead(read).build(), outputs);
  }

  private Translated visitTopN(TopNNode node) {
    Translated source = visit(node.getSource());
    TopNRel topN = TopNRel.newBuilder()
        .setInput(source.rel())
        .addAllSorts(sortFields(node.getOrderingScheme(), source.layout()))
        .setCount(i64(node.getCount()))
        .setMode(FetchMode.FETCH_MODE_ROWS_ONLY)
        .build();
    return new Translated(Rel.newBuilder().setTopN(topN).build(), source.layout());
  }

  private Translated visitLimit(LimitNode node) {
    if (node.isWithTies()) {
      throw new UnsupportedPlanException("limit with ties");
    }
    Translated source = visit(node.getSource());
    FetchRel fetch = FetchRel.newBuilder().setInput(source.rel()).setCountExpr(i64(node.getCount())).build();
    return new Translated(Rel.newBuilder().setFetch(fetch).build(), source.layout());
  }

  private Translated sort(Translated source, OrderingScheme ordering) {
    SortRel sort = SortRel.newBuilder()
        .setInput(source.rel())
        .addAllSorts(sortFields(ordering, source.layout()))
        .build();
    return new Translated(Rel.newBuilder().setSort(sort).build(), source.layout());
  }

  private static List<SortField> sortFields(OrderingScheme ordering, List<Symbol> layout) {
    List<SortField> fields = new ArrayList<>();
    for (Symbol symbol : ordering.orderBy()) {
      SortField.SortDirection direction = switch (ordering.ordering(symbol)) {
        case ASC_NULLS_FIRST -> SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_FIRST;
        case ASC_NULLS_LAST -> SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST;
        case DESC_NULLS_FIRST -> SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_FIRST;
        case DESC_NULLS_LAST -> SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_LAST;
      };
      fields.add(SortField.newBuilder()
          .setExpr(ExpressionTranslator.fieldReference(layout.indexOf(symbol)))
          .setDirection(direction)
          .build());
    }
    return fields;
  }

  private static Expression i64(long value) {
    return Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setI64(value)).build();
  }

  private Translated visitRemoteSource(RemoteSourceNode node) {
    Translated read = readRemoteSource(node);
    // Pages from the upstream tasks arrive interleaved through one input, so a merge becomes a sort.
    return node.getOrderingScheme().map(ordering -> sort(read, ordering)).orElse(read);
  }

  private Translated readRemoteSource(RemoteSourceNode node) {
    int index = inputs.size();
    List<Type> types = node.getOutputSymbols().stream().map(Symbol::type).collect(ImmutableList.toImmutableList());
    inputs.add(new RemoteInput(index, node, types));
    List<String> names = new ArrayList<>();
    for (int i = 0; i < types.size(); i++) {
      names.add("c" + i);
    }
    ReadRel read = ReadRel.newBuilder()
        .setBaseSchema(TypeTranslator.namedStruct(names, types))
        .setLocalFiles(ReadRel.LocalFiles.newBuilder()
            .addItems(ReadRel.LocalFiles.FileOrFiles.newBuilder().setUriFile("iterator:" + index)))
        .build();
    return new Translated(Rel.newBuilder().setRead(read).build(), node.getOutputSymbols());
  }

  /** Reorders or narrows a translated relation to the given symbols. */
  private Translated project(Translated source, List<Symbol> outputs) {
    if (source.layout().equals(outputs)) {
      return source;
    }
    RelCommon.Emit.Builder emit = RelCommon.Emit.newBuilder();
    for (Symbol output : outputs) {
      int index = source.layout().indexOf(output);
      if (index < 0) {
        throw new IllegalStateException("Symbol " + output + " is not produced by " + source.layout());
      }
      emit.addOutputMapping(index);
    }
    ProjectRel project = ProjectRel.newBuilder()
        .setInput(source.rel())
        .setCommon(RelCommon.newBuilder().setEmit(emit))
        .build();
    return new Translated(Rel.newBuilder().setProject(project).build(), outputs);
  }
}
