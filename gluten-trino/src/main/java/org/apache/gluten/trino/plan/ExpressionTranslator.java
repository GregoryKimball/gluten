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

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.airlift.slice.Slice;
import io.substrait.proto.Expression;
import io.substrait.proto.FunctionArgument;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.type.LikePattern;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BooleanType.BOOLEAN;

/**
 * Translates Trino IR expressions over a fixed input layout into Substrait expressions. Scalar
 * functions resolve to Velox's Presto functions, registered under {@code presto.} by
 * libgluten_trino.so; {@code and}, {@code or}, {@code if}, {@code switch}, {@code coalesce} and
 * casts are Velox special forms and keep their own names.
 */
public final class ExpressionTranslator {
  static final String PRESTO_PREFIX = "presto.";

  private static final Map<String, String> OPERATORS =
      ImmutableMap.<String, String>builder()
          .put("$operator$add", "plus")
          .put("$operator$subtract", "minus")
          .put("$operator$multiply", "multiply")
          .put("$operator$divide", "divide")
          .put("$operator$modulus", "mod")
          .put("$operator$negation", "negate")
          .put("$operator$equal", "eq")
          .put("$operator$less_than", "lt")
          .put("$operator$less_than_or_equal", "lte")
          .put("$operator$is_distinct_from", "distinct_from")
          .put("$not", "not")
          .put("substring", "substr")
          .buildOrThrow();

  // Builtin functions whose Velox Presto implementation has the same name and semantics. Kept
  // explicit so an unverified function fails the query instead of running with different
  // semantics.
  private static final java.util.Set<String> FUNCTIONS =
      java.util.Set.of("abs", "substr", "lower", "upper", "length", "year", "month", "day");

  private final FunctionCollector functions;
  private final Map<Symbol, Integer> layout;
  private final Map<String, io.trino.sql.ir.Expression> bindings = new HashMap<>();

  public ExpressionTranslator(FunctionCollector functions, List<Symbol> inputLayout) {
    this.functions = functions;
    this.layout = new HashMap<>();
    for (int i = 0; i < inputLayout.size(); i++) {
      layout.putIfAbsent(inputLayout.get(i), i);
    }
  }

  public static Expression fieldReference(int index) {
    return Expression.newBuilder()
        .setSelection(
            Expression.FieldReference.newBuilder()
                .setDirectReference(
                    Expression.ReferenceSegment.newBuilder()
                        .setStructField(
                            Expression.ReferenceSegment.StructField.newBuilder().setField(index)))
                .setRootReference(Expression.FieldReference.RootReference.getDefaultInstance()))
        .build();
  }

  public int indexOf(Symbol symbol) {
    Integer index = layout.get(symbol);
    if (index == null) {
      throw new IllegalStateException("Symbol " + symbol + " is not in the input layout");
    }
    return index;
  }

  public Expression translate(io.trino.sql.ir.Expression expression) {
    return switch (expression) {
      case Reference reference -> translateReference(reference);
      case Constant constant -> literal(constant.type(), constant.value());
      case Call call -> translateCall(call);
      case Logical logical -> translateLogical(logical);
      case Case caseExpression -> translateCase(caseExpression);
      case Cast cast -> translateCast(cast);
      case IsNull isNull -> scalar("is_null", BOOLEAN, List.of(isNull.value()));
      case In in -> translateIn(in);
      case Coalesce coalesce -> specialForm("coalesce", coalesce.type(), coalesce.operands());
      case Let let -> translateLet(let);
      default -> throw new UnsupportedPlanException("expression " + expression);
    };
  }

  private Expression translateReference(Reference reference) {
    io.trino.sql.ir.Expression bound = bindings.get(reference.name());
    if (bound != null) {
      return translate(bound);
    }
    return fieldReference(indexOf(Symbol.from(reference)));
  }

  private Expression translateLet(Let let) {
    // Inlining evaluates the bound value once per use; Trino binds only deterministic values
    // this way (for example the operand of BETWEEN).
    bindings.put(let.name().name(), let.value());
    try {
      return translate(let.body());
    } finally {
      bindings.remove(let.name().name());
    }
  }

  private Expression translateCall(Call call) {
    CatalogSchemaFunctionName name = call.function().name();
    if (!GlobalFunctionCatalog.isBuiltinFunctionName(name)) {
      throw new UnsupportedPlanException("function " + name);
    }
    String functionName = name.functionName();
    if (functionName.equals("$like")) {
      return translateLike(call);
    }
    if (functionName.equals("$divide_round_to_scale")) {
      // Presto's decimal divide keeps the dividend's scale when the divisor has scale 0, and rounds
      // HALF_UP like Trino.
      Expression divisor = cast(translate(call.arguments().get(1)), DecimalType.createDecimalType(19, 0));
      return decimalCall("divide", call.type(), translate(call.arguments().get(0)),
          (DecimalType) call.arguments().get(0).type(), divisor, DecimalType.createDecimalType(19, 0));
    }
    if (functionName.equals("$operator$divide") && call.type() instanceof DecimalType result) {
      return translateDecimalDivide(call, result);
    }
    String velox = OPERATORS.get(functionName);
    if (velox == null && FUNCTIONS.contains(functionName)) {
      velox = functionName;
    }
    if (velox == null) {
      throw new UnsupportedPlanException("function " + functionName);
    }
    return scalar(velox, call.type(), call.arguments());
  }

  // Trino widens the quotient's scale (to at least 6), while Presto's divide returns
  // max(dividend scale, divisor scale). Rescaling the dividend to Trino's result scale makes
  // Presto's quotient carry that scale with the same HALF_UP rounding.
  private Expression translateDecimalDivide(Call call, DecimalType result) {
    DecimalType dividendType = (DecimalType) call.arguments().get(0).type();
    DecimalType divisorType = (DecimalType) call.arguments().get(1).type();
    if (divisorType.getScale() > result.getScale()) {
      throw new UnsupportedPlanException("decimal divide by " + divisorType + " into " + result);
    }
    Expression dividend = translate(call.arguments().get(0));
    if (dividendType.getScale() != result.getScale()) {
      dividendType = DecimalType.createDecimalType(38, result.getScale());
      dividend = cast(dividend, dividendType);
    }
    DecimalType quotient = DecimalType.createDecimalType(
        Math.min(38, dividendType.getPrecision() + divisorType.getScale()
            + Math.max(0, divisorType.getScale() - dividendType.getScale())),
        result.getScale());
    Expression divide = decimalCall("divide", quotient, dividend, dividendType,
        translate(call.arguments().get(1)), divisorType);
    return quotient.equals(result) ? divide : cast(divide, result);
  }

  private Expression decimalCall(
      String name, Type outputType, Expression left, DecimalType leftType, Expression right, DecimalType rightType) {
    String signature = PRESTO_PREFIX + name + ":" + TypeTranslator.signatureName(leftType)
        + "_" + TypeTranslator.signatureName(rightType);
    return Expression.newBuilder()
        .setScalarFunction(Expression.ScalarFunction.newBuilder()
            .setFunctionReference(functions.anchor(signature))
            .setOutputType(TypeTranslator.toSubstrait(outputType))
            .addArguments(FunctionArgument.newBuilder().setValue(left))
            .addArguments(FunctionArgument.newBuilder().setValue(right)))
        .build();
  }

  private static Expression cast(Expression input, Type target) {
    return Expression.newBuilder()
        .setCast(Expression.Cast.newBuilder()
            .setType(TypeTranslator.toSubstrait(target))
            .setInput(input)
            .setFailureBehavior(Expression.Cast.FailureBehavior.FAILURE_BEHAVIOR_THROW_EXCEPTION))
        .build();
  }

  // Trino folds the pattern into a compiled LikePattern constant; Velox's like takes the pattern
  // text and escape character.
  private Expression translateLike(Call call) {
    if (!(call.arguments().get(1) instanceof Constant constant) || !(constant.value() instanceof LikePattern pattern)) {
      throw new UnsupportedPlanException("like with a non-constant pattern");
    }
    List<Expression> arguments = new java.util.ArrayList<>();
    arguments.add(translate(call.arguments().get(0)));
    arguments.add(literal(VarcharType.VARCHAR, io.airlift.slice.Slices.utf8Slice(pattern.getPattern())));
    pattern.getEscape().ifPresent(escape ->
        arguments.add(literal(VarcharType.VARCHAR, io.airlift.slice.Slices.utf8Slice(escape.toString()))));
    String signature = PRESTO_PREFIX + "like:" + String.join("_", java.util.Collections.nCopies(arguments.size(), "str"));
    Expression.ScalarFunction.Builder function = Expression.ScalarFunction.newBuilder()
        .setFunctionReference(functions.anchor(signature))
        .setOutputType(TypeTranslator.toSubstrait(BOOLEAN));
    arguments.forEach(argument -> function.addArguments(FunctionArgument.newBuilder().setValue(argument)));
    return Expression.newBuilder().setScalarFunction(function).build();
  }

  private Expression translateLogical(Logical logical) {
    String name = logical.operator() == Logical.Operator.AND ? "and" : "or";
    return specialForm(name, BOOLEAN, logical.terms());
  }

  private Expression translateCase(Case caseExpression) {
    Expression.IfThen.Builder ifThen = Expression.IfThen.newBuilder();
    for (WhenClause clause : caseExpression.whenClauses()) {
      ifThen.addIfs(
          Expression.IfThen.IfClause.newBuilder()
              .setIf(translate(clause.operand()))
              .setThen(translate(clause.result())));
    }
    ifThen.setElse(translate(caseExpression.defaultValue()));
    return Expression.newBuilder().setIfThen(ifThen).build();
  }

  private Expression translateCast(Cast cast) {
    Type source = cast.expression().type();
    Type target = cast.type();
    // Varchar length changes are representation-only. Other casts must be ones where Presto and
    // Trino agree; numeric widening and narrowing do (both fail on overflow).
    if (!(isNumeric(source) && isNumeric(target))
        && !(source instanceof VarcharType && target instanceof VarcharType)) {
      throw new UnsupportedPlanException("cast from " + source + " to " + target);
    }
    return Expression.newBuilder()
        .setCast(
            Expression.Cast.newBuilder()
                .setType(TypeTranslator.toSubstrait(target))
                .setInput(translate(cast.expression()))
                .setFailureBehavior(Expression.Cast.FailureBehavior.FAILURE_BEHAVIOR_THROW_EXCEPTION))
        .build();
  }

  private Expression translateIn(In in) {
    Expression.SingularOrList.Builder list =
        Expression.SingularOrList.newBuilder().setValue(translate(in.value()));
    for (io.trino.sql.ir.Expression option : in.valueList()) {
      if (!(option instanceof Constant)) {
        throw new UnsupportedPlanException("IN with non-constant values");
      }
      list.addOptions(translate(option));
    }
    return Expression.newBuilder().setSingularOrList(list).build();
  }

  private Expression scalar(String veloxName, Type outputType, List<io.trino.sql.ir.Expression> arguments) {
    return call(PRESTO_PREFIX + veloxName, outputType, arguments);
  }

  private Expression specialForm(String name, Type outputType, List<io.trino.sql.ir.Expression> arguments) {
    return call(name, outputType, arguments);
  }

  private Expression call(String name, Type outputType, List<io.trino.sql.ir.Expression> arguments) {
    StringBuilder signature = new StringBuilder(name).append(':');
    for (int i = 0; i < arguments.size(); i++) {
      if (i > 0) {
        signature.append('_');
      }
      signature.append(TypeTranslator.signatureName(arguments.get(i).type()));
    }
    Expression.ScalarFunction.Builder function =
        Expression.ScalarFunction.newBuilder()
            .setFunctionReference(functions.anchor(signature.toString()))
            .setOutputType(TypeTranslator.toSubstrait(outputType));
    for (io.trino.sql.ir.Expression argument : arguments) {
      function.addArguments(FunctionArgument.newBuilder().setValue(translate(argument)));
    }
    return Expression.newBuilder().setScalarFunction(function).build();
  }

  public static Expression literal(Type type, Object value) {
    Expression.Literal.Builder literal = Expression.Literal.newBuilder().setNullable(true);
    if (value == null) {
      literal.setNull(TypeTranslator.toSubstrait(type));
    } else if (type instanceof BooleanType) {
      literal.setBoolean((Boolean) value);
    } else if (type instanceof TinyintType) {
      literal.setI8(((Long) value).intValue());
    } else if (type instanceof SmallintType) {
      literal.setI16(((Long) value).intValue());
    } else if (type instanceof IntegerType) {
      literal.setI32(((Long) value).intValue());
    } else if (type instanceof BigintType) {
      literal.setI64((Long) value);
    } else if (type instanceof RealType) {
      literal.setFp32(Float.intBitsToFloat(((Long) value).intValue()));
    } else if (type instanceof DoubleType) {
      literal.setFp64((Double) value);
    } else if (type instanceof DateType) {
      literal.setDate(((Long) value).intValue());
    } else if (type instanceof VarcharType) {
      literal.setString(((Slice) value).toStringUtf8());
    } else if (type instanceof VarbinaryType) {
      literal.setBinary(ByteString.copyFrom(((Slice) value).getBytes()));
    } else if (type instanceof DecimalType decimal) {
      long high;
      long low;
      if (decimal.isShort()) {
        low = (Long) value;
        high = low < 0 ? -1 : 0;
      } else {
        Int128 int128 = (Int128) value;
        high = int128.getHigh();
        low = int128.getLow();
      }
      ByteBuffer bytes = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
      bytes.putLong(low).putLong(high);
      literal.setDecimal(
          Expression.Literal.Decimal.newBuilder()
              .setValue(ByteString.copyFrom(bytes.array()))
              .setPrecision(decimal.getPrecision())
              .setScale(decimal.getScale()));
    } else {
      throw new UnsupportedPlanException("literal of type " + type);
    }
    return Expression.newBuilder().setLiteral(literal).build();
  }

  private static boolean isNumeric(Type type) {
    return type instanceof TinyintType
        || type instanceof SmallintType
        || type instanceof IntegerType
        || type instanceof BigintType
        || type instanceof RealType
        || type instanceof DoubleType
        || type instanceof DecimalType;
  }
}
