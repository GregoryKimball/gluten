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

import io.substrait.proto.NamedStruct;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.List;

/**
 * Maps Trino types to the Substrait types Gluten's converter understands. Types whose page
 * encoding or semantics native code does not handle are rejected, which sends the fragment back
 * to Trino.
 */
public final class TypeTranslator {
  private static final io.substrait.proto.Type.Nullability NULLABLE =
      io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE;

  private TypeTranslator() {}

  public static io.substrait.proto.Type toSubstrait(Type type) {
    io.substrait.proto.Type.Builder builder = io.substrait.proto.Type.newBuilder();
    if (type instanceof BooleanType) {
      builder.setBool(io.substrait.proto.Type.Boolean.newBuilder().setNullability(NULLABLE));
    } else if (type instanceof TinyintType) {
      builder.setI8(io.substrait.proto.Type.I8.newBuilder().setNullability(NULLABLE));
    } else if (type instanceof SmallintType) {
      builder.setI16(io.substrait.proto.Type.I16.newBuilder().setNullability(NULLABLE));
    } else if (type instanceof IntegerType) {
      builder.setI32(io.substrait.proto.Type.I32.newBuilder().setNullability(NULLABLE));
    } else if (type instanceof BigintType) {
      builder.setI64(io.substrait.proto.Type.I64.newBuilder().setNullability(NULLABLE));
    } else if (type instanceof RealType) {
      builder.setFp32(io.substrait.proto.Type.FP32.newBuilder().setNullability(NULLABLE));
    } else if (type instanceof DoubleType) {
      builder.setFp64(io.substrait.proto.Type.FP64.newBuilder().setNullability(NULLABLE));
    } else if (type instanceof VarcharType) {
      builder.setString(io.substrait.proto.Type.String.newBuilder().setNullability(NULLABLE));
    } else if (type instanceof VarbinaryType) {
      builder.setBinary(io.substrait.proto.Type.Binary.newBuilder().setNullability(NULLABLE));
    } else if (type instanceof DateType) {
      builder.setDate(io.substrait.proto.Type.Date.newBuilder().setNullability(NULLABLE));
    } else if (type instanceof DecimalType decimal) {
      builder.setDecimal(
          io.substrait.proto.Type.Decimal.newBuilder()
              .setPrecision(decimal.getPrecision())
              .setScale(decimal.getScale())
              .setNullability(NULLABLE));
    } else if (type instanceof RowType row) {
      // Rows appear as aggregation intermediates, such as avg(double) as row(double, bigint).
      io.substrait.proto.Type.Struct.Builder struct =
          io.substrait.proto.Type.Struct.newBuilder().setNullability(NULLABLE);
      for (Type field : row.getTypeParameters()) {
        struct.addTypes(toSubstrait(field));
      }
      builder.setStruct(struct);
    } else {
      // CHAR is rejected too: Trino compares CHAR values with padding semantics that VARCHAR
      // functions do not reproduce.
      throw new UnsupportedPlanException("type " + type);
    }
    return builder.build();
  }

  /** Short name used in Gluten function signatures, such as {@code presto.lt:i64_i64}. */
  public static String signatureName(Type type) {
    if (type instanceof BooleanType) {
      return "bool";
    } else if (type instanceof TinyintType) {
      return "i8";
    } else if (type instanceof SmallintType) {
      return "i16";
    } else if (type instanceof IntegerType) {
      return "i32";
    } else if (type instanceof BigintType) {
      return "i64";
    } else if (type instanceof RealType) {
      return "fp32";
    } else if (type instanceof DoubleType) {
      return "fp64";
    } else if (type instanceof VarcharType) {
      return "str";
    } else if (type instanceof VarbinaryType) {
      return "vbin";
    } else if (type instanceof DateType) {
      return "date";
    } else if (type instanceof DecimalType decimal) {
      return "dec<" + decimal.getPrecision() + "," + decimal.getScale() + ">";
    }
    throw new UnsupportedPlanException("type " + type);
  }

  public static NamedStruct namedStruct(List<String> names, List<Type> types) {
    io.substrait.proto.Type.Struct.Builder struct =
        io.substrait.proto.Type.Struct.newBuilder().setNullability(NULLABLE);
    for (Type type : types) {
      struct.addTypes(toSubstrait(type));
    }
    return NamedStruct.newBuilder().addAllNames(names).setStruct(struct).build();
  }
}
