package com.ververica.platform.sql.functions;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.utils.DataTypeUtils;

/** Aggregate function for collecting the latest non-null data of type T (using internal types). */
@SuppressWarnings("unused")
public class LastNonNullValueAggFunction2
    extends AggregateFunction<Object, LastNonNullValueAggFunction2.MyAccumulator> {

  public static class MyAccumulator {
    public Object acc = null;
  }

  @Override
  public MyAccumulator createAccumulator() {
    return new MyAccumulator();
  }

  public void accumulate(MyAccumulator acc, Object value) {
    if (value != null) {
      acc.acc = value;
    }
  }

  public void retract(MyAccumulator acc, Object value) {
    acc.acc = null;
  }

  @Override
  public Object getValue(MyAccumulator acc) {
    return acc.acc;
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return TypeInference.newBuilder()
        .inputTypeStrategy(
            new InputTypeStrategy() {
              @Override
              public ArgumentCount getArgumentCount() {
                return ConstantArgumentCount.of(1);
              }

              @Override
              public Optional<List<DataType>> inferInputTypes(
                  CallContext callContext, boolean throwOnFailure) {
                DataType argType = callContext.getArgumentDataTypes().get(0);
                return Optional.of(
                    Collections.singletonList(DataTypeUtils.toInternalDataType(argType)));
              }

              @Override
              public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
                return Collections.singletonList(Signature.of(Signature.Argument.of("value", "T")));
              }
            })
        .accumulatorTypeStrategy(
            callContext -> {
              DataType argType = callContext.getArgumentDataTypes().get(0);
              DataType argTypeInternal = DataTypeUtils.toInternalDataType(argType);
              return Optional.of(
                  DataTypes.STRUCTURED(
                      MyAccumulator.class, DataTypes.FIELD("acc", argTypeInternal)));
            })
        .outputTypeStrategy(
            callContext -> {
              DataType argType = callContext.getArgumentDataTypes().get(0);
              DataType argTypeInternal = DataTypeUtils.toInternalDataType(argType);
              return Optional.of(argTypeInternal);
            })
        .build();
  }
}
