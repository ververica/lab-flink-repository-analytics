package com.ververica.platform.sql.functions;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
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

/**
 * Aggregate function for putting data of type T into an array of elements T (using internal types).
 */
@SuppressWarnings("unused")
public class ArrayListAggFunction4
    extends AggregateFunction<ArrayData, ArrayListAggFunction4.MyAccumulator> {

  public static class MyAccumulator {
    public ListView<Object> list = new ListView<>();

    public long count = 0L;
  }

  @Override
  public MyAccumulator createAccumulator() {
    return new MyAccumulator();
  }

  public void accumulate(MyAccumulator acc, Object value) throws Exception {
    if (value != null) {
      acc.list.add(value);
      acc.count++;
    }
  }

  @Override
  public ArrayData getValue(MyAccumulator acc) {
    return new GenericArrayData(acc.list.getList().toArray(new Object[0]));
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

                /*
                // if you need to verify the input type or want to exclude a certain type:
                if (argType.getLogicalType().getTypeRoot() == LogicalTypeRoot.ARRAY) {
                  if (throwOnFailure) {
                    throw callContext.newValidationError("Invalid input type");
                  } else {
                    return Optional.empty();
                  }
                }
                */

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
                      MyAccumulator.class,
                      DataTypes.FIELD("list", ListView.newListViewDataType(argTypeInternal)),
                      DataTypes.FIELD("count", DataTypes.BIGINT().bridgedTo(long.class))));
            })
        .outputTypeStrategy(
            callContext -> {
              DataType argType = callContext.getArgumentDataTypes().get(0);
              DataType argTypeInternal = DataTypeUtils.toInternalDataType(argType);
              return Optional.of(DataTypes.ARRAY(argTypeInternal).bridgedTo(ArrayData.class));
            })
        .build();
  }
}
