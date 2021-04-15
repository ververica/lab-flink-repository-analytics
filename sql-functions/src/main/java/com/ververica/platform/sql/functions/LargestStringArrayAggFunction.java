package com.ververica.platform.sql.functions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.functions.AggregateFunction;

/** Aggregate function retaining the largest array seen. */
@FunctionHint(
    input = @DataTypeHint(value = "ARRAY< STRING >", bridgedTo = ArrayData.class),
    output = @DataTypeHint(value = "ARRAY< STRING >", bridgedTo = ArrayData.class))
public class LargestStringArrayAggFunction
    extends AggregateFunction<ArrayData, LargestStringArrayAggFunction.MyAccumulator> {

  public static class MyAccumulator {
    @DataTypeHint(value = "ARRAY< STRING >", bridgedTo = ArrayData.class)
    public ArrayData last;
  }

  @Override
  public MyAccumulator createAccumulator() {
    return new MyAccumulator();
  }

  public void accumulate(MyAccumulator acc, ArrayData value) {
    if (value != null && (acc.last == null || value.size() > acc.last.size())) {
      acc.last = value;
    }
  }

  public void retract(MyAccumulator acc, ArrayData value) {
    acc.last = null;
  }

  @Override
  public ArrayData getValue(MyAccumulator acc) {
    return acc.last;
  }
}
