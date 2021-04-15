package com.ververica.platform.sql.functions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AggregateFunction;

/** Aggregate function for putting strings into an array (using internal types). */
@SuppressWarnings("unused")
@FunctionHint(
    input = @DataTypeHint(value = "STRING", bridgedTo = StringData.class),
    output = @DataTypeHint(value = "ARRAY<STRING>", bridgedTo = ArrayData.class))
public class ArrayListAggFunction2
    extends AggregateFunction<ArrayData, ArrayListAggFunction2.MyAccumulator> {

  public static class MyAccumulator {
    @DataTypeHint(value = "ARRAY< STRING >", bridgedTo = ArrayData.class)
    public ListView<StringData> list = new ListView<>();

    public long count = 0L;
  }

  @Override
  public MyAccumulator createAccumulator() {
    return new MyAccumulator();
  }

  public void accumulate(MyAccumulator acc, StringData value) throws Exception {
    if (value != null) {
      acc.list.add(value);
      acc.count++;
    }
  }

  @Override
  public ArrayData getValue(MyAccumulator acc) {
    return new GenericArrayData(acc.list.getList().toArray(new StringData[0]));
  }
}
