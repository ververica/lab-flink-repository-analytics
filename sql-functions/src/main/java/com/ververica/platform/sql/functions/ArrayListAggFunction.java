package com.ververica.platform.sql.functions;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.functions.AggregateFunction;

/** Aggregate function for putting strings into an array. */
@SuppressWarnings("unused")
public class ArrayListAggFunction extends AggregateFunction<List<String>, List<String>> {

  public void accumulate(List<String> acc, String value) {
    if (value != null) {
      acc.add(value);
    }
  }

  @Override
  public List<String> getValue(List<String> acc) {
    return acc;
  }

  @Override
  public List<String> createAccumulator() {
    return new ArrayList<>();
  }
}
