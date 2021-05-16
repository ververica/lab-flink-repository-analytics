package com.ververica.platform.sql.functions;

import java.util.Arrays;
import org.apache.flink.table.functions.ScalarFunction;

/** Scalar SQL function that repeats the given input N times into an array of size N. */
@SuppressWarnings("unused")
public class ArrayRepeat extends ScalarFunction {

  public String[] eval(String pattern, Integer repetitions) {
    if (pattern == null || repetitions == null) {
      return null;
    } else {
      String[] result = new String[repetitions];
      Arrays.fill(result, pattern);
      return result;
    }
  }

  public Integer[] eval(Integer pattern, Integer repetitions) {
    if (pattern == null || repetitions == null) {
      return null;
    } else {
      Integer[] result = new Integer[repetitions];
      Arrays.fill(result, pattern);
      return result;
    }
  }
}
