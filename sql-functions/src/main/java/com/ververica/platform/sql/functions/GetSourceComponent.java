package com.ververica.platform.sql.functions;

import static com.ververica.platform.Utils.SOURCE_FILENAME_COMPONENT_PATTERN;

import java.util.regex.Matcher;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Scalar SQL function to extract the Flink source code component from a filename (including its
 * path relative to the Flink repository).
 */
@SuppressWarnings("unused")
public class GetSourceComponent extends ScalarFunction {

  public String eval(String filename) {
    if (filename == null) {
      return null;
    } else {
      Matcher matcher = SOURCE_FILENAME_COMPONENT_PATTERN.matcher(filename);
      if (!matcher.matches()) {
        return null;
      } else {
        return matcher.group("component");
      }
    }
  }
}
