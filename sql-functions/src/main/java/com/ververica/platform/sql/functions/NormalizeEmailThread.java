package com.ververica.platform.sql.functions;

import com.ververica.platform.PatternUtils;
import java.util.regex.Matcher;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Scalar SQL function to extract a normalized the email thread name from the subject, e.g. by
 * removing "Re:" etc.
 */
@SuppressWarnings("unused")
public class NormalizeEmailThread extends ScalarFunction {

  public String eval(String subjectField) {
    if (subjectField == null) {
      return null;
    } else {
      Matcher matcher = PatternUtils.EMAIL_SUBJECT_THREAD_PATTERN.matcher(subjectField);
      if (!matcher.matches()) {
        return null;
      } else {
        return matcher.group("emailthread");
      }
    }
  }
}
