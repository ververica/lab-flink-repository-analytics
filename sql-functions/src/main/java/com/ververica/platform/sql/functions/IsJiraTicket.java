package com.ververica.platform.sql.functions;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Scalar SQL function to identify emails on the flink-dev mailing list which are sent by Jira (by
 * looking at the FROM field).
 */
@SuppressWarnings("unused")
public class IsJiraTicket extends ScalarFunction {

  public Boolean eval(String fromField) {
    if (fromField == null) {
      return false;
    } else {
      return PatternUtils.EMAIL_FROM_JIRA_TICKET_AUTHOR_PATTERN.matcher(fromField).matches();
    }
  }
}
