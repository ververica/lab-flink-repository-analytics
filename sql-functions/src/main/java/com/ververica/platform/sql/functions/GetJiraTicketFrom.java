package com.ververica.platform.sql.functions;

import java.util.regex.Matcher;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Scalar SQL function to extract the author name of a ticket created/change email from the FROM
 * field sent by Jira.
 */
@SuppressWarnings("unused")
public class GetJiraTicketFrom extends ScalarFunction {

  public String eval(String fromField) {
    if (fromField == null) {
      return null;
    } else {
      Matcher matcher = PatternUtils.EMAIL_FROM_JIRA_TICKET_AUTHOR_PATTERN.matcher(fromField);
      if (!matcher.matches()) {
        return null;
      } else {
        return matcher.group(1);
      }
    }
  }
}
