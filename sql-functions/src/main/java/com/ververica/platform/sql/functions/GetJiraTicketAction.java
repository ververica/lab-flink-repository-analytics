package com.ververica.platform.sql.functions;

import java.util.regex.Matcher;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Scalar SQL function to get the Jira ticket action from an email subject, i.e.
 *
 * <ul>
 *   <li>Assigned,
 *   <li>Closed,
 *   <li>Commented,
 *   <li>Created,
 *   <li>Resolved,
 *   <li>Updated,
 *   <li>...
 * </ul>
 */
@SuppressWarnings("unused")
public class GetJiraTicketAction extends ScalarFunction {

  public String eval(String fromField) {
    if (fromField == null) {
      return null;
    } else {
      Matcher matcher = PatternUtils.EMAIL_SUBJECT_JIRA_TICKET_PATTERN.matcher(fromField);
      if (!matcher.matches()) {
        return null;
      } else {
        return matcher.group(1);
      }
    }
  }
}
