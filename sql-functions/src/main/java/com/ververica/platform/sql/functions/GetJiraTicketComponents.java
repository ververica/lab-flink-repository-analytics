package com.ververica.platform.sql.functions;

import com.ververica.platform.PatternUtils;
import java.util.regex.Matcher;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Scalar SQL function to extract the Jira component of a ticket created/change email from text body
 * field sent by Jira.
 *
 * @see ExpandJiraTicketComponents for a Table function version of this
 */
@SuppressWarnings("unused")
public class GetJiraTicketComponents extends ScalarFunction {

  public String[] eval(String textBody) {
    if (textBody == null) {
      return null;
    } else {
      Matcher matcher = PatternUtils.EMAIL_BODY_JIRA_TICKET_COMPONENTS_PATTERN.matcher(textBody);
      if (!matcher.find()) {
        return new String[0];
      } else {
        return PatternUtils.EMAIL_BODY_JIRA_TICKET_COMPONENTS_SPLIT_PATTERN.split(
            matcher.group("components"));
      }
    }
  }
}
