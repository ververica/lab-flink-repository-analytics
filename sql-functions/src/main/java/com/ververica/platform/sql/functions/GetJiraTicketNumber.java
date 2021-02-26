package com.ververica.platform.sql.functions;

import com.ververica.platform.PatternUtils;
import java.util.regex.Matcher;
import org.apache.flink.table.functions.ScalarFunction;

/** Scalar SQL function to extract the ticket ID (FLINK-***) from the email subject sent by Jira. */
@SuppressWarnings("unused")
public class GetJiraTicketNumber extends ScalarFunction {

  public String eval(String subjectField) {
    if (subjectField == null) {
      return null;
    } else {
      Matcher matcher = PatternUtils.EMAIL_SUBJECT_JIRA_TICKET_PATTERN.matcher(subjectField);
      if (!matcher.matches()) {
        return null;
      } else {
        return matcher.group("ticketnumber");
      }
    }
  }
}
