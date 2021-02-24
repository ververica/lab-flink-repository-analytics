package com.ververica.platform.sql.functions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Scalar SQL function to extract the Jira component of a ticket created/change email from text body
 * field sent by Jira.
 */
@SuppressWarnings("unused")
public class GetJiraTicketComponents extends ScalarFunction {

  /** Pattern to match a line of the email's text body that contains component information. */
  static Pattern EMAIL_BODY_JIRA_TICKET_COMPONENTS_PATTERN =
      Pattern.compile(" {10}Components: (?<components>.*)");

  /**
   * Pattern to extract components from a (comma-separated) list of components that also takes care
   * of components which include commas like "Formats (JSON, Avro, Parquet, ORC, SequenceFile)".
   *
   * <p>It uses a positive look-ahead to search for all ", " followed by any pairwise bracketed
   * content (if available).
   */
  static Pattern EMAIL_BODY_JIRA_TICKET_COMPONENTS_SPLIT_PATTERN =
      Pattern.compile(", (?=(?:[^(]*\\([^)]*\\))*[^)]*$)");

  public String[] eval(String textBody) {
    if (textBody == null) {
      return null;
    } else {
      Matcher matcher = EMAIL_BODY_JIRA_TICKET_COMPONENTS_PATTERN.matcher(textBody);
      if (!matcher.find()) {
        return new String[0];
      } else {
        return EMAIL_BODY_JIRA_TICKET_COMPONENTS_SPLIT_PATTERN.split(matcher.group("components"));
      }
    }
  }
}
