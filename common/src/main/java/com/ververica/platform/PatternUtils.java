package com.ververica.platform;

import java.util.regex.Pattern;

/** Common store for Regexp patterns and other utility functions. */
public class PatternUtils {
  /** Matches Flink source code components from file names (relative to the repository path). */
  public static final Pattern SOURCE_FILENAME_COMPONENT_PATTERN =
      Pattern.compile(
          "^(?<component>.+?(?=/src/.*|pom.xml|README.md)|(?:flink-)?docs(?=/.*)|tools(?=/.*)|flink-python(?=/.*)|flink-end-to-end-tests/test-scripts(?=/.*)|flink-scala-shell(?=/start-script/.*)|flink-container(?=/.*)|flink-contrib/docker-flink(?=/.*)|flink-table/flink-sql-client(?=/.*)|flink-end-to-end-tests(?=/[^/]*\\.sh)).*?");

  /**
   * Normalises email subjects to a common representation, removing things that email editors add
   * when replying.
   */
  public static final Pattern EMAIL_SUBJECT_THREAD_PATTERN =
      Pattern.compile("^\\s*(?:(?:Re|AW):\\s*)*(?<emailthread>.*?)\\s*$", Pattern.CASE_INSENSITIVE);

  /** Finds a Jira ticket action and number from the message's "Subject" field (as sent by Jira). */
  public static final Pattern EMAIL_SUBJECT_JIRA_TICKET_PATTERN =
      Pattern.compile(
          "\\[jira\\]\\s*\\[(?<ticketaction>.*)\\]\\s*\\((?<ticketnumber>FLINK-[0-9]+)\\).*");

  /** Finds a Jira ticket author from the message's "From" field (as sent by Jira). */
  public static final Pattern EMAIL_FROM_JIRA_TICKET_AUTHOR_PATTERN =
      Pattern.compile("\"(?<ticketauthor>.*)\\s*\\((?:Jira|JIRA)\\)\"\\s*<jira@apache.org>");

  /**
   * Matches a line of the email's text body that contains component information (using the message
   * format sent by Jira).
   */
  public static final Pattern EMAIL_BODY_JIRA_TICKET_COMPONENTS_PATTERN =
      Pattern.compile(" {10}Components: (?<components>.*)");

  /**
   * Extracts components from a (comma-separated) list of components (as matched by {@link
   * #EMAIL_BODY_JIRA_TICKET_COMPONENTS_PATTERN}) that also takes care of components which include
   * commas like "Formats (JSON, Avro, Parquet, ORC, SequenceFile)".
   *
   * <p>It uses a positive look-ahead to search for all ", " followed by any pairwise bracketed
   * content (if available).
   */
  public static final Pattern EMAIL_BODY_JIRA_TICKET_COMPONENTS_SPLIT_PATTERN =
      Pattern.compile(", (?=(?:[^(]*\\([^)]*\\))*[^)]*$)");
}
