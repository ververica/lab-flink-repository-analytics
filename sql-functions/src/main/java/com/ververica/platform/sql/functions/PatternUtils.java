package com.ververica.platform.sql.functions;

import java.util.regex.Pattern;

/** Common store for Regexp patterns and other utility functions. */
public class PatternUtils {
  static Pattern EMAIL_SUBJECT_THREAD_PATTERN =
      Pattern.compile("^\\s*(?:(?:Re|AW):\\s*)*(?<emailthread>.*?)\\s*$", Pattern.CASE_INSENSITIVE);

  static Pattern EMAIL_SUBJECT_JIRA_TICKET_PATTERN =
      Pattern.compile(
          "\\[jira\\]\\s*\\[(?<ticketaction>.*)\\]\\s*\\((?<ticketnumber>FLINK-[0-9]+)\\).*");

  static Pattern EMAIL_FROM_JIRA_TICKET_AUTHOR_PATTERN =
      Pattern.compile("\"(?<ticketauthor>.*)\\s*\\((?:Jira|JIRA)\\)\"\\s*<jira@apache.org>");
}
