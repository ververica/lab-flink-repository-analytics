package com.ververica.platform.sql.functions;

import java.util.regex.Pattern;

/** Common store for Regexp patterns and other utility functions. */
public class PatternUtils {
  static Pattern EMAIL_SUBJECT_JIRA_TICKET_PATTERN =
      Pattern.compile("\\[jira\\]\\s*\\[(.*)\\]\\s*\\((FLINK-[0-9]+)\\).*");

  static Pattern EMAIL_FROM_JIRA_TICKET_AUTHOR_PATTERN =
      Pattern.compile("\"(.*)\\s*\\((?:Jira|JIRA)\\)\"\\s*<jira@apache.org>");
}
