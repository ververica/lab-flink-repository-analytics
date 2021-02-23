package com.ververica.platform;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.regex.Pattern;

public class Utils {
  public static Pattern SOURCE_FILENAME_COMPONENT_PATTERN =
      Pattern.compile(
          "^(?<component>.+?(?=/src/.*|pom.xml|README.md)|(?:flink-)?docs(?=/.*)|tools(?=/.*)|flink-python(?=/.*)|flink-end-to-end-tests/test-scripts(?=/.*)|flink-scala-shell(?=/start-script/.*)|flink-container(?=/.*)|flink-contrib/docker-flink(?=/.*)|flink-table/flink-sql-client(?=/.*)|flink-end-to-end-tests(?=/[^/]*\\.sh)).*?");

  public static final ZoneId EVALUATION_ZONE = ZoneId.of("UTC");

  public static final DateTimeFormatter DATE_OR_DATETIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
          .appendLiteral('-')
          .appendValue(MONTH_OF_YEAR, 2)
          .optionalStart()
          .appendLiteral('-')
          .appendValue(DAY_OF_MONTH, 2)
          .optionalStart()
          .appendLiteral('T')
          .append(ISO_LOCAL_TIME)
          .appendLiteral('Z')
          .optionalEnd()
          .optionalEnd()
          .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
          .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
          .toFormatter();

  public static LocalDateTime parseFlexibleDate(String dateString) {
    if (dateString == null || dateString.isEmpty()) {
      return LocalDateTime.now();
    } else {
      return LocalDateTime.parse(dateString, DATE_OR_DATETIME_FORMATTER);
    }
  }

  public static LocalDateTime dateToLocalDateTime(Date date) {
    if (date != null) {
      return date.toInstant().atZone(EVALUATION_ZONE).toLocalDateTime();
    } else {
      return null;
    }
  }

  public static Instant localDateTimeToInstant(LocalDateTime localDateTime) {
    if (localDateTime != null) {
      return localDateTime.atZone(EVALUATION_ZONE).toInstant();
    } else {
      return null;
    }
  }
}
