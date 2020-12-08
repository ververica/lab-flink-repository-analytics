package com.ververica.platform;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public class Utils {

  public static final ZoneId EVALUATION_ZONE = ZoneId.of("UTC");

  public static LocalDateTime dateToLocalDateTime(Date date) {
    if (date != null) {
      return date.toInstant().atZone(EVALUATION_ZONE).toLocalDateTime();
    } else {
      return null;
    }
  }
}
