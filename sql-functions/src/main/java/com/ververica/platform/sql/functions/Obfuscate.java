package com.ververica.platform.sql.functions;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Obfuscates names and emails (only the part before the '@') by replying them with their short md5
 * hash (6 characters).
 */
@SuppressWarnings("unused")
public class Obfuscate extends ScalarFunction {

  private transient MessageDigest md5;

  @Override
  public void open(FunctionContext context) throws Exception {
    this.md5 = MessageDigest.getInstance("MD5");
  }

  private String md5Short(String input) {
    md5.update(input.getBytes());
    return String.format("%032x", new BigInteger(1, md5.digest())).substring(0, 6);
  }

  private String obfuscateEmailAddress(String emailAddress) {
    if (emailAddress != null) {
      String[] components = emailAddress.split("@", 2);
      if (components.length == 2) {
        return md5Short(components[0]) + "@" + components[1];
      } else if (components.length == 1) {
        return md5Short(components[0]);
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  public String eval(String emailAddress) {
    return obfuscateEmailAddress(emailAddress);
  }

  public List<String> eval(List<String> emailAddresss) {
    return emailAddresss.stream().map(this::obfuscateEmailAddress).collect(Collectors.toList());
  }
}
