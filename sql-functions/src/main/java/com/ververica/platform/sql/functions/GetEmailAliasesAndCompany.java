package com.ververica.platform.sql.functions;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * Aggregate function <tt>GetEmailAliasesAndCompany(email, dateSeen)</tt> for finding email aliases
 * and company associations based on the following strategy:
 *
 * <ul>
 *   <li>collect all email aliases seen so far
 *   <li>the most recent non-excluded domain (see {@link #COMPANY_EXCLUDES}) will become the
 *       associated company
 * </ul>
 */
@SuppressWarnings("unused")
@FunctionHint(
    input = {
      @DataTypeHint(value = "STRING", bridgedTo = StringData.class),
      @DataTypeHint(value = "TIMESTAMP(3)", bridgedTo = TimestampData.class)
    },
    output =
        @DataTypeHint(
            value = "ROW<aliases ARRAY<STRING>, company STRING, companySince TIMESTAMP(3)>",
            bridgedTo = RowData.class))
public class GetEmailAliasesAndCompany
    extends AggregateFunction<RowData, GetEmailAliasesAndCompany.MyAccumulator> {

  /** Domains that are not interpreted as companies, e.g. email providers. */
  private static final Set<String> COMPANY_EXCLUDES = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

  static {
    COMPANY_EXCLUDES.addAll(
        Arrays.asList(
            "126.com",
            "163.com",
            "apache.org",
            "gmail.com",
            "gmx.de",
            "gmx.net",
            "gmx.org",
            "googlemail.com",
            "hotmail.com",
            "hotmail.de",
            "hotmail.it",
            "icloud.com",
            "live.com",
            "live.it",
            "mail.ru",
            "mailbox.org",
            "msn.com",
            "outlook.com",
            "outlook.de",
            "pobox.com",
            "posteo.de",
            "web.de",
            "yahoo.com",
            "yahoo.in",
            "qq.com"));
  }

  private static final Pattern VERVERICA_ALIASES =
      Pattern.compile("(data-artisans|da-platform).com");

  public static class MyAccumulator {
    @DataTypeHint(value = "MAP< STRING, BOOLEAN >", bridgedTo = MapData.class)
    public MapView<StringData, Boolean> aliasMap = new MapView<>();

    public long count = 0L;
    public String company;

    @DataTypeHint("TIMESTAMP(3)")
    public TimestampData companySince;
  }

  @Override
  public MyAccumulator createAccumulator() {
    return new MyAccumulator();
  }

  public void accumulate(MyAccumulator acc, StringData emailAddress, TimestampData date)
      throws Exception {
    if (emailAddress != null) {
      acc.aliasMap.put(emailAddress, true);
      acc.count++;

      String stringVal = emailAddress.toString();
      int idx = stringVal.lastIndexOf('@') + 1;
      if (idx > 0 && idx < stringVal.length()) {
        String company = stringVal.substring(idx);
        if (!COMPANY_EXCLUDES.contains(company) && !company.equals(acc.company)) {
          acc.company = company;
          acc.companySince = date;
        }
      }
    }
  }

  @Override
  public RowData getValue(MyAccumulator acc) {
    String company =
        acc.company == null
            ? null
            : VERVERICA_ALIASES.matcher(acc.company).replaceAll("ververica.com");
    return GenericRowData.of(
        new GenericArrayData(acc.aliasMap.getMap().keySet().toArray(new StringData[0])),
        StringData.fromString(company),
        acc.companySince);
  }
}
