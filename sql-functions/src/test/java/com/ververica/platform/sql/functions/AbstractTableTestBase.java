package com.ververica.platform.sql.functions;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/** Base class for unit tests with common functionality. */
public class AbstractTableTestBase {

  /**
   * Executes the statements described by the table and retrieves the results as a list of rows.
   *
   * @param resultTable table to get results for
   */
  static List<Row> getRowsFromTable(TableResult resultTable) throws Exception {
    try (CloseableIterator<Row> rowCloseableIterator = resultTable.collect()) {
      List<Row> results = new ArrayList<>();
      rowCloseableIterator.forEachRemaining(results::add);
      return results;
    }
  }
}
