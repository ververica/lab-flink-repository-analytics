package com.ververica.platform.sql.functions;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.ClassRule;

/** Base class for unit tests with common functionality. */
public class AbstractTableTestBase {
  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberSlotsPerTaskManager(4)
              .setNumberTaskManagers(1)
              .build());

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

  /**
   * Executes the DataStream job and retrieves the results as a list of rows.
   *
   * @param resultStream stream to execute
   */
  static List<Row> getRowsFromDataStream(DataStream<Row> resultStream) throws Exception {
    try (CloseableIterator<Row> rowCloseableIterator = resultStream.executeAndCollect()) {
      List<Row> results = new ArrayList<>();
      rowCloseableIterator.forEachRemaining(results::add);
      return results;
    }
  }
}
