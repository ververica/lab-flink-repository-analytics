package com.ververica.platform.operators;

import com.ververica.platform.entities.Commit;
import com.ververica.platform.entities.ComponentChanged;
import com.ververica.platform.entities.FileChanged;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComponentExtractor extends RichFlatMapFunction<Commit, ComponentChanged>
    implements CheckpointedFunction, CheckpointListener {

  private static final Logger LOG = LoggerFactory.getLogger(ComponentExtractor.class);

  private static final Pattern COMPONENT = Pattern.compile("(?<component>.*)\\/src\\/.*");

  private final boolean featureFlag;

  private transient Counter componentNotFoundCounter;

  private transient boolean hasRestored;

  private transient int numCompletedCheckpoints;

  public ComponentExtractor(boolean featureFlag) {
    this.featureFlag = featureFlag;
  }

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    componentNotFoundCounter = getRuntimeContext().getMetricGroup().counter("component-not-found");
    numCompletedCheckpoints = 0;
  }

  @Override
  public void flatMap(Commit value, Collector<ComponentChanged> out) throws Exception {
    for (FileChanged file : value.getFilesChanged()) {
      Matcher matcher = COMPONENT.matcher(file.getFilename());

      if (!matcher.matches()) {
        LOG.trace("No component found for file {}", file.getFilename());
        componentNotFoundCounter.inc();
        return;
      }

      String componentName = matcher.group("component");

      ComponentChanged component =
          ComponentChanged.builder()
              .name(componentName)
              .linesChanged(file.getLinesChanged())
              .build();

      out.collect(component);
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) {}

  @Override
  public void initializeState(FunctionInitializationContext functionInitializationContext) {
    hasRestored = functionInitializationContext.isRestored();
  }

  @Override
  public void notifyCheckpointComplete(long l) throws Exception {
    numCompletedCheckpoints++;

    if (featureFlag && (hasRestored || numCompletedCheckpoints > 1)) {
      throw new RuntimeException("Something has gone terribly wrong");
    }
  }
}
