package com.ververica.platform.operators;

import static com.ververica.platform.PatternUtils.SOURCE_FILENAME_COMPONENT_PATTERN;

import com.ververica.platform.entities.Commit;
import com.ververica.platform.entities.ComponentChanged;
import com.ververica.platform.entities.FileChanged;
import java.util.regex.Matcher;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComponentExtractor extends RichFlatMapFunction<Commit, ComponentChanged>
    implements CheckpointListener {

  private static final Logger LOG = LoggerFactory.getLogger(ComponentExtractor.class);

  private final boolean featureFlag;

  private transient Counter componentNotFoundCounter;

  public ComponentExtractor(boolean featureFlag) {
    this.featureFlag = featureFlag;
  }

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    componentNotFoundCounter = getRuntimeContext().getMetricGroup().counter("component-not-found");
  }

  @Override
  public void flatMap(Commit value, Collector<ComponentChanged> out) throws Exception {
    for (FileChanged file : value.getFilesChanged()) {
      Matcher matcher = SOURCE_FILENAME_COMPONENT_PATTERN.matcher(file.getFilename());

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
  public void notifyCheckpointComplete(long l) throws Exception {
    if (featureFlag) {
      throw new RuntimeException("Something has gone terribly wrong");
    }
  }
}
