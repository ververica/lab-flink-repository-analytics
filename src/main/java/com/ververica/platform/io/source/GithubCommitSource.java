package com.ververica.platform.io.source;

import com.ververica.platform.entities.Commit;
import com.ververica.platform.entities.FileChanged;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHUser;
import org.kohsuke.github.PagedIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GithubCommitSource extends GithubSource<Commit> implements CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(GithubCommitSource.class);

  private static final int PAGE_SIZE = 100;

  private final long pollIntervalMillis;

  private Instant lastTime;

  private volatile boolean running = true;

  private transient ListState<Instant> state;

  public GithubCommitSource(String repoName) {
    this(repoName, Instant.now(), 1000);
  }

  public GithubCommitSource(String repoName, Instant startTime, long pollIntervalMillis) {
    super(repoName);
    this.lastTime = startTime;
    this.pollIntervalMillis = pollIntervalMillis;
  }

  @Override
  public void run(SourceContext<Commit> ctx) throws IOException {
    GHRepository repo = gitHub.getRepository(repoName);
    while (running) {
      Instant until = getUntilFor(lastTime);
      LOG.debug("Fetching commits since {} until {}", lastTime, until);
      PagedIterable<GHCommit> commits =
          repo.queryCommits().since(Date.from(lastTime)).until(Date.from(until)).list();

      List<Commit> changes =
          StreamSupport.stream(commits.withPageSize(PAGE_SIZE).spliterator(), false)
              .map(GithubCommitSource::fromGHCommit)
              .collect(Collectors.toList());

      synchronized (ctx.getCheckpointLock()) {
        for (Commit commit : changes) {
          ctx.collectWithTimestamp(commit, commit.getTimestamp().getTime());
        }

        lastTime = until;
        ctx.emitWatermark(new Watermark(lastTime.toEpochMilli()));
      }

      if (pollIntervalMillis > 0) {
        try {
          //noinspection BusyWait
          Thread.sleep(pollIntervalMillis);
        } catch (InterruptedException e) {
          running = false;
        }
      }
    }
  }

  private static Commit fromGHCommit(GHCommit ghCommit) {
    try {
      Date lastCommitDate = ghCommit.getCommitDate();
      GHUser author = ghCommit.getAuthor();

      return Commit.builder()
          .author(getUserName(author))
          .filesChanged(
              ghCommit.getFiles().stream()
                  .map(
                      file ->
                          FileChanged.builder()
                              .filename(file.getFileName())
                              .linesChanged(file.getLinesChanged())
                              .build())
                  .collect(Collectors.toList()))
          .timestamp(lastCommitDate)
          .build();
    } catch (IOException e) {
      throw new RuntimeException("Failed to pull commit from GH", e);
    }
  }

  private static String getUserName(GHUser user) throws IOException {
    if (user == null) {
      return "unknown";
    } else if (user.getName() == null) {
      return user.getLogin() == null ? "unknown" : user.getLogin();
    } else {
      return user.getName();
    }
  }

  @Override
  public void cancel() {
    running = false;
  }

  public static Instant getUntilFor(Instant since) {
    Instant maybeUntil = since.plus(1, ChronoUnit.HOURS);

    if (maybeUntil.compareTo(Instant.now()) > 0) {
      return Instant.now();
    } else {
      return maybeUntil;
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
    state.clear();
    state.add(lastTime);
  }

  @Override
  public void initializeState(FunctionInitializationContext ctx) throws Exception {
    state =
        ctx.getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("instant", Instant.class));

    if (ctx.isRestored()) {
      Iterator<Instant> data = state.get().iterator();
      if (data.hasNext()) {
        lastTime = data.next();
      }
    }
  }
}
