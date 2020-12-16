package com.ververica.platform.io.source;

import com.ververica.platform.Utils;
import com.ververica.platform.entities.Commit;
import com.ververica.platform.entities.FileChanged;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
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

/** Reads commits from a Github repository using the Github API and extracts metadata. */
public class GithubCommitSource extends GithubSource<Commit> implements CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(GithubCommitSource.class);

  private static final int PAGE_SIZE = 100;

  private final long pollIntervalMillis;

  private Instant lastTime;

  private volatile boolean running = true;

  private transient ListState<Instant> state;

  public GithubCommitSource(String repoName, Instant startTime, long pollIntervalMillis) {
    super(repoName);
    this.lastTime = startTime;
    this.pollIntervalMillis = pollIntervalMillis;
  }

  @Override
  public void run(SourceContext<Commit> ctx) throws IOException {
    boolean delayNextPoll;
    GHRepository repo = gitHub.getRepository(repoName);
    while (running) {
      Tuple2<Instant, Boolean> untilTuple = getUntilFor(lastTime);
      Instant until = untilTuple.f0;
      delayNextPoll = untilTuple.f1;
      LOG.debug("Fetching commits since {} until {}", lastTime, until);
      PagedIterable<GHCommit> commits =
          repo.queryCommits().since(Date.from(lastTime)).until(Date.from(until)).list();

      List<Tuple2<Commit, Date>> changes =
          StreamSupport.stream(commits.withPageSize(PAGE_SIZE).spliterator(), false)
              .map(GithubCommitSource::fromGHCommit)
              .collect(Collectors.toList());

      synchronized (ctx.getCheckpointLock()) {
        for (Tuple2<Commit, Date> commit : changes) {
          ctx.collectWithTimestamp(commit.f0, commit.f1.getTime());
        }

        lastTime = until;
        ctx.emitWatermark(new Watermark(lastTime.toEpochMilli()));
      }

      if (delayNextPoll && pollIntervalMillis > 0) {
        try {
          //noinspection BusyWait
          Thread.sleep(pollIntervalMillis);
        } catch (InterruptedException e) {
          running = false;
        }
      }
    }
  }

  private static Tuple2<Commit, Date> fromGHCommit(GHCommit ghCommit) {
    try {
      LocalDateTime lastCommitDate = Utils.dateToLocalDateTime(ghCommit.getCommitDate());
      LocalDateTime lastCommitAuthorDate = Utils.dateToLocalDateTime(ghCommit.getAuthoredDate());
      GHUser author = ghCommit.getAuthor();
      GHUser committer = ghCommit.getCommitter();
      String authorEmail = author != null ? author.getEmail() : null;
      String committerEmail = committer != null ? committer.getEmail() : null;
      String shortInfo = ghCommit.getCommitShortInfo().getMessage();
      String sha1 = ghCommit.getSHA1();

      return Tuple2.of(
          Commit.builder()
              .author(getUserName(author))
              .authorEmail(authorEmail)
              .committer(getUserName(committer))
              .committerEmail(committerEmail)
              .filesChanged(
                  ghCommit.getFiles().stream()
                      .map(
                          file ->
                              FileChanged.builder()
                                  .filename(file.getFileName())
                                  .linesChanged(file.getLinesChanged())
                                  .linesAdded(file.getLinesAdded())
                                  .linesRemoved(file.getLinesDeleted())
                                  .build())
                      .toArray(FileChanged[]::new))
              .commitDate(lastCommitDate)
              .authorDate(lastCommitAuthorDate)
              .shortInfo(shortInfo)
              .sha1(sha1)
              .build(),
          ghCommit.getCommitDate());
    } catch (IOException e) {
      throw new RuntimeException("Failed to pull commit from GH", e);
    }
  }

  @Override
  public void cancel() {
    running = false;
  }

  public static Tuple2<Instant, /* delay next? */ Boolean> getUntilFor(Instant since) {
    Instant maybeUntil = since.plus(1, ChronoUnit.DAYS);

    Instant now = Instant.now();
    if (maybeUntil.compareTo(now) > 0) {
      return Tuple2.of(now, true);
    } else {
      return Tuple2.of(maybeUntil, false);
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
