package com.ververica.platform.io.source;

import com.ververica.platform.Utils;
import com.ververica.platform.entities.PullRequest;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.kohsuke.github.GHDirection;
import org.kohsuke.github.GHIssueState;
import org.kohsuke.github.GHLabel;
import org.kohsuke.github.GHPullRequest;
import org.kohsuke.github.GHPullRequestQueryBuilder;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHUser;
import org.kohsuke.github.PagedIterable;
import org.kohsuke.github.PagedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reads pull requests issued to a Github repository using the Github API and extracts metadata. */
public class GithubPullRequestSource extends GithubSource<PullRequest>
    implements CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(GithubPullRequestSource.class);

  private static final int PAGE_SIZE = 100;

  private final long pollIntervalMillis;

  private Instant lastTime;

  private volatile boolean running = true;

  private transient ListState<Instant> state;

  private transient Field ghPRMergeCommitField;

  public GithubPullRequestSource(String repoName, Instant startTime, long pollIntervalMillis) {
    super(repoName);
    this.lastTime = startTime;
    this.pollIntervalMillis = pollIntervalMillis;
  }

  @Override
  public void run(SourceContext<PullRequest> ctx) throws IOException, IllegalAccessException {
    GHRepository repo = gitHub.getRepository(repoName);
    while (running) {
      LOG.debug("Fetching pull requests since {}", lastTime);
      PagedIterable<GHPullRequest> pullRequests =
          repo.queryPullRequests()
              .base("master")
              .state(GHIssueState.ALL)
              .sort(GHPullRequestQueryBuilder.Sort.CREATED)
              .direction(GHDirection.ASC)
              .list();

      Date maxDate = Date.from(lastTime);

      PagedIterator<GHPullRequest> it = pullRequests.withPageSize(PAGE_SIZE).iterator();
      long prCount = 0;
      while (it.hasNext() && running) {
        GHPullRequest pr = it.next();
        Date createdAt = pr.getCreatedAt();
        if (createdAt.after(maxDate)) {
          PullRequest myPr = fromGHPullRequest(pr);
          ++prCount;

          synchronized (ctx.getCheckpointLock()) {
            ctx.collectWithTimestamp(myPr, createdAt.getTime());
            lastTime = createdAt.toInstant();
            maxDate = createdAt;

            if (prCount % PAGE_SIZE == 0) {
              ctx.emitWatermark(new Watermark(createdAt.getTime()));
            }
          }
          if (prCount % PAGE_SIZE == 0 || !it.hasNext()) {
            LOG.debug(
                "Fetched pull requests: {} (last PR: {}), rate limits: {}",
                prCount,
                myPr.getNumber(),
                gitHub.getRateLimit());
          }
        }
      }

      // If end of PR list is reached, delay further polls for data
      if (pollIntervalMillis > 0) {
        try {
          //noinspection BusyWait
          Thread.sleep(pollIntervalMillis);
        } catch (InterruptedException e) {
          running = false;
        }
      } else if (pollIntervalMillis < 0) {
        cancel();
      }
    }
  }

  private PullRequest fromGHPullRequest(GHPullRequest ghPullRequest)
      throws IOException, IllegalAccessException {
    GHUser creator = fillUserDetailsFromCache(ghPullRequest.getUser());
    String creatorEmail = creator != null ? creator.getEmail() : null;

    return PullRequest.builder()
        .number(ghPullRequest.getNumber())
        .state(ghPullRequest.getState().toString())
        .title(ghPullRequest.getTitle())
        .description(ghPullRequest.getBody())
        .creator(getUserName(creator))
        .creatorEmail(creatorEmail)
        .labels(ghPullRequest.getLabels().stream().map(GHLabel::getName).toArray(String[]::new))
        .createdAt(Utils.dateToLocalDateTime(ghPullRequest.getCreatedAt()))
        .updatedAt(Utils.dateToLocalDateTime(ghPullRequest.getUpdatedAt()))
        .closedAt(Utils.dateToLocalDateTime(ghPullRequest.getClosedAt()))
        .mergedAt(Utils.dateToLocalDateTime(ghPullRequest.getMergedAt()))
        .mergeCommit(
            ghPullRequest.getMergedAt() == null
                ? null
                : (String) ghPRMergeCommitField.get(ghPullRequest))
        .commentsCount(ghPullRequest.getCommentsCount())
        .build();
  }

  @Override
  public void open(Configuration configuration) throws Exception {
    super.open(configuration);

    // hack to apply https://github.com/hub4j/github-api/pull/1136
    ghPRMergeCommitField = GHPullRequest.class.getDeclaredField("merge_commit_sha");
    ghPRMergeCommitField.setAccessible(true);
  }

  @Override
  public void cancel() {
    running = false;
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
