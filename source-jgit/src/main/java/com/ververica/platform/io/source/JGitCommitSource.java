package com.ververica.platform.io.source;

import com.ververica.platform.Utils;
import com.ververica.platform.entities.Commit;
import com.ververica.platform.entities.FileChanged;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.Edit;
import org.eclipse.jgit.diff.EditList;
import org.eclipse.jgit.diff.RawTextComparator;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevSort;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.util.io.DisabledOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads commits from a Git repository using JGit and extracts metadata.
 *
 * <p>See https://github.com/centic9/jgit-cookbook for a couple of examples
 */
public class JGitCommitSource extends RichSourceFunction<Commit> implements CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(JGitCommitSource.class);

  private static final int LOG_EVERY = 100;

  private final String repoUrl;
  private final String repoBranch;
  @Nullable private final String ignoreCommitsBefore;

  private final long pollIntervalMillis;

  private volatile boolean running = true;

  private String lastCommitHash;
  private transient ListState<String> state;
  private transient Path gitRepoDir = null;

  /**
   * @param repoUrl full URL to the git repository
   * @param repoBranch branch to clone; must be specified as a full ref name (e.g. refs/heads/master
   *     or refs/tags/v1.0.0).
   * @param ignoreCommitsBefore commit hash to mark the start of the import (exclusive); or
   *     <tt>null</tt> to start from earliest
   * @param pollIntervalMillis time to wait after walking the repository and before pulling it again
   */
  public JGitCommitSource(
      String repoUrl,
      String repoBranch,
      @Nullable String ignoreCommitsBefore,
      long pollIntervalMillis) {
    this.repoUrl = repoUrl;
    this.repoBranch = repoBranch;
    this.ignoreCommitsBefore = ignoreCommitsBefore;
    this.pollIntervalMillis = pollIntervalMillis;
  }

  @Override
  public void run(SourceContext<Commit> ctx) throws GitAPIException, IOException {
    try (Git gitRepo = createGitRepo()) {
      while (running) {
        Repository repo = gitRepo.getRepository();
        try (RevWalk revWalk = new RevWalk(repo)) {
          revWalk.sort(RevSort.REVERSE);
          final String start;
          if (lastCommitHash != null) {
            // do not read commits already seen
            RevCommit commit = revWalk.parseCommit(repo.resolve(lastCommitHash));
            start = commit.getName();
            revWalk.markUninteresting(commit);
          } else if (ignoreCommitsBefore != null && !ignoreCommitsBefore.isEmpty()) {
            // do not read commits including and before the provided one
            RevCommit commit = revWalk.parseCommit(repo.resolve(ignoreCommitsBefore));
            start = commit.getName();
            revWalk.markUninteresting(commit);
          } else {
            start = "<root>";
          }
          // read until head of branch
          final RevCommit headCommit = revWalk.parseCommit(repo.exactRef(repoBranch).getObjectId());
          revWalk.markStart(headCommit);
          int count = 0;
          for (RevCommit gitCommit : revWalk) {

            PersonIdent author = gitCommit.getAuthorIdent();
            PersonIdent committer = gitCommit.getCommitterIdent();

            Commit myCommit =
                Commit.builder()
                    .author(author.getName())
                    .authorEmail(author.getEmailAddress())
                    .committer(committer.getName())
                    .committerEmail(committer.getEmailAddress())
                    .filesChanged(getFileChanges(repo, gitCommit))
                    .commitDate(Utils.dateToLocalDateTime(gitCommit.getCommitTime() * 1000L))
                    .authorDate(Utils.dateToLocalDateTime(author.getWhen()))
                    .shortInfo(gitCommit.getFullMessage())
                    .sha1(gitCommit.getName())
                    .build();

            synchronized (ctx.getCheckpointLock()) {
              ctx.collectWithTimestamp(myCommit, gitCommit.getCommitTime());
              lastCommitHash = myCommit.getSha1();
            }

            count++;

            if (count % LOG_EVERY == 0) {
              LOG.trace("Finished parsing {}", gitCommit);
            }

            if (!running) {
              break;
            }
          }
          LOG.info("Found {} new commits in range ({}, {}]", count, start, headCommit.getName());
        }

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
        // refresh repo after waiting!
        if (running) {
          gitRepo.pull().call();
        }
      }
    }
  }

  private FileChanged[] getFileChanges(Repository repo, RevCommit commit) throws IOException {
    // handle root commit (with no parent) differently!
    final RevTree parentTree;
    if (commit.getParentCount() == 0) {
      parentTree = null;
    } else {
      parentTree = commit.getParent(0).getTree();
    }

    ArrayList<FileChanged> filesChanged = new ArrayList<>();

    try (DiffFormatter diffFormatter = new DiffFormatter(DisabledOutputStream.INSTANCE)) {
      diffFormatter.setRepository(repo);
      diffFormatter.setDiffComparator(RawTextComparator.DEFAULT);
      // diffFormatter.setDetectRenames(false);
      diffFormatter.setContext(0);

      List<DiffEntry> diffs = diffFormatter.scan(parentTree, commit.getTree());
      for (DiffEntry diff : diffs) {
        final String filename;
        if (diff.getChangeType() == DiffEntry.ChangeType.ADD) {
          filename = diff.getNewPath();
        } else {
          filename = diff.getOldPath();
        }
        EditList edits = diffFormatter.toFileHeader(diff).toEditList();
        int linesAdded = 0;
        int linesRemoved = 0;
        for (Edit edit : edits) {
          if (edit.getType() == Edit.Type.DELETE) {
            linesRemoved += edit.getLengthA();
            assert 0 == edit.getLengthB();
          } else if (edit.getType() == Edit.Type.INSERT) {
            linesAdded += edit.getLengthB();
            assert 0 == edit.getLengthA();
          } else if (edit.getType() == Edit.Type.REPLACE) {
            linesRemoved += edit.getLengthA();
            linesAdded += edit.getLengthB();
          }
        }
        filesChanged.add(
            FileChanged.builder()
                .filename(filename)
                .linesChanged(linesAdded + linesRemoved)
                .linesAdded(linesAdded)
                .linesRemoved(linesRemoved)
                .build());
      }
    }
    return filesChanged.toArray(new FileChanged[0]);
  }

  private Git createGitRepo() throws GitAPIException {
    LOG.info("Cloning repository {} with branch {} at {}.", repoUrl, repoBranch, gitRepoDir);
    Git git =
        Git.cloneRepository()
            .setURI(repoUrl)
            .setDirectory(gitRepoDir.toFile())
            .setBranchesToClone(Collections.singletonList(repoBranch))
            .setBranch(repoBranch)
            .call();
    LOG.info("Finished cloning {}", git);
    return git;
  }

  @Override
  public void cancel() {
    running = false;
  }

  @Override
  public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
    state.clear();
    state.add(lastCommitHash);
  }

  @Override
  public void initializeState(FunctionInitializationContext ctx) throws Exception {
    state =
        ctx.getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("instant", Types.STRING));

    if (ctx.isRestored()) {
      Iterator<String> data = state.get().iterator();
      if (data.hasNext()) {
        lastCommitHash = data.next();
      }
    }
  }

  @Override
  public void open(Configuration configuration) throws IOException, URISyntaxException {
    gitRepoDir =
        Files.createTempDirectory(
            "jgit-"
                + Paths.get(new URI(repoUrl).getPath()).getFileName().toString()
                + "_"
                + Paths.get(repoBranch).getFileName());
  }

  @Override
  public void close() throws IOException {
    if (gitRepoDir != null) {
      //noinspection ResultOfMethodCallIgnored
      Files.walk(gitRepoDir)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
      if (gitRepoDir.toFile().exists()) {
        throw new IOException("Failed to delete directory " + gitRepoDir);
      }
    }
  }
}
