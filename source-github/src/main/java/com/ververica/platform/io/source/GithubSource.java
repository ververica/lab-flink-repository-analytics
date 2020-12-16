package com.ververica.platform.io.source;

import java.io.IOException;
import java.nio.file.Files;
import okhttp3.Cache;
import okhttp3.OkHttpClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.kohsuke.github.GHUser;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.kohsuke.github.RateLimitChecker;
import org.kohsuke.github.extras.okhttp3.OkHttpConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for sources reading from the Github API
 *
 * @param <T> The type of the records produced by this source.
 */
public abstract class GithubSource<T> extends RichSourceFunction<T> {

  private static final Logger LOG = LoggerFactory.getLogger(GithubSource.class);

  protected final String repoName;
  private OkHttpClient okHttpClient;
  protected GitHub gitHub;

  public GithubSource(String repoName) {
    this.repoName = repoName;
  }

  protected static String getUserName(GHUser user) throws IOException {
    if (user == null) {
      return "unknown";
    } else if (user.getName() == null) {
      return user.getLogin() == null ? "unknown" : user.getLogin();
    } else {
      return user.getName();
    }
  }

  @Override
  public void open(Configuration configuration) throws IOException {
    okHttpClient = setupOkHttpClient();
    LOG.info("Setting up GitHub client.");
    gitHub = createGitHub(okHttpClient);
  }

  @Override
  public void close() throws IOException {
    closeOkHttpClient(okHttpClient);
  }

  private static GitHub createGitHub(OkHttpClient okHttpClient) throws IOException {
    return GitHubBuilder.fromEnvironment()
        .withConnector(new OkHttpConnector(okHttpClient))
        .withRateLimitChecker(new RateLimitChecker.LiteralValue(1))
        .build();
  }

  private static OkHttpClient setupOkHttpClient() throws IOException {
    Cache cache = new Cache(Files.createTempDirectory("flink-service").toFile(), 4 * 1024 * 1024);
    LOG.info("Setting up OkHttp client with cache at {}.", cache.directory());
    final OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder();
    okHttpClient.cache(cache);
    return okHttpClient.build();
  }

  private static void closeOkHttpClient(OkHttpClient okHttpClient) throws IOException {
    okHttpClient.dispatcher().executorService().shutdown();
    okHttpClient.connectionPool().evictAll();
    Cache cache = okHttpClient.cache();
    if (cache != null) {
      cache.close();
    }
  }
}
