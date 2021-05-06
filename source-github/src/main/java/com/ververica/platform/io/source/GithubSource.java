package com.ververica.platform.io.source;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import javax.annotation.Nullable;
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

  private static final int DEFAULT_MAX_USERS_IN_CACHE = 10_000;

  protected final String repoName;
  private OkHttpClient okHttpClient;
  protected GitHub gitHub;

  protected final Map<String, GHUser> githubUserCache;

  public GithubSource(String repoName) {
    this(repoName, -1);
  }

  public GithubSource(String repoName, int maxUsersInCache) {
    this.repoName = repoName;
    if (maxUsersInCache < 0) {
      this.githubUserCache = new LRUCache<>(DEFAULT_MAX_USERS_IN_CACHE);
    } else {
      this.githubUserCache = new LRUCache<>(maxUsersInCache);
    }
  }

  /**
   * Fetches user details such as name and email from a separate endpoint (these details are not
   * normally returned from the Github APIs by default). Uses an internal cache for the retrieved
   * objects to reduce the load on the Github APIs.
   *
   * @param maybeShallowUserInfo user object that may contain shallow user info data only
   * @return the user object with all details filled (or <tt>null</tt> if
   *     <tt>maybeShallowUserInfo</tt> was <tt>null</tt>)
   */
  @Nullable
  protected GHUser fillUserDetailsFromCache(@Nullable GHUser maybeShallowUserInfo)
      throws IOException {
    if (maybeShallowUserInfo == null) {
      return null;
    }

    GHUser entry = githubUserCache.get(maybeShallowUserInfo.getLogin());
    if (entry != null) {
      return entry;
    } else {
      // issue a call that triggers fetching user details
      maybeShallowUserInfo.getName();
      githubUserCache.put(maybeShallowUserInfo.getLogin(), maybeShallowUserInfo);
      return maybeShallowUserInfo;
    }
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
