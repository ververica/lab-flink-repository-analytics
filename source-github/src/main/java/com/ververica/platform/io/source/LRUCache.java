package com.ververica.platform.io.source;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Least-recently accessed element cache implementation based on {@link LinkedHashMap}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {

  private final int maxEntries;

  public LRUCache(int maxEntries) {
    super(16, 0.75f, true);
    this.maxEntries = maxEntries;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    return size() >= maxEntries;
  }
}
