/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib;

/**
 * A mock implementation of MetricClient. Useful for users who do not have any metric client set up
 * but still want to use the functionality of airbyte, or in a unit test where user calls the
 * testing function but did not initialize the metric client in the first place.
 */
public class NotImplementedMetricClient implements MetricClient {

  @Override
  public void count(MetricsRegistry metric, long val, String... tags) {
    // Not Implemented.
  }

  @Override
  public void gauge(MetricsRegistry metric, double val, String... tags) {
    // Not Implemented.
  }

  @Override
  public void distribution(MetricsRegistry metric, double val, String... tags) {
    // Not Implemented.
  }

  @Override
  public void shutdown() {
    // Not Implemented.
  }

}
