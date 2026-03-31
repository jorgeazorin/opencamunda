/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.client.api;

import java.util.Set;

/**
 * Extended dispatch strategy that can route a request to multiple partitions. This is needed during
 * partition scaling transitions, where a message subscription might live on any of several
 * partitions depending on which routing generation it was created under.
 *
 * <p>Implementations must be thread-safe.
 */
public interface MultiPartitionDispatchStrategy {

  /**
   * Determines all partitions that should receive the request.
   *
   * @return a non-empty set of partition IDs
   * @throws NoTopologyAvailableException if the topology is not available
   */
  Set<Integer> determinePartitions(final BrokerTopologyManager topologyManager);
}
