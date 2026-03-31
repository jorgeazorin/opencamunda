/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.impl.broker;

import io.camunda.zeebe.broker.client.api.BrokerTopologyManager;
import io.camunda.zeebe.broker.client.api.MultiPartitionDispatchStrategy;
import io.camunda.zeebe.broker.client.api.NoTopologyAvailableException;
import io.camunda.zeebe.broker.client.api.RequestDispatchStrategy;
import io.camunda.zeebe.protocol.impl.SubscriptionUtil;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.util.Set;

/**
 * Dispatch strategy for published messages that respects routing generations. During partition
 * scaling transitions, messages are sent to all partitions that any active generation maps the
 * correlation key to.
 */
public final class PublishMessageDispatchStrategy
    implements RequestDispatchStrategy, MultiPartitionDispatchStrategy {

  private final String correlationKey;

  public PublishMessageDispatchStrategy(final String correlationKey) {
    this.correlationKey = correlationKey;
  }

  @Override
  public int determinePartition(final BrokerTopologyManager topologyManager) {
    final var topology = topologyManager.getTopology();
    if (topology == null || topology.getPartitionsCount() == 0) {
      throw new NoTopologyAvailableException(
          String.format(
              "Expected to pick partition for message with correlation key '%s', but no topology is available",
              correlationKey));
    }

    final int partitionsCount = topology.getPartitionsCount();
    return SubscriptionUtil.getSubscriptionPartitionId(
        BufferUtil.wrapString(correlationKey), partitionsCount);
  }

  /**
   * Returns all partitions that should receive this message across active routing generations.
   *
   * <p>If the topology has active routing generations (e.g. after a partition scaling from 10→15),
   * this returns multiple partitions. Otherwise behaves like {@link #determinePartition}.
   */
  @Override
  public Set<Integer> determinePartitions(final BrokerTopologyManager topologyManager) {
    final var topology = topologyManager.getTopology();
    if (topology == null || topology.getPartitionsCount() == 0) {
      throw new NoTopologyAvailableException(
          String.format(
              "Expected to pick partition for message with correlation key '%s', but no topology is available",
              correlationKey));
    }

    final var activePartitionCounts = topology.getActiveRoutingPartitionCounts();
    if (activePartitionCounts == null || activePartitionCounts.isEmpty()) {
      // Fallback: no routing generations info → use current partition count
      return Set.of(
          SubscriptionUtil.getSubscriptionPartitionId(
              BufferUtil.wrapString(correlationKey), topology.getPartitionsCount()));
    }

    return SubscriptionUtil.getSubscriptionPartitionIds(
        BufferUtil.wrapString(correlationKey), activePartitionCounts);
  }
}
