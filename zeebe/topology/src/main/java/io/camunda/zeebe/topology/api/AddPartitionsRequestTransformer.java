/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.topology.api;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.partition.PartitionId;
import io.camunda.zeebe.topology.api.TopologyRequestFailedException.InvalidRequest;
import io.camunda.zeebe.topology.changes.TopologyChangeCoordinator.TopologyChangeRequest;
import io.camunda.zeebe.topology.state.ClusterTopology;
import io.camunda.zeebe.topology.state.TopologyChangeOperation;
import io.camunda.zeebe.topology.state.TopologyChangeOperation.PartitionChangeOperation.PartitionBootstrapOperation;
import io.camunda.zeebe.topology.state.TopologyChangeOperation.PartitionChangeOperation.PartitionJoinOperation;
import io.camunda.zeebe.topology.util.RoundRobinPartitionDistributor;
import io.camunda.zeebe.util.Either;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms an {@link io.camunda.zeebe.topology.api.TopologyManagementRequest.AddPartitionsRequest}
 * into a sequence of {@link PartitionBootstrapOperation} operations.
 *
 * <p>New partitions get IDs starting from currentPartitionCount + 1, and are distributed across the
 * provided members using the round-robin algorithm.
 */
final class AddPartitionsRequestTransformer implements TopologyChangeRequest {

  private static final Logger LOG = LoggerFactory.getLogger(AddPartitionsRequestTransformer.class);

  private final int newPartitionCount;
  private final Set<MemberId> members;
  private final int replicationFactor;

  AddPartitionsRequestTransformer(
      final int newPartitionCount, final Set<MemberId> members, final int replicationFactor) {
    this.newPartitionCount = newPartitionCount;
    this.members = members;
    this.replicationFactor = replicationFactor;
  }

  @Override
  public Either<Exception, List<TopologyChangeOperation>> operations(
      final ClusterTopology currentTopology) {

    final int currentPartitionCount = currentTopology.partitionCount();

    if (newPartitionCount <= currentPartitionCount) {
      return Either.left(
          new InvalidRequest(
              String.format(
                  "New partition count [%d] must be greater than current [%d]",
                  newPartitionCount, currentPartitionCount)));
    }

    if (members.isEmpty()) {
      return Either.left(
          new InvalidRequest("Cannot add partitions if no brokers are provided"));
    }

    if (replicationFactor <= 0) {
      return Either.left(
          new InvalidRequest(
              String.format("Replication factor [%d] must be greater than 0", replicationFactor)));
    }

    if (members.size() < replicationFactor) {
      return Either.left(
          new InvalidRequest(
              String.format(
                  "Number of brokers [%d] is less than the replication factor [%d]",
                  members.size(), replicationFactor)));
    }

    // Generate partition IDs for the new partitions only
    final int newPartitionsToAdd = newPartitionCount - currentPartitionCount;
    final var newPartitionIds =
        IntStream.rangeClosed(currentPartitionCount + 1, newPartitionCount)
            .mapToObj(i -> PartitionId.from("raft-partition", i))
            .sorted()
            .toList();

    // Use round-robin to distribute only the NEW partitions across the members
    final var distributor = new RoundRobinPartitionDistributor();
    final var distribution =
        distributor.distributePartitions(members, newPartitionIds, replicationFactor);

    // Convert distribution into PartitionBootstrapOperation operations
    final List<TopologyChangeOperation> operations = new ArrayList<>();

    for (final var partitionMetadata : distribution) {
      final int partitionId = partitionMetadata.id().id();
      final Map<MemberId, Integer> priorities = partitionMetadata.priorities();

      // Sort members by priority descending — highest priority member is the primary.
      // The primary bootstraps the Raft group from scratch. All subsequent members JOIN
      // via PartitionJoinOperation (they replicate from the bootstrapped primary).
      final var sortedMembers =
          priorities.entrySet().stream()
              .sorted(Comparator.comparingInt(e -> -e.getValue()))
              .toList();

      boolean isFirst = true;
      for (final var entry : sortedMembers) {
        if (isFirst) {
          // Primary: bootstrap the Raft group from scratch
          operations.add(
              new PartitionBootstrapOperation(
                  entry.getKey(), partitionId, entry.getValue(), priorities));
          isFirst = false;
        } else {
          // Secondary: join the existing Raft group (replicate from primary)
          operations.add(
              new PartitionJoinOperation(entry.getKey(), partitionId, entry.getValue()));
        }
      }
    }

    return Either.right(operations);
  }

  @Override
  public UnaryOperator<ClusterTopology> preApplyTransformer() {
    return topology -> {
      final int currentPartitionCount = topology.partitionCount();
      LOG.info(
          "Starting partition scaling transition: {} -> {} partitions. "
              + "A new routing generation will be created. Messages will be fanned out to all "
              + "active generations until old generations are retired.",
          currentPartitionCount,
          newPartitionCount);
      final var initialized = topology.initializeMessageRouting(currentPartitionCount);
      final var updated = initialized.addRoutingGeneration(newPartitionCount);
      LOG.info(
          "Routing generation added. Active generations: {}",
          updated.messageRoutingState().activeGenerations());
      return updated;
    };
  }

  @Override
  public boolean isForced() {
    return false;
  }
}
