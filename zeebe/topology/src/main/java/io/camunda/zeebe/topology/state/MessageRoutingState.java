/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.topology.state;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Tracks routing generations for message correlation stability during partition scaling.
 *
 * <p>Each time new partitions are added to the cluster, a new {@link RoutingGeneration} is created
 * that records the partition count at that point. Existing message subscriptions continue to use the
 * partition count from the generation under which they were created, so their routing remains
 * stable.
 *
 * <p>A generation can be retired (removed) once all message subscriptions created under it have been
 * fulfilled, cancelled, or their process instances have completed. This is tracked externally by the
 * engine and signalled via the topology API.
 *
 * <p><b>Multiple scaling example (10 → 15 → 30):</b>
 *
 * <pre>
 * Generation 1: partitionCount=10  (initial cluster)
 * Generation 2: partitionCount=15  (first scale-up)
 * Generation 3: partitionCount=30  (second scale-up)  ← current/active
 *
 * When a message arrives with correlationKey "order-123":
 *   - Gen 1 target: hash % 10 + 1 = partition 4
 *   - Gen 2 target: hash % 15 + 1 = partition 9
 *   - Gen 3 target: hash % 30 + 1 = partition 24
 *   → Message is sent to partitions {4, 9, 24} (deduplicated)
 *
 * Once all Gen 1 subscriptions complete → Gen 1 is retired:
 *   → Message is sent to partitions {9, 24} only
 * </pre>
 *
 * <p>A generation is "complete" when there are zero open message subscriptions that were created
 * with that generation's partition count. The engine tracks this per-partition and notifies the
 * coordinator to retire old generations.
 *
 * @param generations ordered list of routing generations (oldest first, last = current)
 */
public record MessageRoutingState(List<RoutingGeneration> generations) {

  public static MessageRoutingState init(final int initialPartitionCount) {
    return new MessageRoutingState(
        List.of(new RoutingGeneration(1, initialPartitionCount, false)));
  }

  public static MessageRoutingState uninitialized() {
    return new MessageRoutingState(List.of());
  }

  /** Returns the current (latest) routing generation, used for new subscriptions. */
  public RoutingGeneration currentGeneration() {
    if (generations.isEmpty()) {
      throw new IllegalStateException("No routing generations available");
    }
    return generations.get(generations.size() - 1);
  }

  /** Returns the current partition count (from the latest generation). */
  public int currentPartitionCount() {
    return currentGeneration().partitionCount();
  }

  /**
   * Returns all active (non-retired) generations. These are the generations that still have or
   * might have open message subscriptions.
   */
  public List<RoutingGeneration> activeGenerations() {
    return generations.stream().filter(g -> !g.retired()).toList();
  }

  /**
   * Returns the set of all distinct partition counts across active generations. When routing a
   * message, the message must be sent to the target partition computed for EACH of these counts.
   */
  public Set<Integer> activePartitionCounts() {
    return activeGenerations().stream()
        .map(RoutingGeneration::partitionCount)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  /**
   * Adds a new generation after partitions have been scaled up.
   *
   * @param newPartitionCount the new total partition count
   * @return updated state with the new generation appended
   */
  public MessageRoutingState addGeneration(final int newPartitionCount) {
    final int nextId =
        generations.isEmpty() ? 1 : generations.get(generations.size() - 1).generationId() + 1;

    if (!generations.isEmpty() && currentPartitionCount() >= newPartitionCount) {
      throw new IllegalArgumentException(
          "New partition count %d must be greater than current %d"
              .formatted(newPartitionCount, currentPartitionCount()));
    }

    final var newGenerations =
        new java.util.ArrayList<>(generations);
    newGenerations.add(new RoutingGeneration(nextId, newPartitionCount, false));
    return new MessageRoutingState(Collections.unmodifiableList(newGenerations));
  }

  /**
   * Retires (marks as complete) a routing generation. This should be called when there are no more
   * open message subscriptions that used this generation's partition count.
   *
   * <p>The current (latest) generation can never be retired.
   *
   * @param generationId the generation to retire
   * @return updated state with the generation marked as retired
   */
  public MessageRoutingState retireGeneration(final int generationId) {
    final var current = currentGeneration();
    if (current.generationId() == generationId) {
      throw new IllegalArgumentException("Cannot retire the current generation");
    }

    final var updated =
        generations.stream()
            .map(
                g ->
                    g.generationId() == generationId
                        ? new RoutingGeneration(g.generationId(), g.partitionCount(), true)
                        : g)
            .toList();
    return new MessageRoutingState(updated);
  }

  /**
   * Returns true if there is only one active generation (the current one), meaning no transition
   * period is in effect.
   */
  public boolean isFullyConverged() {
    return activeGenerations().size() <= 1;
  }

  /**
   * A routing generation represents a snapshot of the cluster's partition count at a point in time.
   *
   * @param generationId monotonically increasing identifier
   * @param partitionCount the total number of partitions when this generation was created
   * @param retired true if all subscriptions from this generation have completed
   */
  public record RoutingGeneration(int generationId, int partitionCount, boolean retired) {}
}
