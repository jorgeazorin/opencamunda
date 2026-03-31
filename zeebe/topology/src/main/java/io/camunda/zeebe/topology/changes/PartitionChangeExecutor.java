/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.topology.changes;

import io.atomix.cluster.MemberId;
import io.camunda.zeebe.scheduler.future.ActorFuture;
import java.util.Collection;
import java.util.Map;

/**
 * Represents the executor that executes the actual process to start or start. The concrete
 * implementation of this interface is expected to be a call back to the system component that can
 * start or stop partition. This is typically the PartitionManager in the Broker.
 */
public interface PartitionChangeExecutor {

  /**
   * The implementation of this method must start the partition on this member. The partition must
   * join the replication group formed by the members given in the {@code membersWithPriority}. The
   * implementation must be idempotent. If the node restarts after this method was called, but
   * before marking the operation as completed, it will be retried after the restart.
   *
   * @param partitionId id of the partition
   * @param membersWithPriority priority of each replicas used of leader election
   * @return a future that completes when the partition is started and joined the replication group
   */
  ActorFuture<Void> join(int partitionId, Map<MemberId, Integer> membersWithPriority);

  /**
   * The implementation of this method must remove the member from the replication group of the
   * given partition and stops the partition on this member. The implementation must be idempotent.
   * If the node restarts after this method was called, but before marking the operation as
   * completed, it will be retried after the restart.
   *
   * @param partitionId id of the partition
   * @return a future that completes when the partition is stopped and removed from the replication.
   */
  ActorFuture<Void> leave(int partitionId);

  /**
   * Updates the priority of the member used for raft priority election for the given partition.
   *
   * @param partitionId id of the partition
   * @param newPriority new priority value
   * @return a future that completes when the priority is updated
   */
  ActorFuture<Void> reconfigurePriority(int partitionId, int newPriority);

  /**
   * Force reconfigure a partition to include only the given members in the replication group.
   *
   * @param partitionId id of the partition
   * @param members members that will be part of the replication group after reconfiguring
   * @return a future that completes when the partition is reconfigured
   */
  ActorFuture<Void> forceReconfigure(final int partitionId, final Collection<MemberId> members);

  /**
   * Bootstrap a brand-new partition that does not yet exist in the cluster. This creates a new Raft
   * group from scratch with an empty log and state.
   *
   * <p>This differs from {@link #join(int, Map)} in that join expects the partition to already
   * exist with at least one active member, while bootstrap creates it from nothing.
   *
   * @param partitionId id of the new partition
   * @param membersWithPriority all initial members of the new Raft group with their priorities
   * @return a future that completes when the partition is bootstrapped and active
   */
  ActorFuture<Void> bootstrap(int partitionId, Map<MemberId, Integer> membersWithPriority);
}
