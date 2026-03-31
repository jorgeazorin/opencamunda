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
import io.camunda.zeebe.scheduler.future.CompletableActorFuture;
import io.camunda.zeebe.topology.changes.TopologyChangeAppliers.MemberOperationApplier;
import io.camunda.zeebe.topology.state.ClusterTopology;
import io.camunda.zeebe.topology.state.MemberState;
import io.camunda.zeebe.topology.state.MemberState.State;
import io.camunda.zeebe.topology.state.PartitionState;
import io.camunda.zeebe.util.Either;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Applier for bootstrapping a brand-new partition that does not exist anywhere in the cluster. This
 * differs from {@link PartitionJoinApplier} which requires the partition to already have an active
 * member (so the joiner can replicate from it).
 *
 * <p>This creates the Raft group from scratch with an empty log and state.
 */
final class PartitionBootstrapApplier implements MemberOperationApplier {

  private final int partitionId;
  private final int priority;
  private final MemberId localMemberId;
  private final Map<MemberId, Integer> membersWithPriority;
  private final PartitionChangeExecutor partitionChangeExecutor;

  PartitionBootstrapApplier(
      final int partitionId,
      final int priority,
      final MemberId localMemberId,
      final Map<MemberId, Integer> membersWithPriority,
      final PartitionChangeExecutor partitionChangeExecutor) {
    this.partitionId = partitionId;
    this.priority = priority;
    this.localMemberId = localMemberId;
    this.membersWithPriority = membersWithPriority;
    this.partitionChangeExecutor = partitionChangeExecutor;
  }

  @Override
  public MemberId memberId() {
    return localMemberId;
  }

  @Override
  public Either<Exception, UnaryOperator<MemberState>> initMemberState(
      final ClusterTopology currentClusterTopology) {

    // Validate: the member must be ACTIVE
    final boolean localMemberIsActive =
        currentClusterTopology.hasMember(localMemberId)
            && currentClusterTopology.getMember(localMemberId).state() == State.ACTIVE;
    if (!localMemberIsActive) {
      return Either.left(
          new IllegalStateException(
              "Expected to bootstrap partition, but the local member is not active"));
    }

    // Validate: the partition must NOT already exist on any member
    final boolean partitionExists =
        currentClusterTopology.members().values().stream()
            .anyMatch(memberState -> memberState.partitions().containsKey(partitionId));
    if (partitionExists) {
      final MemberState localMemberState = currentClusterTopology.getMember(localMemberId);
      if (localMemberState.partitions().containsKey(partitionId)
          && localMemberState.getPartition(partitionId).state()
              == PartitionState.State.BOOTSTRAPPING) {
        // Idempotent: already bootstrapping from a previous attempt
        return Either.right(memberState -> memberState);
      }
      return Either.left(
          new IllegalStateException(
              String.format(
                  "Expected to bootstrap partition %d, but it already exists in the cluster",
                  partitionId)));
    }

    return Either.right(
        memberState ->
            memberState.addPartition(partitionId, PartitionState.bootstrapping(priority)));
  }

  @Override
  public ActorFuture<UnaryOperator<MemberState>> applyOperation() {
    final CompletableActorFuture<UnaryOperator<MemberState>> result =
        new CompletableActorFuture<>();

    partitionChangeExecutor
        .bootstrap(partitionId, membersWithPriority)
        .onComplete(
            (ignore, error) -> {
              if (error == null) {
                result.complete(
                    memberState ->
                        memberState.updatePartition(partitionId, PartitionState::toActive));
              } else {
                result.completeExceptionally(error);
              }
            });
    return result;
  }
}
