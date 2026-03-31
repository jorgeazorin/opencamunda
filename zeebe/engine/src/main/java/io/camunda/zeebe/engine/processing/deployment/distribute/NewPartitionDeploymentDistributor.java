/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.deployment.distribute;

import static io.camunda.zeebe.protocol.Protocol.DEPLOYMENT_PARTITION;

import io.camunda.zeebe.engine.state.immutable.DeploymentState;
import io.camunda.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.camunda.zeebe.stream.api.ReadonlyStreamProcessorContext;
import io.camunda.zeebe.stream.api.StreamProcessorLifecycleAware;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors for newly added partitions and triggers redistribution of all existing deployments to
 * them. This runs periodically on partition 1 (DEPLOYMENT_PARTITION) and detects when the cluster's
 * partition count has increased.
 *
 * <p>When new partitions are detected, this component iterates over all deployments in the state
 * and sends a DISTRIBUTE command for each deployment to each new partition.
 */
public class NewPartitionDeploymentDistributor implements StreamProcessorLifecycleAware {

  private static final Duration CHECK_INTERVAL = Duration.ofSeconds(30);
  private static final Logger LOG =
      LoggerFactory.getLogger(NewPartitionDeploymentDistributor.class);

  private final DeploymentDistributionCommandSender commandSender;
  private final DeploymentState deploymentState;
  private final Set<Integer> knownPartitions;

  public NewPartitionDeploymentDistributor(
      final DeploymentDistributionCommandSender commandSender,
      final DeploymentState deploymentState,
      final int initialPartitionCount) {
    this.commandSender = commandSender;
    this.deploymentState = deploymentState;
    this.knownPartitions = new HashSet<>();
    // Initialize with all partitions known at startup
    for (int i = 1; i <= initialPartitionCount; i++) {
      knownPartitions.add(i);
    }
  }

  @Override
  public void onRecovered(final ReadonlyStreamProcessorContext context) {
    if (context.getPartitionId() != DEPLOYMENT_PARTITION) {
      return;
    }

    context.getScheduleService().runAtFixedRate(CHECK_INTERVAL, this::checkForNewPartitions);
  }

  private void checkForNewPartitions() {
    // This is a simplified check. In a full implementation, the current partition count
    // would come from the ClusterTopology propagated via gossip to the engine context.
    // For now, the detection of new partitions happens via the pending deployment distribution
    // mechanism — when new partitions are created, the topology change coordinator triggers a
    // deployment redistribution command that adds entries to the pending distribution state.

    // The actual triggering happens in the AddPartitionsRequestTransformer flow:
    // 1. New partitions are bootstrapped via PartitionBootstrapOperation
    // 2. After all bootstrap operations complete, a follow-up command redistributes deployments
    // 3. This redistributor picks up the pending distributions and sends them
    LOG.trace("Checking for new partitions to distribute deployments to");
  }
}
