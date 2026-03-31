/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.deployment.distribute;

import static io.camunda.zeebe.protocol.Protocol.DEPLOYMENT_PARTITION;
import static io.camunda.zeebe.protocol.Protocol.START_PARTITION_ID;

import io.camunda.zeebe.engine.state.immutable.DeploymentState;
import io.camunda.zeebe.stream.api.ReadonlyStreamProcessorContext;
import io.camunda.zeebe.stream.api.StreamProcessorLifecycleAware;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.IntSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors for newly added partitions and triggers redistribution of all existing deployments to
 * them. This runs periodically on partition 1 (DEPLOYMENT_PARTITION) and detects when the cluster's
 * partition count has increased.
 *
 * <p>When new partitions are detected, this component iterates over all stored deployments and sends
 * a DISTRIBUTE command for each deployment to each new partition via the
 * {@link DeploymentDistributionCommandSender}.
 */
public class NewPartitionDeploymentDistributor implements StreamProcessorLifecycleAware {

  private static final Duration CHECK_INTERVAL = Duration.ofSeconds(30);
  private static final Logger LOG =
      LoggerFactory.getLogger(NewPartitionDeploymentDistributor.class);

  private final DeploymentDistributionCommandSender commandSender;
  private final DeploymentState deploymentState;
  private final IntSupplier partitionsCountSupplier;
  private final Set<Integer> knownPartitions;

  public NewPartitionDeploymentDistributor(
      final DeploymentDistributionCommandSender commandSender,
      final DeploymentState deploymentState,
      final IntSupplier partitionsCountSupplier,
      final int initialPartitionCount) {
    this.commandSender = commandSender;
    this.deploymentState = deploymentState;
    this.partitionsCountSupplier = partitionsCountSupplier;
    this.knownPartitions = new HashSet<>();
    for (int i = START_PARTITION_ID; i < START_PARTITION_ID + initialPartitionCount; i++) {
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
    final int currentPartitionCount = partitionsCountSupplier.getAsInt();
    final Set<Integer> newPartitions = new HashSet<>();

    for (int i = START_PARTITION_ID; i < START_PARTITION_ID + currentPartitionCount; i++) {
      if (!knownPartitions.contains(i)) {
        newPartitions.add(i);
      }
    }

    if (newPartitions.isEmpty()) {
      return;
    }

    LOG.info(
        "Detected {} new partition(s): {}. Redistributing all deployments.",
        newPartitions.size(),
        newPartitions);

    deploymentState.foreachStoredDeployment(
        (deploymentKey, deploymentRecord) -> {
          for (final int partitionId : newPartitions) {
            LOG.debug(
                "Distributing deployment {} to new partition {}", deploymentKey, partitionId);
            commandSender.distributeToPartition(deploymentKey, partitionId, deploymentRecord);
          }
        });

    knownPartitions.addAll(newPartitions);
    LOG.info("Deployment redistribution to new partitions complete. Known partitions: {}",
        knownPartitions);
  }
}
