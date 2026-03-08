/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.state.appliers;

import io.camunda.zeebe.engine.state.TypedEventApplier;
import io.camunda.zeebe.engine.state.mutable.MutableDecisionState;
import io.camunda.zeebe.engine.state.mutable.MutableDeploymentState;
import io.camunda.zeebe.engine.state.mutable.MutableFormState;
import io.camunda.zeebe.engine.state.mutable.MutableProcessState;
import io.camunda.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.camunda.zeebe.protocol.record.intent.DeploymentIntent;

/**
 * Version 3 of the DeploymentIntent.CREATED applier. Extends v1 behavior (storing the deployment
 * record) with population of binding-type column families (CF82-84) that map resource identifiers
 * to deployment keys.
 */
public class DeploymentCreatedV3Applier
    implements TypedEventApplier<DeploymentIntent, DeploymentRecord> {

  private final MutableDeploymentState mutableDeploymentState;
  private final MutableProcessState mutableProcessState;
  private final MutableDecisionState mutableDecisionState;
  private final MutableFormState mutableFormState;

  public DeploymentCreatedV3Applier(
      final MutableDeploymentState mutableDeploymentState,
      final MutableProcessState mutableProcessState,
      final MutableDecisionState mutableDecisionState,
      final MutableFormState mutableFormState) {
    this.mutableDeploymentState = mutableDeploymentState;
    this.mutableProcessState = mutableProcessState;
    this.mutableDecisionState = mutableDecisionState;
    this.mutableFormState = mutableFormState;
  }

  @Override
  public void applyState(final long key, final DeploymentRecord value) {
    mutableDeploymentState.storeDeploymentRecord(key, value);
    setDeploymentKeyOnResources(key, value);
  }

  private void setDeploymentKeyOnResources(final long deploymentKey, final DeploymentRecord value) {
    value
        .processesMetadata()
        .forEach(
            metadata -> {
              if (metadata.isDuplicate()) {
                mutableProcessState.addDeploymentKeyMapping(
                    metadata.getTenantId(),
                    metadata.getBpmnProcessId(),
                    metadata.getKey(),
                    deploymentKey);
              } else {
                mutableProcessState.setMissingDeploymentKey(
                    metadata.getTenantId(), metadata.getKey(), deploymentKey);
              }
            });

    value
        .decisionsMetadata()
        .forEach(
            metadata -> {
              if (metadata.isDuplicate()) {
                mutableDecisionState.addDeploymentKeyMapping(
                    metadata.getTenantId(),
                    metadata.getDecisionId(),
                    metadata.getDecisionKey(),
                    deploymentKey);
              } else {
                mutableDecisionState.setMissingDeploymentKey(
                    metadata.getTenantId(), metadata.getDecisionKey(), deploymentKey);
              }
            });

    value
        .formMetadata()
        .forEach(
            metadata -> {
              if (metadata.isDuplicate()) {
                mutableFormState.addDeploymentKeyMapping(
                    metadata.getTenantId(),
                    metadata.getFormId(),
                    metadata.getFormKey(),
                    deploymentKey);
              } else {
                mutableFormState.setMissingDeploymentKey(
                    metadata.getTenantId(), metadata.getFormKey(), deploymentKey);
              }
            });
  }
}
