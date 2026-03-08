/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.deployment.model.transformer.zeebe;

import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableJobWorkerTask;
import io.camunda.zeebe.engine.processing.deployment.model.element.JobWorkerProperties;
import io.camunda.zeebe.engine.processing.deployment.model.element.LinkedResource;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeLinkedResources;
import java.util.Collection;

public final class LinkedResourcesTransformer {

  public void transform(
      final ExecutableJobWorkerTask task, final ZeebeLinkedResources linkedResources) {

    if (linkedResources == null) {
      return;
    }

    var jobWorkerProperties = task.getJobWorkerProperties();
    if (jobWorkerProperties == null) {
      jobWorkerProperties = new JobWorkerProperties();
      task.setJobWorkerProperties(jobWorkerProperties);
    }

    final Collection<? extends io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeLinkedResource>
        resources = linkedResources.getLinkedResources();

    if (resources == null || resources.isEmpty()) {
      return;
    }

    for (final var bpmnResource : resources) {
      final var linkedResource = new LinkedResource();
      linkedResource.setResourceId(bpmnResource.getResourceId());
      linkedResource.setLinkName(bpmnResource.getLinkName());
      linkedResource.setResourceType(bpmnResource.getResourceType());
      linkedResource.setBindingType(bpmnResource.getBindingType());
      linkedResource.setVersionTag(bpmnResource.getVersionTag());
      jobWorkerProperties.getLinkedResources().add(linkedResource);
    }
  }
}
