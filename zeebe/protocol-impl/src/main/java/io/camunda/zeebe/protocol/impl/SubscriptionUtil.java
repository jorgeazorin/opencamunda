/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.protocol.impl;

import static io.camunda.zeebe.protocol.Protocol.START_PARTITION_ID;

import java.util.Set;
import java.util.TreeSet;
import org.agrona.DirectBuffer;

public final class SubscriptionUtil {

  /**
   * Get the hash code of the subscription based on the given correlation key.
   *
   * @param correlationKey the correlation key
   * @return the hash code of the subscription
   */
  static int getSubscriptionHashCode(final DirectBuffer correlationKey) {
    // is equal to java.lang.String#hashCode
    int hashCode = 0;

    for (int i = 0, length = correlationKey.capacity(); i < length; i++) {
      hashCode = 31 * hashCode + correlationKey.getByte(i);
    }
    return hashCode;
  }

  /**
   * Get the partition id for message subscription based on the given correlation key.
   *
   * @param correlationKey the correlation key
   * @param partitionCount the number of partitions
   * @return the partition id for the subscription
   */
  public static int getSubscriptionPartitionId(
      final DirectBuffer correlationKey, final int partitionCount) {
    final int hashCode = getSubscriptionHashCode(correlationKey);
    // partition ids range from START_PARTITION_ID .. START_PARTITION_ID + partitionCount
    return Math.abs(hashCode % partitionCount) + START_PARTITION_ID;
  }

  /**
   * Computes the set of target partition IDs for a message across all active routing generations.
   *
   * <p>During a partition scaling transition, different message subscriptions may have been created
   * under different partition counts. To ensure messages reach the correct partition regardless of
   * which generation the subscription was created under, the message is sent to the target partition
   * for EACH active partition count.
   *
   * <p><b>Example (10 → 15 → 30 partitions):</b>
   * <pre>
   * activePartitionCounts = {10, 15, 30}
   * correlationKey = "order-123" (hash=12345)
   *   Gen 1: 12345 % 10 + 1 = partition 6
   *   Gen 2: 12345 % 15 + 1 = partition 1
   *   Gen 3: 12345 % 30 + 1 = partition 16
   *   → result: {1, 6, 16}
   * </pre>
   *
   * <p>When the cluster is fully converged (single generation), this returns a singleton set.
   *
   * @param correlationKey the correlation key
   * @param activePartitionCounts set of partition counts from each active routing generation
   * @return deduplicated set of partition IDs to send the message to
   */
  public static Set<Integer> getSubscriptionPartitionIds(
      final DirectBuffer correlationKey, final Set<Integer> activePartitionCounts) {
    final int hashCode = getSubscriptionHashCode(correlationKey);
    final Set<Integer> partitionIds = new TreeSet<>();
    for (final int partitionCount : activePartitionCounts) {
      partitionIds.add(Math.abs(hashCode % partitionCount) + START_PARTITION_ID);
    }
    return partitionIds;
  }
}
