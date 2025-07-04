/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.exception.PartitionNotExistsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.HashSet;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.BROKER_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus.*;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.legitMove;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.legitMoveBetweenDisks;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.eligibleBrokers;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.eligibleReplicasForSwap;


/**
 * An abstract class for goals. This class will be extended to create custom goals for different purposes -- e.g.
 * balancing the distribution of replicas or resources in the cluster.
 */
public abstract class AbstractGoal implements Goal {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractGoal.class);
  protected boolean _finished;
  protected boolean _succeeded;
  protected BalancingConstraint _balancingConstraint;
  protected int _numWindows;
  protected double _minMonitoredPartitionPercentage;
  protected ProvisionResponse _provisionResponse;
  protected Cluster _kafkaCluster;
  // Track which KAFKA-19148 blocks have been logged to avoid spammy logs
  private final Set<String> _loggedKafka19148Blocks = new HashSet<>();

  /**
   * Constructor of Abstract Goal class sets the
   * <ul>
   *   <li>{@link #_finished} flag to {@code false} to signal that the goal has not been optimized, yet.</li>
   *   <li>{@link #_provisionResponse} with {@link ProvisionStatus#UNDECIDED} status to signal that the goal has not identified the
   *   cluster as under-provisioned, over-provisioned, or right-sized.</li>
   * </ul>
   */
  public AbstractGoal() {
    _finished = false;
    _succeeded = true;
    _provisionResponse = new ProvisionResponse(UNDECIDED);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    KafkaCruiseControlConfig parsedConfig = new KafkaCruiseControlConfig(configs);
    _balancingConstraint = new BalancingConstraint(parsedConfig);
    _numWindows = parsedConfig.getInt(MonitorConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG);
    _minMonitoredPartitionPercentage = parsedConfig.getDouble(MonitorConfig.MIN_VALID_PARTITION_RATIO_CONFIG);
  }

  /**
   * Set the Kafka cluster metadata for ISR safety checks.
   *
   * @param kafkaCluster The Kafka cluster metadata.
   */
  public void setKafkaCluster(Cluster kafkaCluster) {
    _kafkaCluster = kafkaCluster;
    // Clear logged blocks when cluster metadata is refreshed for a new optimization run
    _loggedKafka19148Blocks.clear();
  }

  /**
   * Safety check for KAFKA-19148: Prevent replica movements that could trigger unclean leader elections
   * in KRaft mode. The issue occurs when removing the current leader from the replica set while
   * there are non-ISR replicas in the remaining set.
   *
   * @param clusterModel The cluster model.
   * @param replica The replica being moved.
   * @param destinationBroker The destination broker for the move.
   * @param action The type of action being performed.
   * @return true if the move is safe to proceed, false if it should be skipped.
   */
  private boolean isReplicaMoveSafeForKRaft(ClusterModel clusterModel, Replica replica, Broker destinationBroker, ActionType action) {
    // Only check inter-broker movements that could affect ISR
    if (_kafkaCluster == null ||
        (action != ActionType.INTER_BROKER_REPLICA_MOVEMENT &&
         action != ActionType.INTER_BROKER_REPLICA_SWAP)) {
      return true;
    }

    org.apache.kafka.common.TopicPartition topicPartition = replica.topicPartition();

    try {
      PartitionInfo partitionInfo = _kafkaCluster.partition(topicPartition);
      if (partitionInfo == null) {
        String blockKey = String.format("NO_METADATA:%s", topicPartition);
        if (_loggedKafka19148Blocks.add(blockKey)) {
          LOG.warn("KAFKA-19148 check: Partition {} does not exist in Kafka metadata, skipping move", topicPartition);
        }
        return false;
      }

      // Get current state from Kafka metadata
      Node currentLeader = partitionInfo.leader();
      if (currentLeader == null) {
        LOG.debug("KAFKA-19148 check: Partition {} has no leader, allowing move", topicPartition);
        return true;
      }

      Set<Integer> currentReplicaIds = Arrays.stream(partitionInfo.replicas())
          .mapToInt(Node::id)
          .boxed()
          .collect(Collectors.toSet());
      Set<Integer> currentIsrIds = Arrays.stream(partitionInfo.inSyncReplicas())
          .mapToInt(Node::id)
          .boxed()
          .collect(Collectors.toSet());

      // Log current state for debugging
      LOG.debug("KAFKA-19148 check for {}: current leader={}, replicas={}, ISR={}, " +
                "moving replica from broker {} to broker {}",
                topicPartition, currentLeader.id(), currentReplicaIds, currentIsrIds,
                replica.broker().id(), destinationBroker.id());

      // Check if we're moving the current leader
      boolean movingCurrentLeader = currentLeader.id() == replica.broker().id();
      if (!movingCurrentLeader) {
        LOG.debug("KAFKA-19148 check: Not moving current leader for {}, move is safe", topicPartition);
        return true;
      }

      // Special case: Single-replica partitions
      if (currentReplicaIds.size() == 1) {
        // Moving a single-replica partition means removing the current leader with no other replicas
        // This is exactly the KAFKA-19148 scenario we need to prevent
        String blockKey = String.format("SINGLE_REPLICA:%s:%d->%d", topicPartition, currentLeader.id(), destinationBroker.id());
        if (_loggedKafka19148Blocks.add(blockKey)) {
          LOG.warn("KAFKA-19148 BLOCKED: Cannot move single-replica partition {} - would remove current leader {} " +
                   "with no other replicas. Should first add replica to destination broker {}",
                   topicPartition, currentLeader.id(), destinationBroker.id());
        }
        return false;
      }

      // Multi-replica case: determine the new replica set after the move
      Set<Integer> newReplicaIds = new HashSet<>(currentReplicaIds);
      // Remove the source broker
      newReplicaIds.remove(replica.broker().id());
      // Add the destination broker
      newReplicaIds.add(destinationBroker.id());

      // Check if the current leader will be removed from the replica set
      boolean leaderBeingRemoved = !newReplicaIds.contains(currentLeader.id());

      if (leaderBeingRemoved) {
        // Critical check: When removing the current leader, ALL remaining replicas must be in ISR
        // to prevent KRaft from electing an out-of-sync replica
        boolean allNewReplicasInIsr = newReplicaIds.stream().allMatch(currentIsrIds::contains);

        if (!allNewReplicasInIsr) {
          Set<Integer> nonIsrReplicas = newReplicaIds.stream()
              .filter(id -> !currentIsrIds.contains(id))
              .collect(Collectors.toSet());

          String blockKey = String.format("LEADER_REMOVAL_NON_ISR:%s:%d:non-isr%s",
                                          topicPartition, currentLeader.id(), nonIsrReplicas);
          if (_loggedKafka19148Blocks.add(blockKey)) {
            LOG.warn("KAFKA-19148 BLOCKED: Skipping movement for partition {} - removing current leader {} " +
                     "while non-ISR replicas {} would remain in replica set. Current ISR: {}",
                     topicPartition, currentLeader.id(), nonIsrReplicas, currentIsrIds);
          }
          return false;
        }

        LOG.debug("KAFKA-19148 check: Leader removal for {} is safe - all remaining replicas {} are in ISR {}",
                  topicPartition, newReplicaIds, currentIsrIds);
      } else {
        // Moving leader within the replica set - check if destination is in ISR
        if (!currentIsrIds.contains(destinationBroker.id())) {
          String blockKey = String.format("LEADER_TO_NON_ISR:%s:%d->%d",
                                          topicPartition, currentLeader.id(), destinationBroker.id());
          if (_loggedKafka19148Blocks.add(blockKey)) {
            LOG.warn("KAFKA-19148 BLOCKED: Skipping movement for partition {} - moving leader {} to " +
                     "out-of-sync broker {}. Current ISR: {}",
                     topicPartition, currentLeader.id(), destinationBroker.id(), currentIsrIds);
          }
          return false;
        }

        LOG.debug("KAFKA-19148 check: Leader movement for {} is safe - destination {} is in ISR",
                  topicPartition, destinationBroker.id());
      }

    } catch (Exception e) {
      LOG.warn("KAFKA-19148 check: Error checking partition {}, skipping move for safety", topicPartition, e);
      return false;
    }

    return true;
  }

  private static boolean hasExcludedBrokersForReplicaMoveWithReplicas(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    Set<Integer> excludedBrokers = optimizationOptions.excludedBrokersForReplicaMove();
    return clusterModel.aliveBrokers().stream().anyMatch(broker -> excludedBrokers.contains(broker.id()) && !broker.replicas().isEmpty());
  }

  @Override
  public boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    try {
      _succeeded = true;
      // Resetting the provision response ensures fresh provision response if the same goal is optimized multiple times.
      _provisionResponse = new ProvisionResponse(UNDECIDED);
      LOG.debug("Starting optimization for {}.", name());
      // Initialize pre-optimized stats.
      ClusterModelStats statsBeforeOptimization = clusterModel.getClusterStats(_balancingConstraint, optimizationOptions);
      LOG.trace("[PRE - {}] {}", name(), statsBeforeOptimization);
      _finished = false;
      long goalStartTime = System.currentTimeMillis();
      initGoalState(clusterModel, optimizationOptions);
      SortedSet<Broker> brokenBrokers = clusterModel.brokenBrokers();
      boolean originallyHasExcludedBrokersForReplicaMoveWithReplicas = hasExcludedBrokersForReplicaMoveWithReplicas(clusterModel,
                                                                                                                    optimizationOptions);
      while (!_finished) {
        for (Broker broker : brokersToBalance(clusterModel)) {
          rebalanceForBroker(broker, clusterModel, optimizedGoals, optimizationOptions);
        }
        updateGoalState(clusterModel, optimizationOptions);
      }
      ClusterModelStats statsAfterOptimization = clusterModel.getClusterStats(_balancingConstraint, optimizationOptions);
      LOG.trace("[POST - {}] {}", name(), statsAfterOptimization);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Finished optimization for {} in {}ms.", name(), System.currentTimeMillis() - goalStartTime);
      }
      // Log KAFKA-19148 block summary if any movements were blocked
      if (!_loggedKafka19148Blocks.isEmpty()) {
        LOG.info("KAFKA-19148 summary for goal {}: {} unique movements were blocked to prevent unclean leader elections",
                 name(), _loggedKafka19148Blocks.size());
      }
      LOG.trace("Cluster after optimization is {}", clusterModel);
      // The optimization cannot make stats worse unless the cluster has (1) broken brokers or (2) excluded brokers for replica move with replicas.
      if (brokenBrokers.isEmpty() && !originallyHasExcludedBrokersForReplicaMoveWithReplicas) {
        ClusterModelStatsComparator comparator = clusterModelStatsComparator();
        // Throw exception when the stats before optimization is preferred.
        if (comparator.compare(statsAfterOptimization, statsBeforeOptimization) < 0) {
          // If a goal provides worse stats after optimization, that indicates an implementation error with the goal.
          throw new IllegalStateException(String.format("Optimization for goal %s failed because the optimized result is worse than before."
                                                        + " Reason: %s.", name(), comparator.explainLastComparison()));
        }
      }
      // Ensure that a cluster is not identified as over provisioned unless it has the minimum required number of alive brokers and
      // expected number of brokers after the provisioning will still be larger than or equal to the max RF
      _provisionResponse =
          GoalUtils.validateProvisionResponse(_provisionResponse, clusterModel, _balancingConstraint.overprovisionedMinBrokers());
      return _succeeded;
    } catch (OptimizationFailureException ofe) {
      _provisionResponse = new ProvisionResponse(UNDER_PROVISIONED, ofe.provisionRecommendation(), name());
      // Mitigation (if relevant) is reported as part of exception message to provide helpful tips concerning the used optimizationOptions.
      String mitigation = GoalUtils.mitigationForOptimizationFailures(optimizationOptions);
      String message = String.format("%s%s", ofe.getMessage(), mitigation.isEmpty() ? "" : String.format(" || Tips: %s", mitigation));
      throw new OptimizationFailureException(message, ofe.provisionRecommendation());
    } finally {
      // Clear any sorted replicas tracked in the process of optimization.
      clusterModel.clearSortedReplicas();
    }
  }

  /**
   * A default implementation
   * @return Dynamically obtained simple name of the class.  Works even with sub-classing
   */
  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void finish() {
    _finished = true;
  }

  @Override
  public ProvisionStatus provisionStatus() {
    return provisionResponse().status();
  }

  @Override
  public ProvisionResponse provisionResponse() {
    return _provisionResponse;
  }

  /**
   * Get sorted brokers that the rebalance process will go over to apply balancing actions to replicas they contain.
   *
   * @param clusterModel The state of the cluster.
   * @return A collection of brokers that the rebalance process will go over to apply balancing actions to replicas
   * they contain.
   */
  protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
    return clusterModel.brokers();
  }

  /**
   * Check if requirements of this goal are not violated if this action is applied to the given cluster state,
   * {@code false} otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model.
   * @return {@code true} if requirements of this goal are not violated if this action is applied to the given cluster state,
   * {@code false} otherwise.
   */
  protected abstract boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action);

  /**
   * Initialize states that this goal requires -- e.g. run sanity checks regarding hard goals requirements.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  protected abstract void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException;

  /**
   * Update goal state after one round of self-healing / rebalance.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  protected abstract void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException;

  /**
   * Rebalance the given broker without violating the constraints of the current goal and optimized goals.
   *
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param optimizationOptions Options to take into account during optimization.
   */
  protected abstract void rebalanceForBroker(Broker broker,
                                             ClusterModel clusterModel,
                                             Set<Goal> optimizedGoals,
                                             OptimizationOptions optimizationOptions)
      throws OptimizationFailureException;

  /**
   * Attempt to apply the given balancing action to the given replica in the given cluster. The application
   * considers the candidate brokers as the potential destination brokers for replica movement or the location of
   * followers for leadership transfer. If the movement attempt succeeds, the function returns the broker id of the
   * destination, otherwise the function returns null.
   *
   * @param clusterModel    The state of the cluster.
   * @param replica         Replica to be applied the given balancing action.
   * @param candidateBrokers Candidate brokers as the potential destination brokers for replica movement or the location
   *                        of followers for leadership transfer.
   * @param action          Balancing action.
   * @param optimizedGoals  Optimized goals.
   * @param optimizationOptions Options to take into account during optimization -- e.g. excluded brokers for leadership.
   * @return Destination broker if the movement attempt succeeds, null otherwise.
   */
  protected Broker maybeApplyBalancingAction(ClusterModel clusterModel,
                                             Replica replica,
                                             Collection<Broker> candidateBrokers,
                                             ActionType action,
                                             Set<Goal> optimizedGoals,
                                             OptimizationOptions optimizationOptions) {
    // In self healing mode, allow a move only from dead to alive brokers.
    if ((!clusterModel.deadBrokers().isEmpty() && replica.originalBroker().isAlive())
        || (!clusterModel.brokersWithBadDisks().isEmpty() && !replica.isOriginalOffline())) {
      //return null;
      LOG.trace("Applying {} to an online replica in in self-healing mode.", action);
    }
    List<Broker> eligibleBrokers = eligibleBrokers(clusterModel, replica, candidateBrokers, action, optimizationOptions);
    for (Broker broker : eligibleBrokers) {
      BalancingAction proposal = new BalancingAction(replica.topicPartition(), replica.broker().id(), broker.id(), action);
      // A replica should be moved if:
      // 0. The move is legit.
      // 1. The goal requirements are not violated if this action is applied to the given cluster state.
      // 2. The movement is acceptable by the previously optimized goals.

      if (!legitMove(replica, broker, clusterModel, action)) {
        LOG.trace("Replica move to broker is not legit for {}.", proposal);
        continue;
      }

      // KAFKA-19148 safety check: Prevent replica movements that could trigger unclean leader elections
      if (!isReplicaMoveSafeForKRaft(clusterModel, replica, broker, action)) {
        LOG.trace("Skipping replica move for {} to prevent KAFKA-19148.", proposal);
        continue;
      }

      if (!selfSatisfied(clusterModel, proposal)) {
        LOG.trace("Unable to self-satisfy proposal {}.", proposal);
        continue;
      }

      ActionAcceptance acceptance = AnalyzerUtils.isProposalAcceptableForOptimizedGoals(optimizedGoals, proposal, clusterModel);
      LOG.trace("Trying to apply legit and self-satisfied action {} for {}, actionAcceptance = {}",
                proposal, replica.topicPartition(), acceptance);
      if (acceptance == ACCEPT) {
        // Log when we're applying the move
        if (action == ActionType.INTER_BROKER_REPLICA_MOVEMENT || action == ActionType.INTER_BROKER_REPLICA_SWAP || action == ActionType.LEADERSHIP_MOVEMENT) {
          LOG.debug("Applying {} for partition {}: moving replica from broker {} to broker {}",
                    action, replica.topicPartition(), replica.broker().id(), broker.id());
        }
        if (action == ActionType.LEADERSHIP_MOVEMENT) {
          clusterModel.relocateLeadership(replica.topicPartition(), replica.broker().id(), broker.id());
        } else if (action == ActionType.INTER_BROKER_REPLICA_MOVEMENT) {
          clusterModel.relocateReplica(replica.topicPartition(), replica.broker().id(), broker.id());
        }
        return broker;
      } else if (acceptance == BROKER_REJECT) {
        LOG.trace("Broker rejected for {}: {}", replica.topicPartition(), proposal);
      }
    }
    return null;
  }

  /**
   * Attempt to swap the given source replica with a replica from the candidate replicas to swap with. The function
   * returns the swapped in replica if succeeded, null otherwise.
   * All the replicas in the given candidateReplicasToSwapWith must be from the same broker.
   *
   * @param clusterModel The state of the cluster.
   * @param sourceReplica Replica to be swapped with.
   * @param candidateReplicas Candidate replicas (from the same candidate broker) to swap with the source replica in the
   *                          order of attempts to swap.
   * @param optimizedGoals Optimized goals.
   * @param optimizationOptions Options to take into account during optimization -- e.g. excluded brokers for leadership.
   * @return The swapped in replica if succeeded, null otherwise.
   */
  Replica maybeApplySwapAction(ClusterModel clusterModel,
                               Replica sourceReplica,
                               SortedSet<Replica> candidateReplicas,
                               Set<Goal> optimizedGoals,
                               OptimizationOptions optimizationOptions) {
    SortedSet<Replica> eligibleReplicas = eligibleReplicasForSwap(clusterModel, sourceReplica, candidateReplicas, optimizationOptions);
    if (eligibleReplicas.isEmpty()) {
      return null;
    }

    Broker destinationBroker = eligibleReplicas.first().broker();

    for (Replica destinationReplica : eligibleReplicas) {
      BalancingAction swapProposal = new BalancingAction(sourceReplica.topicPartition(),
                                                         sourceReplica.broker().id(), destinationBroker.id(),
                                                         ActionType.INTER_BROKER_REPLICA_SWAP, destinationReplica.topicPartition());
      // A sourceReplica should be swapped with a replicaToSwapWith if:
      // 0. The swap from source to destination is legit.
      // 1. The swap from destination to source is legit.
      // 2. The goal requirements are not violated if this action is applied to the given cluster state.
      // 3. The movement is acceptable by the previously optimized goals.
      if (!legitMove(sourceReplica, destinationBroker, clusterModel, ActionType.INTER_BROKER_REPLICA_MOVEMENT)) {
        LOG.trace("Swap from source to destination broker is not legit for {}.", swapProposal);
        return null;
      }

      if (!legitMove(destinationReplica, sourceReplica.broker(), clusterModel, ActionType.INTER_BROKER_REPLICA_MOVEMENT)) {
        LOG.trace("Swap from destination to source broker is not legit for {}.", swapProposal);
        continue;
      }

      // KAFKA-19148 safety check for both replicas involved in the swap
      if (!isReplicaMoveSafeForKRaft(clusterModel, sourceReplica, destinationBroker, ActionType.INTER_BROKER_REPLICA_SWAP)) {
        LOG.trace("Skipping swap to prevent KAFKA-19148 for source replica in {}.", swapProposal);
        continue;
      }

      if (!isReplicaMoveSafeForKRaft(clusterModel, destinationReplica, sourceReplica.broker(), ActionType.INTER_BROKER_REPLICA_SWAP)) {
        LOG.trace("Skipping swap to prevent KAFKA-19148 for destination replica in {}.", swapProposal);
        continue;
      }

      // The current goal is expected to know whether a swap is doable between given brokers.
      if (!selfSatisfied(clusterModel, swapProposal)) {
        // Unable to satisfy proposal for this eligible replica and the remaining eligible replicas in the list.
        LOG.trace("Unable to self-satisfy swap proposal {}.", swapProposal);
        return null;
      }
      ActionAcceptance acceptance = AnalyzerUtils.isProposalAcceptableForOptimizedGoals(optimizedGoals, swapProposal, clusterModel);
      LOG.trace("Trying to apply legit and self-satisfied swap {}, actionAcceptance = {}.", swapProposal, acceptance);

      if (acceptance == ACCEPT) {
        Broker sourceBroker = sourceReplica.broker();
        clusterModel.relocateReplica(sourceReplica.topicPartition(), sourceBroker.id(), destinationBroker.id());
        clusterModel.relocateReplica(destinationReplica.topicPartition(), destinationBroker.id(), sourceBroker.id());
        return destinationReplica;
      } else if (acceptance == BROKER_REJECT) {
        // Unable to swap the given source replica with any replicas in the destination broker.
        return null;
      }
    }
    return null;
  }

  /**
   * Attempt to move replica between disks of the same broker. The application considers the candidate disks as the potential
   * destination disk for replica movement. If the movement attempt succeeds, the function returns the destination disk,
   * otherwise the function returns null.
   *
   * @param clusterModel    The state of the cluster.
   * @param replica         Replica to be moved.
   * @param candidateDisks  Candidate disks as the potential destination for replica movement.
   * @param optimizedGoals  Optimized goals.
   * @return The destination disk if the movement attempt succeeds, null otherwise.
   */
  protected Disk maybeMoveReplicaBetweenDisks(ClusterModel clusterModel,
                                              Replica replica,
                                              Collection<Disk> candidateDisks,
                                              Set<Goal> optimizedGoals) {
    for (Disk disk : candidateDisks) {
      BalancingAction proposal = new BalancingAction(replica.topicPartition(),
                                                     replica.disk(),
                                                     disk,
                                                     ActionType.INTRA_BROKER_REPLICA_MOVEMENT);
      if (!legitMoveBetweenDisks(replica, disk, ActionType.INTRA_BROKER_REPLICA_MOVEMENT)) {
        LOG.trace("Replica move to disk is not legit for {}.", proposal);
        continue;
      }

      if (!selfSatisfied(clusterModel, proposal)) {
        LOG.trace("Unable to self-satisfy proposal {}.", proposal);
        continue;
      }

      ActionAcceptance acceptance = AnalyzerUtils.isProposalAcceptableForOptimizedGoals(optimizedGoals, proposal, clusterModel);
      LOG.trace("Trying to apply legit and self-satisfied action {}, actionAcceptance = {}", proposal, acceptance);
      if (acceptance == ACCEPT) {
        clusterModel.relocateReplica(replica.topicPartition(), replica.broker().id(), disk.logDir());
        return disk;
      }
    }
    return null;
  }

  /**
   * Attempt to swap replicas on different disks of the same broker. The function returns the swapped in replica if succeeded,
   * null otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param sourceReplica Replica to be swapped with.
   * @param candidateReplicas Candidate replicas to swap with the source replica in the order of attempts to swap.
   * @param optimizedGoals Optimized goals.
   * @return The swapped in replica if succeeded, null otherwise.
   */
  Replica maybeSwapReplicaBetweenDisks(ClusterModel clusterModel,
                                       Replica sourceReplica,
                                       SortedSet<Replica> candidateReplicas,
                                       Set<Goal> optimizedGoals) {
    for (Replica destinationReplica : candidateReplicas) {
      BalancingAction swapProposal = new BalancingAction(sourceReplica.topicPartition(),
                                                         sourceReplica.disk(),
                                                         destinationReplica.disk(),
                                                         ActionType.INTRA_BROKER_REPLICA_SWAP,
                                                         destinationReplica.topicPartition());
      // A sourceReplica should be swapped with a destinationReplica if:
      // 0. The swap from source to destination is legit.
      // 1. The swap from destination to source is legit.
      // 2. The goal requirements are not violated if this action is applied to the given cluster state.
      // 3. The movement is acceptable by the previously optimized goals.
      if (!legitMoveBetweenDisks(sourceReplica, destinationReplica.disk(), ActionType.INTRA_BROKER_REPLICA_MOVEMENT)) {
        LOG.trace("Swap from source to destination disk is not legit for {}.", swapProposal);
        return null;
      }

      if (!legitMoveBetweenDisks(destinationReplica, sourceReplica.disk(), ActionType.INTRA_BROKER_REPLICA_MOVEMENT)) {
        LOG.trace("Swap from destination to source disk is not legit for {}.", swapProposal);
        continue;
      }

      if (!selfSatisfied(clusterModel, swapProposal)) {
        // Unable to satisfy proposal for this eligible replica and the remaining eligible replicas in the list.
        LOG.trace("Unable to self-satisfy swap proposal {}.", swapProposal);
        return null;
      }

      ActionAcceptance acceptance = AnalyzerUtils.isProposalAcceptableForOptimizedGoals(optimizedGoals, swapProposal, clusterModel);
      LOG.trace("Trying to apply legit and self-satisfied swap {}, actionAcceptance = {}.", swapProposal, acceptance);
      if (acceptance == ACCEPT) {
        clusterModel.relocateReplica(sourceReplica.topicPartition(), sourceReplica.broker().id(), destinationReplica.disk().logDir());
        clusterModel.relocateReplica(destinationReplica.topicPartition(), destinationReplica.broker().id(), sourceReplica.disk().logDir());
        return destinationReplica;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return name();
  }
}
