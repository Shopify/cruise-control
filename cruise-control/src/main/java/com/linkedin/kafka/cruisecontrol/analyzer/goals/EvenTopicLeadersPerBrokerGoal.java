package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.INTER_BROKER_REPLICA_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.LEADERSHIP_MOVEMENT;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;
import com.linkedin.kafka.cruisecontrol.common.Utils;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;


/**
 * HARD GOAL: Generate leadership movement and leader replica movement proposals to ensure that each alive broker that
 * is not excluded for replica leadership moves has a number of leader replicas within a tight [floor, ceiling] range
 * for each topic in a configured set of topics (specified by {@link AnalyzerConfig#TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG}).
 *
 * <p>When {@link AnalyzerConfig#MIN_TOPIC_LEADERS_PER_BROKER_CONFIG} is set to 0 (dynamic mode), the bounds are computed as:
 * <ul>
 *   <li>floor = numTopicLeaders / numEligibleBrokers</li>
 *   <li>ceiling = ceil(numTopicLeaders / numEligibleBrokers)</li>
 * </ul>
 *
 * <p>When set to a positive value N, the bounds are [N, N] — every broker must have exactly N leaders.
 *
 * <p>This differs from {@link MinTopicLeadersPerBrokerGoal} which only enforces the floor.
 */
public class EvenTopicLeadersPerBrokerGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(EvenTopicLeadersPerBrokerGoal.class);
  private final String _replicaSortName = replicaSortName(this, true, false);
  private Map<String, Integer> _topicLeaderFloor;
  private Map<String, Integer> _topicLeaderCeiling;

  public EvenTopicLeadersPerBrokerGoal() {

  }

  /**
   * Package private for unit test.
   */
  EvenTopicLeadersPerBrokerGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new GoalUtils.HardGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
  }

  @Override
  public boolean isHardGoal() {
    return true;
  }

  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    if (!actionAffectsRelevantTopics(action)) {
      return ACCEPT;
    }
    switch (action.balancingAction()) {
      case LEADERSHIP_MOVEMENT:
      case INTER_BROKER_REPLICA_MOVEMENT:
        return acceptLeaderMovement(action, clusterModel);
      case INTER_BROKER_REPLICA_SWAP:
        Replica srcReplica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
        Replica dstReplica = clusterModel.broker(action.destinationBrokerId()).replica(action.destinationTopicPartition());
        return acceptReplicaSwap(srcReplica, dstReplica);
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  private ActionAcceptance acceptLeaderMovement(BalancingAction action, ClusterModel clusterModel) {
    Replica replicaToBeRemoved = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
    if (!replicaToBeRemoved.isLeader()) {
      return ACCEPT;
    }
    String topic = replicaToBeRemoved.topicPartition().topic();
    if (!_topicLeaderFloor.containsKey(topic)) {
      return ACCEPT;
    }

    int sourceCount = replicaToBeRemoved.broker().numLeadersFor(topic);
    if (sourceCount <= floorForTopic(topic)) {
      return REPLICA_REJECT;
    }

    Broker destBroker = clusterModel.broker(action.destinationBrokerId());
    if (destBroker.numLeadersFor(topic) >= ceilingForTopic(topic)) {
      return REPLICA_REJECT;
    }

    return ACCEPT;
  }

  private ActionAcceptance acceptReplicaSwap(Replica srcReplica, Replica dstReplica) {
    if (!srcReplica.isLeader() && !dstReplica.isLeader()) {
      return ACCEPT;
    }
    String srcTopic = srcReplica.topicPartition().topic();
    String dstTopic = dstReplica.topicPartition().topic();
    if (srcReplica.isLeader() && dstReplica.isLeader() && Objects.equals(srcTopic, dstTopic)) {
      return ACCEPT;
    }
    // Check floor violations (removing a leader would drop the source broker below the floor)
    if (wouldViolateFloor(srcReplica) || wouldViolateFloor(dstReplica)) {
      return REPLICA_REJECT;
    }
    // Check ceiling violations (gaining a leader would push the receiving broker above the ceiling)
    if (wouldViolateCeiling(srcReplica, dstReplica.broker()) || wouldViolateCeiling(dstReplica, srcReplica.broker())) {
      return REPLICA_REJECT;
    }
    return ACCEPT;
  }

  /**
   * Check whether removing this leader replica from its broker would drop the broker below the floor.
   */
  private boolean wouldViolateFloor(Replica replica) {
    if (!replica.isLeader()) {
      return false;
    }
    String topic = replica.topicPartition().topic();
    if (!_topicLeaderFloor.containsKey(topic)) {
      return false;
    }
    return replica.broker().numLeadersFor(topic) <= floorForTopic(topic);
  }

  /**
   * Check whether moving this leader replica to the receiving broker would push that broker above the ceiling.
   */
  private boolean wouldViolateCeiling(Replica replica, Broker receivingBroker) {
    if (!replica.isLeader()) {
      return false;
    }
    String topic = replica.topicPartition().topic();
    if (!_topicLeaderCeiling.containsKey(topic)) {
      return false;
    }
    return receivingBroker.numLeadersFor(topic) >= ceilingForTopic(topic);
  }

  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    Set<String> matchedTopics = Collections.unmodifiableSet(
        Utils.getTopicNamesMatchedWithPattern(_balancingConstraint.topicsWithMinLeadersPerBrokerPattern(), clusterModel::topics));

    _topicLeaderFloor = new HashMap<>();
    _topicLeaderCeiling = new HashMap<>();
    if (matchedTopics.isEmpty()) {
      return;
    }

    Map<String, Integer> numLeadersByTopic = clusterModel.numLeadersPerTopic(matchedTopics);
    Set<Broker> eligibleBrokers = eligibleBrokersForLeadership(clusterModel, optimizationOptions);
    int numEligible = eligibleBrokers.size();

    for (String topicName : matchedTopics) {
      int numLeaders = numLeadersByTopic.get(topicName);
      int configuredMin = _balancingConstraint.minTopicLeadersPerBroker();
      int floor;
      int ceiling;
      if (configuredMin == 0) {
        floor = numEligible == 0 ? 0 : numLeaders / numEligible;
        ceiling = numEligible == 0 ? 0 : (numLeaders + numEligible - 1) / numEligible;
      } else {
        floor = configuredMin;
        ceiling = configuredMin;
      }
      _topicLeaderFloor.put(topicName, floor);
      _topicLeaderCeiling.put(topicName, ceiling);
      LOG.info("Topic {} leader distribution bounds: floor={}, ceiling={} ({} leaders across {} brokers)",
               topicName, floor, ceiling, numLeaders, numEligible);
    }

    validateTopicsNotExcluded(optimizationOptions);
    validateEnoughLeaders(numLeadersByTopic, eligibleBrokers);
    validateBrokersAllowedReplicaMoveExist(clusterModel, optimizationOptions);

    boolean onlyMoveImmigrantReplicas = optimizationOptions.onlyMoveImmigrantReplicas();
    new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(), onlyMoveImmigrantReplicas)
                              .addSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnIncludedTopics(matchedTopics))
                              .maybeAddPriorityFunc(ReplicaSortFunctionFactory.prioritizeImmigrants(), !onlyMoveImmigrantReplicas)
                              .trackSortedReplicasFor(_replicaSortName, clusterModel);
  }

  private int floorForTopic(String topic) {
    return _topicLeaderFloor.get(topic);
  }

  private int ceilingForTopic(String topic) {
    return _topicLeaderCeiling.get(topic);
  }

  private void validateTopicsNotExcluded(OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    if (optimizationOptions.excludedTopics().isEmpty()) {
      return;
    }
    Set<String> wronglyExcluded = new HashSet<>();
    _topicLeaderFloor.keySet().forEach(topicName -> {
      if (optimizationOptions.excludedTopics().contains(topicName)) {
        wronglyExcluded.add(topicName);
      }
    });
    if (!wronglyExcluded.isEmpty()) {
      throw new OptimizationFailureException(String.format("[%s] Topics that must have even leader distribution cannot be excluded."
                                                           + " Topics should not be excluded=[%s] (see %s).",
                                                           name(), String.join(", ", wronglyExcluded),
                                                           AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG));
    }
  }

  private void validateEnoughLeaders(Map<String, Integer> numLeadersByTopic, Set<Broker> eligibleBrokers)
      throws OptimizationFailureException {
    for (Map.Entry<String, Integer> entry : numLeadersByTopic.entrySet()) {
      String topic = entry.getKey();
      int totalMinNeeded = eligibleBrokers.size() * floorForTopic(topic);
      if (entry.getValue() < totalMinNeeded) {
        ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
            .numPartitions(totalMinNeeded).topicPattern(Pattern.compile(topic)).build();
        throw new OptimizationFailureException(
            String.format("[%s] Cannot distribute %d leaders over %d broker(s) with floor=%d for topic %s.",
                          name(), entry.getValue(), eligibleBrokers.size(), floorForTopic(topic), topic), recommendation);
      }
    }
  }

  private void validateBrokersAllowedReplicaMoveExist(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    Set<Integer> brokersAllowedReplicaMove = GoalUtils.aliveBrokersNotExcludedForReplicaMove(clusterModel, optimizationOptions);
    if (brokersAllowedReplicaMove.isEmpty()) {
      ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
          .numBrokers(clusterModel.maxReplicationFactor()).build();
      throw new OptimizationFailureException(String.format("[%s] All alive brokers are excluded from replica moves.", name()), recommendation);
    }
  }

  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    Replica replicaToBeMoved = sourceBroker.replica(action.topicPartition());

    if (replicaToBeMoved.broker().replica(action.topicPartition()).isCurrentOffline()) {
      return action.balancingAction() == ActionType.INTER_BROKER_REPLICA_MOVEMENT;
    }

    if (!replicaToBeMoved.isLeader()) {
      return true;
    }

    String topic = replicaToBeMoved.topicPartition().topic();
    if (!_topicLeaderFloor.containsKey(topic)) {
      return true;
    }

    int sourceCount = sourceBroker.numLeadersFor(topic);

    if (sourceCount <= floorForTopic(topic)) {
      return false;
    }

    if (action.destinationBrokerId() != null) {
      Broker destBroker = clusterModel.broker(action.destinationBrokerId());
      int destCount = destBroker.numLeadersFor(topic);
      if (destCount >= ceilingForTopic(topic)) {
        return false;
      }
    }

    return true;
  }

  private boolean actionAffectsRelevantTopics(BalancingAction action) {
    if (_topicLeaderFloor.containsKey(action.topic())) {
      return true;
    }
    return action.balancingAction() == ActionType.INTER_BROKER_REPLICA_SWAP
            && _topicLeaderFloor.containsKey(action.destinationTopic());
  }

  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
    ensureLeaderDistributionWithinBounds(clusterModel, optimizationOptions);
    finish();
  }

  private void ensureLeaderDistributionWithinBounds(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    if (_topicLeaderFloor.isEmpty()) {
      return;
    }
    for (Broker broker : clusterModel.aliveBrokers()) {
      if (!isEligibleToHaveLeaders(broker, optimizationOptions)) {
        continue;
      }
      for (String topic : _topicLeaderFloor.keySet()) {
        int leaderCount = broker.numLeadersFor(topic);
        if (leaderCount < floorForTopic(topic)) {
          throw new OptimizationFailureException(String.format("[%s] Broker %d has too few leaders for topic %s"
                                                               + " (floor: %d, current: %d).",
                                                               name(), broker.id(), topic,
                                                               floorForTopic(topic), leaderCount));
        }
        if (leaderCount > ceilingForTopic(topic)) {
          throw new OptimizationFailureException(String.format("[%s] Broker %d has too many leaders for topic %s"
                                                               + " (ceiling: %d, current: %d).",
                                                               name(), broker.id(), topic,
                                                               ceilingForTopic(topic), leaderCount));
        }
      }
    }
  }

  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    LOG.debug("Balancing broker {}, optimized goals = {}", broker, optimizedGoals);
    moveAwayOfflineReplicas(broker, clusterModel, optimizedGoals, optimizationOptions);
    if (_topicLeaderFloor.isEmpty()) {
      return;
    }
    if (!(broker.isAlive() && isEligibleToHaveLeaders(broker, optimizationOptions))) {
      return;
    }
    for (String topic : _topicLeaderFloor.keySet()) {
      maybeMoveLeadersIn(topic, broker, clusterModel, optimizedGoals, optimizationOptions);
      maybeMoveLeadersOut(topic, broker, clusterModel, optimizedGoals, optimizationOptions);
    }
  }

  /**
   * Bring the broker up to the floor by pulling leaders in.
   */
  private void maybeMoveLeadersIn(String topic,
                                  Broker broker,
                                  ClusterModel clusterModel,
                                  Set<Goal> optimizedGoals,
                                  OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    int leaderCount = broker.numLeadersFor(topic);
    if (leaderCount >= floorForTopic(topic)) {
      return;
    }

    // Phase 1: try leadership elections from local follower replicas whose current leader's broker is above the floor
    List<Replica> followerReplicas = broker.trackedSortedReplicas(_replicaSortName)
                                           .sortedReplicas(false).stream()
                                           .filter(r -> !r.isLeader() && r.topicPartition().topic().equals(topic))
                                           .collect(Collectors.toList());

    for (Replica followerReplica : followerReplicas) {
      Replica leader = clusterModel.partition(followerReplica.topicPartition()).leader();
      if (leader.broker().numLeadersFor(topic) > floorForTopic(topic)) {
        if (maybeApplyBalancingAction(clusterModel, leader, Collections.singleton(broker),
                                      LEADERSHIP_MOVEMENT, optimizedGoals, optimizationOptions) != null) {
          leaderCount++;
          if (leaderCount >= floorForTopic(topic)) {
            return;
          }
        }
      }
    }

    // Phase 2: try inter-broker replica movement from brokers with excess leaders
    PriorityQueue<Broker> givers = brokersAboveFloor(topic, clusterModel, broker);

    while (!givers.isEmpty()) {
      Broker giver = givers.poll();
      List<Replica> leaders = giver.trackedSortedReplicas(_replicaSortName)
                                   .sortedReplicas(false).stream()
                                   .filter(r -> r.isLeader() && r.topicPartition().topic().equals(topic))
                                   .collect(Collectors.toList());
      boolean moved = false;
      int giverCount = leaders.size();
      for (Replica leaderReplica : leaders) {
        Broker dest = maybeApplyBalancingAction(clusterModel, leaderReplica, Collections.singleton(broker),
                                               INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions);
        if (dest != null) {
          moved = true;
          break;
        }
      }
      if (moved) {
        leaderCount++;
        if (leaderCount >= floorForTopic(topic)) {
          return;
        }
        giverCount--;
        if (giverCount > floorForTopic(topic)) {
          givers.add(giver);
        }
      }
    }
    throw new OptimizationFailureException(String.format("[%s] Cannot bring broker %d up to floor=%d leaders for topic %s (current: %d).",
                                                         name(), broker.id(), floorForTopic(topic), topic, broker.numLeadersFor(topic)));
  }

  /**
   * Bring the broker down to the ceiling by pushing leaders out.
   */
  private void maybeMoveLeadersOut(String topic,
                                   Broker broker,
                                   ClusterModel clusterModel,
                                   Set<Goal> optimizedGoals,
                                   OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    int leaderCount = broker.numLeadersFor(topic);
    if (leaderCount <= ceilingForTopic(topic)) {
      return;
    }

    // Phase 1: try leadership elections to follower brokers that are below the ceiling
    List<Replica> leaderReplicas = broker.trackedSortedReplicas(_replicaSortName)
                                         .sortedReplicas(false).stream()
                                         .filter(r -> r.isLeader() && r.topicPartition().topic().equals(topic))
                                         .collect(Collectors.toList());

    for (Replica leaderReplica : leaderReplicas) {
      Set<Broker> candidateBrokers = clusterModel.partition(leaderReplica.topicPartition()).partitionBrokers().stream()
          .filter(b -> b != broker
                       && isEligibleToHaveLeaders(b, optimizationOptions)
                       && b.numLeadersFor(topic) < ceilingForTopic(topic)
                       && !b.replica(leaderReplica.topicPartition()).isCurrentOffline())
          .collect(Collectors.toSet());

      if (!candidateBrokers.isEmpty()
          && maybeApplyBalancingAction(clusterModel, leaderReplica, candidateBrokers,
                                      LEADERSHIP_MOVEMENT, optimizedGoals, optimizationOptions) != null) {
        leaderCount--;
        if (leaderCount <= ceilingForTopic(topic)) {
          return;
        }
      }
    }

    // Phase 2: try inter-broker replica movement to brokers below the ceiling
    PriorityQueue<Broker> receivers = brokersBelowCeiling(topic, clusterModel, broker, optimizationOptions);
    // Re-fetch leader list after potential leadership movements above
    leaderReplicas = broker.trackedSortedReplicas(_replicaSortName)
                           .sortedReplicas(false).stream()
                           .filter(r -> r.isLeader() && r.topicPartition().topic().equals(topic))
                           .collect(Collectors.toList());

    for (Replica leaderReplica : leaderReplicas) {
      if (leaderCount <= ceilingForTopic(topic)) {
        return;
      }
      Broker dest = maybeApplyBalancingAction(clusterModel, leaderReplica, receivers,
                                             INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions);
      if (dest != null) {
        leaderCount--;
        // Re-check if the receiver is still below the ceiling
        if (dest.numLeadersFor(topic) >= ceilingForTopic(topic)) {
          receivers.remove(dest);
        }
      }
    }

    if (leaderCount > ceilingForTopic(topic)) {
      throw new OptimizationFailureException(
          String.format("[%s] Cannot bring broker %d down to ceiling=%d leaders for topic %s (current: %d).",
                        name(), broker.id(), ceilingForTopic(topic), topic, broker.numLeadersFor(topic)));
    }
  }

  private PriorityQueue<Broker> brokersAboveFloor(String topic, ClusterModel clusterModel, Broker excludeBroker) {
    PriorityQueue<Broker> queue = new PriorityQueue<>((b1, b2) -> {
      int cmp = Integer.compare(b2.numLeadersFor(topic), b1.numLeadersFor(topic));
      return cmp == 0 ? Integer.compare(b1.id(), b2.id()) : cmp;
    });
    clusterModel.aliveBrokers().stream()
                .filter(b -> b != excludeBroker && b.numLeadersFor(topic) > floorForTopic(topic))
                .forEach(queue::add);
    return queue;
  }

  private PriorityQueue<Broker> brokersBelowCeiling(String topic, ClusterModel clusterModel,
                                                    Broker excludeBroker, OptimizationOptions optimizationOptions) {
    PriorityQueue<Broker> queue = new PriorityQueue<>((b1, b2) -> {
      int cmp = Integer.compare(b1.numLeadersFor(topic), b2.numLeadersFor(topic));
      return cmp == 0 ? Integer.compare(b1.id(), b2.id()) : cmp;
    });
    clusterModel.aliveBrokers().stream()
                .filter(b -> b != excludeBroker
                             && isEligibleToHaveLeaders(b, optimizationOptions)
                             && b.numLeadersFor(topic) < ceilingForTopic(topic))
                .forEach(queue::add);
    return queue;
  }

  private static Set<Broker> eligibleBrokersForLeadership(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    return clusterModel.aliveBrokers()
                       .stream()
                       .filter(broker -> isEligibleToHaveLeaders(broker, optimizationOptions))
                       .collect(Collectors.toSet());
  }

  private static boolean isEligibleToHaveLeaders(Broker broker, OptimizationOptions optimizationOptions) {
    return !optimizationOptions.excludedBrokersForLeadership().contains(broker.id())
           && !optimizationOptions.excludedBrokersForReplicaMove().contains(broker.id());
  }

  private void moveAwayOfflineReplicas(Broker srcBroker,
                                       ClusterModel clusterModel,
                                       Set<Goal> optimizedGoals,
                                       OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    if (srcBroker.currentOfflineReplicas().isEmpty()) {
      return;
    }
    SortedSet<Broker> eligibleBrokersToMoveOfflineReplicasTo = new TreeSet<>(
        Comparator.comparingInt((Broker broker) -> broker.replicas().size()).thenComparingInt(Broker::id));
    eligibleBrokersToMoveOfflineReplicasTo.addAll(clusterModel.aliveBrokers());
    Set<Replica> offlineReplicas = new HashSet<>(srcBroker.currentOfflineReplicas());
    for (Replica offlineReplica : offlineReplicas) {
      if (maybeApplyBalancingAction(clusterModel, offlineReplica, eligibleBrokersToMoveOfflineReplicasTo,
                                    INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions) == null) {
        ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(1).build();
        throw new OptimizationFailureException(String.format("[%s] Cannot remove %s from %s broker %d (has %d replicas).", name(),
                                                             offlineReplica, srcBroker.state(), srcBroker.id(), srcBroker.replicas().size()),
                                               recommendation);
      }
    }
  }
}
