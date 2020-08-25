package org.myPaper.datacenter.vmAllocationPolicies;

import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationAbstract;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.acsAlgorithms.DatacenterSolutionEntry;
import org.myPaper.acsAlgorithms.OurAcsAlgorithm.KneePointSelectionPolicy;
import org.myPaper.acsAlgorithms.OurAcsAlgorithm.OurAcs;
import org.myPaper.broker.DatacenterBrokerOurAcs;
import org.myPaper.datacenter.DatacenterPro;
import org.myPaper.datacenter.VmSelectionPolicy.VmSelectionPolicyMaximumCpuUtilization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class VmAllocationPolicyMigrationStaticThresholdOurAcs
    extends VmAllocationPolicyMigrationAbstract
    implements VmAllocationPolicyMigrationStaticThresholdAcsBased {
    protected static final Logger LOGGER = LoggerFactory.getLogger(VmAllocationPolicyMigrationStaticThresholdOurAcs.class.getSimpleName());

    /**
     * @see #getOverUtilizationThreshold(Host)
     */
    protected static final double DEF_OVER_UTILIZATION_THRESHOLD = 0.9;

    /**
     * @see #getOverUtilizationThreshold(Host)
     * @see #setOverUtilizationThreshold(double)
     */
    protected double overUtilizationThreshold = DEF_OVER_UTILIZATION_THRESHOLD;

    private final OurAcs OUR_ACS;

    public VmAllocationPolicyMigrationStaticThresholdOurAcs(OurAcs ourAcs) {
        super(new VmSelectionPolicyMaximumCpuUtilization());

        OUR_ACS = ourAcs;
    }

    @Override
    protected Optional<Host> defaultFindHostForVm(Vm vm) {
        if (vm.getHost() != Host.NULL) {

            vm.getHost().destroyTemporaryVm(vm);

            if (vm.getHost().isSuitableForVm(vm)) {
                return Optional.of(vm.getHost());
            } else {
                vm.setHost(Host.NULL);
                return Optional.of(Host.NULL);
            }
        }

        return findSuitableHostForVm(vm);
    }

    @Override
    public Map<Vm, Host> getOptimizedAllocationMap(final List<? extends Vm> vmList) {
        List<Host> overloadedHosts = getOverloadedHosts();
        List<Host> underloadedHost = getUnderloadedHosts(overloadedHosts);
        List<Vm> vmMigrationList = getVmMigrationListOfOverloadedAndUnderloadedHosts(overloadedHosts, underloadedHost);

        if (vmMigrationList.isEmpty()) {
            return Collections.emptyMap();
        }

        Optional<Map<Vm, Host>> migrationMap = getMigrationMap(vmMigrationList);

        if ((!migrationMap.isPresent() || migrationMap.get().isEmpty()) && isCloudFederationActive()) {
            migrationMap = Optional.of(requestExternalVmMigrationSolution(vmMigrationList));
        }

        return migrationMap.orElse(Collections.emptyMap());
    }

    public Optional<Host> findSuitableHostForVm(final Vm vm) {
        List<Vm> vmList = new ArrayList<>();
        vmList.add(vm);

        Optional<Map<Vm, Host>> solution = findSolutionForVms(vmList, getHostList());

        return solution.map(vmHostMap -> vmHostMap.get(vm));
    }

    @Override
    public Optional<Map<Vm, Host>> findSolutionForVms(final List<Vm> vmList, List<Host> allowedHostList) {
        if (vmList.isEmpty()) {
            return Optional.empty();
        }

        Optional<Map<Vm, Host>> solution = OUR_ACS.getBestSolution(vmList, getDatacenterPro(), allowedHostList);

        if (solution.isPresent() && !solution.get().isEmpty()) {
            LOGGER.info("{}: {} found a solution for the requested Vm list.",
                getDatacenter().getSimulation().clockStr(),
                getDatacenter());
        } else {
            LOGGER.warn("{}: {} could not find any solution for the requested Vm list!",
                getDatacenter().getSimulation().clockStr(),
                getDatacenter());
        }

        return solution;
    }

    /**
     * Gets a new migration map for the given VM list.
     * Note that it filters the VMs with the destination hosts that are already created in.
     *
     * @param vmList list of VMs which should be migrated out
     * @return the migration map
     */
    private Optional<Map<Vm, Host>> getMigrationMap(final List<Vm> vmList) {
        if (vmList.isEmpty()) {
            return Optional.empty();
        }

        Optional<Map<Vm, Host>> solutionMap = findSolutionForVms(vmList, getHostList());

        if (solutionMap.isPresent()) {
            Map<Vm, Host> solution = solutionMap.get();

            solution.entrySet().stream()
                .filter(vmHostEntry -> vmHostEntry.getKey().getHost() == vmHostEntry.getValue())
                .forEach(vmHostEntry -> {
                    LOGGER.info("{}: {}: According to the given solution, {} does not have any live Vm migration and will stay on {}.",
                        getDatacenter().getSimulation().clockStr(),
                        getDatacenter(),
                        vmHostEntry.getKey(),
                        vmHostEntry.getValue());
                });

            Map<Vm, Host> migrationMap = getSolutionMigrationMap(solution);

            return Optional.of(sortMigrationMap(migrationMap));
        }

        return Optional.empty();
    }

    /**
     * Sorts the given migration map in order to avoid any migration overlap. Note that it does not consider VM migrations that
     * are in loop.
     *
     * @param migrationMap the migration map
     * @return sorted migration map
     */
    private Map<Vm, Host> sortMigrationMap(final Map<Vm, Host> migrationMap) {
        Map<Host, List<Vm>> hostVmListMap = new HashMap<>();
        migrationMap.forEach((vm, host) -> {
            List<Vm> vmList = host.getVmList();
            hostVmListMap.putIfAbsent(host, new ArrayList<>(vmList));
        });

        Map<Vm, Host> feasibleMigrationMap = new LinkedHashMap<>();
        Map<Vm, Host> notFeasibleMigrationMap = new HashMap<>();

        for (Map.Entry<Vm, Host> vmHostEntry : migrationMap.entrySet()) {
            Vm sourceVm = vmHostEntry.getKey();
            Host sourceHost = sourceVm.getHost();
            Host targetHost = vmHostEntry.getValue();

            if (isHostSuitableForVm(sourceVm, targetHost, hostVmListMap.get(targetHost))) {
                feasibleMigrationMap.put(sourceVm, targetHost);

                //deallocating the VM from source targetHost
                if (hostVmListMap.containsKey(sourceHost)) {
                    hostVmListMap.get(sourceHost).remove(sourceVm);
                }

                //allocating resources of the destination targetHost to the VM
                hostVmListMap.get(targetHost).add(sourceVm);

                LOGGER.info("{}: {}: {} is going to be migrated from {} to {}.",
                    getDatacenter().getSimulation().clockStr(),
                    getDatacenter(),
                    sourceVm,
                    sourceHost,
                    targetHost);
            } else {
                notFeasibleMigrationMap.put(sourceVm, targetHost);

                LOGGER.warn("{}: {}: the migration of {} to {} is detected as an infeasible migration plan and " +
                        "the data center will try to make it feasible.",
                    getDatacenter().getSimulation().clockStr(),
                    getDatacenter(),
                    sourceVm,
                    targetHost);
            }
        }

        while (!notFeasibleMigrationMap.isEmpty()) {
            Map<Vm, Host> newFeasibleMigration = new HashMap<>();

            for (Map.Entry<Vm, Host> vmHostEntry : notFeasibleMigrationMap.entrySet()) {
                Vm sourceVm = vmHostEntry.getKey();
                Host sourceHost = sourceVm.getHost();
                Host targetHost = vmHostEntry.getValue();

                if (isHostSuitableForVm(sourceVm, targetHost, hostVmListMap.get(targetHost))) {
                    feasibleMigrationMap.put(sourceVm, targetHost);
                    newFeasibleMigration.put(sourceVm, targetHost);

                    //deallocating the VM from source targetHost
                    if (hostVmListMap.containsKey(sourceHost)) {
                        hostVmListMap.get(sourceHost).remove(sourceVm);
                    }

                    //allocating resources of the destination targetHost to the VM
                    hostVmListMap.get(targetHost).add(sourceVm);

                    LOGGER.info("{}: {}: the migration of {} to {} is able to become feasible after some other Vm migrations. " +
                            "It might not be able to do the migration at the first try (It will be queued).",
                        getDatacenter().getSimulation().clockStr(),
                        getDatacenter(),
                        sourceVm,
                        targetHost);
                }
            }

            if (newFeasibleMigration.isEmpty()) {
                LOGGER.warn("{}: {}: some VMs ({}) are not able to migrate out due to the migration lock-in status!",
                    getDatacenter().getSimulation().clockStr(),
                    getDatacenter(),
                    notFeasibleMigrationMap.keySet().toString());

                notFeasibleMigrationMap.forEach((sourceVm, targetHost) -> {
                    Host sourceHost = sourceVm.getHost();
                    LOGGER.warn("{}: {} wants to migrate from {} to {} while the destination host does not have enough amount of resources.",
                        getDatacenter().getSimulation().clockStr(),
                        sourceVm,
                        sourceHost,
                        targetHost);
                });

                //Try to make the hole of the solution feasible
                Map<Vm, Host> newMigrationMap = closeSolutionFromVmMigrationLockInStatus(migrationMap);

                if (!migrationMap.equals(newMigrationMap)) {
                    LOGGER.info("{}: {}: the migration lock-in solver solves the migration lock-in status successfully. " +
                            "The migration map is going to be resorted again.",
                        getDatacenter().getSimulation().clockStr(),
                        getDatacenter().getName());

                    return sortMigrationMap(newMigrationMap);
                } else {
                    LOGGER.warn("{}: {}: the migration lock-in solver couldn't solve the migration lock-in status!",
                        getDatacenter().getSimulation().clockStr(),
                        getDatacenter().getName());
                }

                break;
            } else {
                newFeasibleMigration.keySet().forEach(notFeasibleMigrationMap::remove);
            }
        }

        return feasibleMigrationMap;
    }

    private boolean isHostSuitableForVm(Vm vm, Host host, List<Vm> hostVmList) {
        int availablePes = (int) host.getNumberOfPes() - hostVmList.parallelStream()
            .mapToInt(hostVm -> (int) hostVm.getNumberOfPes())
            .sum() - (int) vm.getNumberOfPes();

        int availableMemory = (int) host.getRam().getCapacity() - hostVmList.parallelStream()
            .mapToInt(hostVm -> (int) hostVm.getRam().getCapacity())
            .sum() - (int) vm.getRam().getCapacity();

        int availableStorage = (int) host.getStorage().getCapacity() - hostVmList.parallelStream()
            .mapToInt(hostVm -> (int) hostVm.getStorage().getCapacity())
            .sum() - (int) vm.getStorage().getCapacity();

        int availableBw = (int) host.getBw().getCapacity() - hostVmList.parallelStream()
            .mapToInt(hostVm -> (int) hostVm.getBw().getCapacity())
            .sum() - (int) vm.getBw().getCapacity();

        return availablePes >= 0 && availableMemory >= 0 && availableStorage >= 0 && availableBw >= 0;
    }

    /**
     * Gets the migration map of the given solution
     *
     * @param solution the target solution
     * @return the migration map
     */
    public Map<Vm, Host> getSolutionMigrationMap(final Map<Vm, Host> solution) {
        return solution.entrySet().parallelStream()
            .filter(vmHostEntry -> vmHostEntry.getKey().isCreated())
            .filter(vmHostEntry -> vmHostEntry.getKey().getHost() != vmHostEntry.getValue())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Gets the VM migration list of overloaded and underloaded hosts according to the OurAcs 2017 paper VM selection policy.
     *
     * @param overloadedHostList  list of overloaded hosts
     * @param underloadedHostList list of underloaded hosts
     * @return the list of VMs which should be migrated
     */
    private List<Vm> getVmMigrationListOfOverloadedAndUnderloadedHosts(List<Host> overloadedHostList, List<Host> underloadedHostList) {
        List<Vm> totalVmList = new ArrayList<>();

        overloadedHostList.forEach(overloadedHost -> {
            LOGGER.info("{}: {} is detected as an overloaded host during the migration checkup process.",
                getDatacenter().getSimulation().clockStr(),
                overloadedHost);

            List<Vm> overloadedHostVmList = getVmsToMigrateFromOverloadedHost(overloadedHost);

            if (!overloadedHostVmList.isEmpty()) {
                totalVmList.addAll(overloadedHostVmList);

                LOGGER.info("{}: {}: The following Vms are selected for migrating out from overloaded {}: {}",
                    getDatacenter().getSimulation().clockStr(),
                    getDatacenter(),
                    overloadedHost,
                    overloadedHostVmList);
            } else {
                LOGGER.warn("{}: {}: No suitable Vm was found for migration out from overloaded {}!",
                    getDatacenter().getSimulation().clockStr(),
                    getDatacenter(),
                    overloadedHost);
            }

        });

        for (Host underloadedHost : underloadedHostList) {

            LOGGER.info("{}: {} is detected as an underloaded host during the migration checkup process.",
                getDatacenter().getSimulation().clockStr(),
                underloadedHost);

            List<Vm> underloadedHostVmList = underloadedHost.getVmList().parallelStream()
                .filter(Vm::isCreated)
                .filter(vm -> !vm.getCloudletScheduler().getCloudletExecList().isEmpty())
                .collect(Collectors.toList());
            if (underloadedHostVmList.size() == underloadedHost.getVmList().size()) {
                totalVmList.addAll(underloadedHostVmList);

                LOGGER.info("{}: {}: The following Vms are selected for migrating out from underloaded {}: {}",
                    getDatacenter().getSimulation().clockStr(),
                    getDatacenter(),
                    underloadedHost,
                    underloadedHostVmList.toString());
            } else {
                LOGGER.warn("{}: {}: The underloaded {} is not ready to have any Vm migration!",
                    getDatacenter().getSimulation().clockStr(),
                    getDatacenter(),
                    underloadedHost);
            }
        }

        return totalVmList;
    }

    /**
     * Gets the List of overloaded hosts.
     * If a Host is overloaded but it has VMs migrating out,
     * then it's not included in the returned List
     * because the VMs to be migrated to move the Host from
     * the overload state already are in migration.
     *
     * @return the over utilized hosts
     */
    private List<Host> getOverloadedHosts() {
        return getHostList().parallelStream()
            .filter(Host::isActive)
            .filter(this::isHostOverloaded)
            .filter(host -> host.getVmsMigratingOut().isEmpty())
            .collect(Collectors.toList());
    }

    /**
     * Gets the list of Vms to migrate from the given over-loaded host.
     *
     * @param host the host
     * @return the list of Vms for migrating out
     */
    private List<Vm> getVmsToMigrateFromOverloadedHost(final Host host) {
        if (!isHostOverloaded(host) || host.getVmList().isEmpty()) {
            return Collections.emptyList();
        }

        VmSelectionPolicyMaximumCpuUtilization vmSelectionPolicy =
            (VmSelectionPolicyMaximumCpuUtilization) getVmSelectionPolicy();

        List<Vm> temporaryVmList = new ArrayList<>(host.getVmList());
        List<Vm> selectedVmList = new ArrayList<>();

        do {
            Vm selectedVm = vmSelectionPolicy.getVmToMigrate(temporaryVmList);

            if (selectedVm == Vm.NULL) {
                break;
            }

            temporaryVmList.remove(selectedVm);

            if (!selectedVm.getCloudletScheduler().getCloudletExecList().isEmpty()) {
                selectedVmList.add(selectedVm);
            }

        } while (!temporaryVmList.isEmpty() && isHostOverloaded(host, temporaryVmList));

        if (!temporaryVmList.isEmpty() && isHostOverloaded(host, temporaryVmList)) {
            return Collections.emptyList();
        }

        return selectedVmList;
    }

    /**
     * Gets the List of underloaded hosts.
     * If a host is VM free or an overloaded one, it is not considered as an underloaded host.
     *
     * @param overloadedHostList the list of overloaded hosts
     * @return list of underloaded hosts
     */
    private List<Host> getUnderloadedHosts(List<Host> overloadedHostList) {

        return getHostList().parallelStream()
            .filter(Host::isActive)
            .filter(host -> !overloadedHostList.contains(host))
            .filter(host -> !host.getVmList().isEmpty())
            .filter(host -> host.getVmsMigratingOut().isEmpty())
            .filter(this::isHostUnderloaded)
            .collect(Collectors.toList());
    }

    @Override
    public boolean isHostOverloaded(Host host) {
        return host.getCpuPercentUtilization() > getOverUtilizationThreshold();
    }

    public boolean isHostOverloaded(Host host, List<Vm> hostVmList) {
        if (host == Host.NULL) {
            throw new IllegalStateException("The host object could not be null!");
        }

        if (hostVmList.isEmpty()) {
            throw new IllegalStateException("The host Vm list could not be empty!");
        }

        double totalVmsMipsUtilization = hostVmList.stream()
            .mapToDouble(Vm::getTotalCpuMipsUtilization)
            .sum();

        double hostCpuUtilization = totalVmsMipsUtilization / host.getTotalMipsCapacity();

        return hostCpuUtilization > getOverUtilizationThreshold();
    }

    @Override
    public boolean isHostUnderloaded(final Host host) {
        return host.getCpuPercentUtilization() <= getUnderUtilizationThreshold();
    }

    public boolean isHostUnderloaded(final Host host, final List<Vm> hostVmList) {
        if (host == Host.NULL) {
            throw new IllegalStateException("The host object could not be null!");
        }

        if (hostVmList.isEmpty()) {
            throw new IllegalStateException("The host Vm list could not be empty!");
        }

        double totalVmsMipsUtilization = hostVmList.stream()
            .mapToDouble(Vm::getTotalCpuMipsUtilization)
            .sum();

        double hostCpuUtilization = totalVmsMipsUtilization / host.getTotalMipsCapacity();

        return hostCpuUtilization <= getUnderUtilizationThreshold();
    }

    @Override
    public double getOverUtilizationThreshold(Host host) {
        return overUtilizationThreshold;
    }

    public double getOverUtilizationThreshold() {
        return overUtilizationThreshold;
    }

    /**
     * Sets the hosts over-utilization threshold. It is a percentage value from 0 to 1.
     *
     * @param threshold the over-utilization threshold
     */
    public void setOverUtilizationThreshold(final double threshold) {
        if (threshold <= 0 || threshold > 1) {
            throw new IllegalStateException("The over-utilization threshold must be in range (0, 1]!");
        }

        overUtilizationThreshold = threshold;
    }

    @Override
    public boolean areHostsUnderOrOverloaded() {
        for (Host host : getHostList()) {
            if (host.isActive() && !host.getVmList().isEmpty()) {
                if (host.getCpuPercentUtilization() > getOverUtilizationThreshold() ||
                    host.getCpuPercentUtilization() <= getUnderUtilizationThreshold()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Requests the cloud broker to find some migration solutions from the entire of cloud federation environment.
     *
     * @param vmList the list of Vms
     */
    private Map<Vm, Host> requestExternalVmMigrationSolution(final List<Vm> vmList) {
        if (!isCloudFederationActive()) {
            return Collections.emptyMap();
        }

        DatacenterBrokerOurAcs datacenterBrokerOurAcs =
            (DatacenterBrokerOurAcs) getDatacenterPro().getCloudCoordinator().getCloudBroker();

        List<DatacenterSolutionEntry> solutionMapList =
            datacenterBrokerOurAcs.getMigrationSolutionMapList(getDatacenter(), vmList, true);

        if (!solutionMapList.isEmpty()) {

            Map<Vm, Host> externalMigrationMap;
            KneePointSelectionPolicy kneePointSelectionPolicy = new KneePointSelectionPolicy(vmList);
            externalMigrationMap = kneePointSelectionPolicy.getKneePoint(solutionMapList, true);

            return externalMigrationMap;
        }

        return Collections.emptyMap();
    }

    /**
     * Checks if any external cloud broker is available for this cloud provider or not.
     *
     * @return true if external brokers are available, false otherwise
     */
    private boolean isCloudFederationActive() {
        if (getDatacenterPro().getCloudCoordinator() == null) {
            return false;
        } else {
            return !getDatacenterPro().getCloudCoordinator().getExternalBrokerList().isEmpty();
        }
    }

    /**
     * Gets the Datacenter pro
     *
     * @return the datacenter pro
     */
    private DatacenterPro getDatacenterPro() {
        return (DatacenterPro) getDatacenter();
    }

    /**
     * Closes the given migration map solution from the migration lock-in status and return new migration map solution.
     * Note that if the algorithm could not find any suitable host for closing the given solution from the migration map,
     * the original solution will be returned.
     *
     * @param migrationMap the migration map
     * @return a new solution if possible
     */
    private Map<Vm, Host> closeSolutionFromVmMigrationLockInStatus(final Map<Vm, Host> migrationMap) {
        LOGGER.info("{}: {}: is calling the migration lock-in solver algorithm to solve the migration loops.",
            getDatacenter().getSimulation().clockStr(),
            getDatacenter().getName());

        List<Map<Vm, Host>> migrationLoopMapList = detectLoopsInMigrationLockInSolution(migrationMap);

        if (migrationLoopMapList.isEmpty()) {
            //No loop was found in the given migration map
            LOGGER.warn("{}: {}: not any loop was found by the lock-in solver for the migration map!",
                getDatacenter().getSimulation().clockStr(),
                getDatacenter().getName());

            return migrationMap;
        }

        LOGGER.info("{}: {}: the following loops were found by the lock-in solver from the migration map: {}",
            getDatacenter().getSimulation().clockStr(),
            getDatacenter().getName(),
            migrationLoopMapList.toString());

        //Make a shallow copy from the original migration map
        Map<Vm, Host> newMigrationMap = new LinkedHashMap<>(migrationMap);

        for (Map<Vm, Host> migrationLoopEntry : migrationLoopMapList) {
            Map<Vm, Host> currentLoopMap = new HashMap<>(migrationLoopEntry);
            Map<Host, List<Vm>> hostMigratingOutVmMap = new HashMap<>();
            migrationLoopEntry.forEach((vm, host) -> {
                hostMigratingOutVmMap.putIfAbsent(vm.getHost(), new ArrayList<>());
                hostMigratingOutVmMap.get(vm.getHost()).add(vm);
            });

            //Sorting the loop host according to the list of their migrating out Vms in ascending order
            List<Host> loopSortedHostList = hostMigratingOutVmMap.entrySet().stream()
                .sorted(Comparator.comparing(hostListEntry -> hostListEntry.getValue().size()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

            loopHostIteration:
            for (Host hostInLoop : loopSortedHostList) {
                //Sort host's migrating out Vm list according to Vm's number of Pes
                List<Vm> loopHostSortedVmList = hostMigratingOutVmMap.get(hostInLoop).parallelStream()
                    .sorted(Comparator.comparing(Vm::getNumberOfPes))
                    .collect(Collectors.toList());

                List<Host> currentSolutionDestinationHostList = migrationMap.values().stream()
                    .distinct()
                    .collect(Collectors.toList());

                List<Host> currentSolutionSourceHostList = migrationMap.keySet().stream()
                    .map(Vm::getHost)
                    .filter(host -> !currentSolutionDestinationHostList.contains(host))
                    .distinct()
                    .collect(Collectors.toList());

                List<Host> concatList = new ArrayList<>();
                concatList.addAll(currentSolutionDestinationHostList);
                concatList.addAll(currentSolutionSourceHostList);

                //List of sorted hosts at current migration map
                List<Host> currentSolutionTotalHostList = concatList.parallelStream()
                    .sorted(Comparator.comparing(Host::getCpuMipsUtilization).reversed())
                    .collect(Collectors.toList());

                //List of sorted hosts that has not been used in the current migration map yet
                List<Host> notTriedHostList = getHostList().parallelStream()
                    .filter(host -> !currentSolutionTotalHostList.contains(host))
                    .filter(host -> host.getFreePesNumber() > 0)
                    .sorted(Comparator.comparing(Host::getCpuMipsUtilization).reversed())
                    .collect(Collectors.toList());

                loopVmIteration:
                for (Vm vmInLoop : loopHostSortedVmList) {
                    //Try the hosts that exist in the current migration map
                    for (Host host : currentSolutionTotalHostList) {

                        //Check if the Vm is going to be powered off
                        boolean allVmsWillBeMigratedAndNoIncomingVm = host.getVmList().parallelStream()
                            .allMatch(newMigrationMap::containsKey) && !newMigrationMap.containsValue(host);

                        if (host == vmInLoop.getHost() || host.getFreePesNumber() == 0 || allVmsWillBeMigratedAndNoIncomingVm) {
                            continue ;
                        }

                        //In order to avoid from other new loops, we do not consider the migrating of host's Vms
                        //In other words, we consider the occupied space by Vms which are chosen from this host for migrating out to another host
                        List<Vm> newVmList = getHostNewVmList(host, newMigrationMap, false);

                        if (isHostSuitableForVm(vmInLoop, host, newVmList)) {
                            newVmList.add(vmInLoop);

                            if (isHostOverloaded(host, newVmList)) {
                                continue ;
                            }

                            newMigrationMap.replace(vmInLoop, host);
                            currentLoopMap.replace(vmInLoop, host);

                            if (isMigrationMapFeasible(currentLoopMap)) {
                                break loopHostIteration;
                            } else {
                                continue loopVmIteration;
                            }
                        }
                    }

                    //Try the hosts that do not exist in the current migration map
                    /*for (Host host : notTriedHostList) {
                        List<Vm> newVmList = getHostNewVmList(host, newMigrationMap, true);

                        if (isHostSuitableForVm(vmInLoop, host, newVmList)) {
                            newVmList.add(vmInLoop);

                            if (isHostOverloaded(host, newVmList)) {
                                continue;
                            }

                            newMigrationMap.replace(vmInLoop, host);
                            currentLoopMap.replace(vmInLoop, host);

                            if (isMigrationMapFeasible(currentLoopMap)) {
                                break loopHostIteration;
                            } else {
                                continue loopVmIteration;
                            }
                        }
                    }*/
                }
            }
        }

        if (isMigrationMapFeasible(newMigrationMap)) {
            return newMigrationMap;
        } else {
            return migrationMap;
        }
    }

    /**
     * Gets the new Vm list of the given host according to the given solution map.
     *
     * @param host     the host
     * @param solution the solution map
     * @param considerVmsMigratingOut set true if current temporary list of migrating out Vms must be considered (do not consider their occupied space)
     * @return the new Vm list
     */
    private List<Vm> getHostNewVmList(final Host host, final Map<Vm, Host> solution, boolean considerVmsMigratingOut) {

        List<Vm> vmList = host.getVmList().parallelStream()
            .filter(vm -> !solution.containsKey(vm) || !considerVmsMigratingOut)
            .collect(Collectors.toList());

        List<Vm> hostTemporaryVmList = solution.entrySet().parallelStream()
            .filter(vmHostEntry -> !vmList.contains(vmHostEntry.getKey()))
            .filter(vmHostEntry -> vmHostEntry.getValue() == host)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

        vmList.addAll(hostTemporaryVmList);

        return vmList;
    }

    /**
     * Detects the infeasible loops of the given migration lock-in map. Note that if no loop exist in the given map, an empty list
     * will be returned.
     *
     * @param migrationLockInMap the migration map that all of its Vms are in the migration lock-in status
     * @return list of infeasible loop in migration maps that are in the migration lock-in status
     */
    private List<Map<Vm, Host>> detectLoopsInMigrationLockInSolution(final Map<Vm, Host> migrationLockInMap) {
        if (migrationLockInMap.isEmpty()) {
            throw new IllegalStateException("The migration lock-in map could not be empty!");
        }

        Map<Vm, Host> newMigrationLockInMap = new HashMap<>(migrationLockInMap);
        List<Map<Vm, Host>> migrationLockInMapList = new ArrayList<>();

        for (Map.Entry<Vm, Host> vmHostEntry : migrationLockInMap.entrySet()) {
            Map<Vm, Host> temporaryLoopMap = new HashMap<>();

            Vm sourceVm = vmHostEntry.getKey();
            Host sourceHost = sourceVm.getHost();
            Host destinationHost = vmHostEntry.getValue();

            if (!newMigrationLockInMap.containsKey(sourceVm)) {
                continue;
            }

            temporaryLoopMap.put(sourceVm, destinationHost);
            newMigrationLockInMap.remove(sourceVm);

            Host nextDestination = destinationHost;
            List<Host> migrationPath = new ArrayList<>();
            List<Host> ignoredDestinationHostList = new ArrayList<>();
            List<Host> notTriedDestinationList = new ArrayList<>();

            migrationPath.add(sourceHost);

            boolean infeasibleLoopIsDetected = false;
            while (true) {

                Host finalNextDestination = nextDestination;
                //Find migration plans that their source current host is the destination of nextDestination
                Map<Vm, Host> connectedMigrationPlans = newMigrationLockInMap.entrySet().parallelStream()
                    .filter(comparativeVmHostEntry -> comparativeVmHostEntry.getKey().getHost() == finalNextDestination)
                    .filter(comparativeVmHostEntry ->
                        !migrationPath.contains(comparativeVmHostEntry.getKey().getHost()) || comparativeVmHostEntry.getKey().getHost() == sourceHost)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                Host previousDestination = nextDestination;
                migrationPath.add(previousDestination);

                //Find connected destination hosts
                List<Host> possibleDestinationHosts = connectedMigrationPlans.values().parallelStream().distinct()
                    .filter(host -> !ignoredDestinationHostList.contains(host))
                    .filter(host -> !migrationPath.contains(host) || sourceHost == host)
                    .collect(Collectors.toList());

                //Choose one of the possible destinations
                nextDestination = possibleDestinationHosts.parallelStream()
                    .findFirst()
                    .orElse(Host.NULL);

                if (nextDestination != Host.NULL && possibleDestinationHosts.size() > 1) {
                    //Add not tried destinations to not tried destination list
                    Host finalNextDestination1 = nextDestination;
                    List<Host> remainingHostList = possibleDestinationHosts.parallelStream()
                        .filter(notTriedDestination -> !notTriedDestinationList.contains(notTriedDestination))
                        .filter(notTriedDestination -> notTriedDestination != finalNextDestination1)
                        .collect(Collectors.toList());

                    notTriedDestinationList.addAll(remainingHostList);
                }

                if (nextDestination == Host.NULL) {
                    if (!ignoredDestinationHostList.contains(previousDestination)) {
                        //Add the previous destination host to the list of ignored hosts in order to be checked again

                        ignoredDestinationHostList.add(previousDestination);
                    }

                    //Remove previous destination from not tried destination list if exist
                    notTriedDestinationList.remove(previousDestination);

                    if (notTriedDestinationList.isEmpty()) {
                        //Break from while loop because the source node does not involve in any loop
                        break;
                    } else {
                        //Reset the temporary loop map and the migration path if any not tried destination is available at not tried destination list

                        nextDestination = destinationHost;
                        temporaryLoopMap.clear();
                        temporaryLoopMap.put(sourceVm, destinationHost);
                        migrationPath.clear();
                        migrationPath.add(sourceHost);

                        continue;
                    }
                }

                notTriedDestinationList.remove(previousDestination);

                Host finalNextDestination2 = nextDestination;
                Map<Vm, Host> nextNodes = connectedMigrationPlans.entrySet().parallelStream()
                    .filter(vmHostEntry1 -> vmHostEntry1.getValue() == finalNextDestination2)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                temporaryLoopMap.putAll(nextNodes);

                if (sourceHost == nextDestination) {
                    //Select the loop if it is an infeasible migration map
                    if (!isMigrationMapFeasible(temporaryLoopMap)) {
                        migrationLockInMapList.add(temporaryLoopMap);
                        infeasibleLoopIsDetected = true;
                    }

                    break;
                }
            }

            if (infeasibleLoopIsDetected) {
                temporaryLoopMap.forEach(newMigrationLockInMap::remove);
            }
        }

        return migrationLockInMapList;
    }

    /**
     * Checks if the given migration map is feasible or not. In other words, it checks if the given migration map
     * consists any migration lock-in status or not.
     *
     * @param migrationMap the migration map
     * @return true if the given migration map is feasible, false otherwise
     */
    private boolean isMigrationMapFeasible(final Map<Vm, Host> migrationMap) {
        Map<Host, List<Vm>> hostVmListMap = new HashMap<>();
        migrationMap.forEach((vm, host) -> {
            List<Vm> vmList = host.getVmList();
            hostVmListMap.putIfAbsent(host, new ArrayList<>(vmList));
        });

        Map<Vm, Host> notFeasibleMigrationMap = new LinkedHashMap<>();

        for (Map.Entry<Vm, Host> vmHostEntry : migrationMap.entrySet()) {
            Vm sourceVm = vmHostEntry.getKey();
            Host sourceHost = sourceVm.getHost();
            Host targetHost = vmHostEntry.getValue();

            if (isHostSuitableForVm(sourceVm, targetHost, hostVmListMap.get(targetHost))) {
                //deallocating the VM from source targetHost
                if (hostVmListMap.containsKey(sourceHost)) {
                    hostVmListMap.get(sourceHost).remove(sourceVm);
                }

                //allocating resources of the destination targetHost to the VM
                hostVmListMap.get(targetHost).add(sourceVm);
            } else {
                notFeasibleMigrationMap.put(sourceVm, targetHost);
            }
        }

        while (!notFeasibleMigrationMap.isEmpty()) {
            Map<Vm, Host> newFeasibleMigration = new HashMap<>();

            for (Map.Entry<Vm, Host> vmHostEntry : notFeasibleMigrationMap.entrySet()) {
                Vm sourceVm = vmHostEntry.getKey();
                Host sourceHost = sourceVm.getHost();
                Host targetHost = vmHostEntry.getValue();

                if (isHostSuitableForVm(sourceVm, targetHost, hostVmListMap.get(targetHost))) {
                    newFeasibleMigration.put(sourceVm, targetHost);

                    //deallocating the VM from source targetHost
                    if (hostVmListMap.containsKey(sourceHost)) {
                        hostVmListMap.get(sourceHost).remove(sourceVm);
                    }

                    //allocating resources of the destination targetHost to the VM
                    hostVmListMap.get(targetHost).add(sourceVm);
                }
            }

            if (newFeasibleMigration.isEmpty()) {
                return false;
            } else {
                newFeasibleMigration.keySet().forEach(notFeasibleMigrationMap::remove);
            }
        }

        return true;
    }
}
