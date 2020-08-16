package org.myPaper.datacenter.vmAllocationPolicies;

import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationAbstract;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicyRandomSelection;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.acsAlgorithms.DatacenterSolutionEntry;
import org.myPaper.acsAlgorithms.Liu.Liu;
import org.myPaper.broker.DatacenterBrokerLiu;
import org.myPaper.datacenter.DatacenterPro;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class VmAllocationPolicyMigrationStaticThresholdLiu
    extends VmAllocationPolicyMigrationAbstract
    implements VmAllocationPolicyMigrationStaticThresholdAcsBased {
    protected static final Logger LOGGER = LoggerFactory.getLogger(VmAllocationPolicyMigrationStaticThresholdLiu.class.getSimpleName());

    /**
     * @see #getOverUtilizationThreshold(Host)
     */
    protected static final double DEF_OVER_UTILIZATION_THRESHOLD = 0.9;

    /**
     * @see #getOverUtilizationThreshold(Host)
     * @see #setOverUtilizationThreshold(double)
     */
    protected double overUtilizationThreshold = DEF_OVER_UTILIZATION_THRESHOLD;

    private final Liu LIU_ACS;

    public VmAllocationPolicyMigrationStaticThresholdLiu(Liu liu) {
        super(new VmSelectionPolicyRandomSelection());

        LIU_ACS = liu;
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

        Optional<Map<Vm, Host>> solution = LIU_ACS.getBestSolution(vmList, getDatacenter(), allowedHostList);

        if (solution.isPresent() && !solution.get().isEmpty()) {
            LOGGER.info("{}: {} found a solution for the requested Vm list.",
                getDatacenter().getSimulation().clockStr(),
                getDatacenter());
        }else {
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

                    LOGGER.info("{}: {}: the migration of {} to {} is able to become feasible after some other Vm migrations." +
                            "It might not be able to do the migration at the first try (It will be queued).",
                        getDatacenter().getSimulation().clockStr(),
                        getDatacenter(),
                        sourceVm,
                        targetHost);
                }
            }

            if (newFeasibleMigration.isEmpty()) {
                LOGGER.warn("{}: {}: some VMs ({}) are not able to migrate out due to the migration lock in problem.",
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
     * Gets the solution without migration map.
     *
     * @param solution the target solution
     * @return the solution
     */
    public Map<Vm, Host> getSolutionWithoutMigrationMap(final Map<Vm, Host> solution) {
        return solution.entrySet().parallelStream()
            .filter(vmHostEntry -> !vmHostEntry.getKey().isCreated())
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

            List<Vm> overloadedHostVmList = overloadedHost.getVmList().parallelStream()
                .filter(Vm::isCreated)
                .filter(vm -> !vm.getCloudletScheduler().getCloudletExecList().isEmpty())
                .collect(Collectors.toList());

            if (!overloadedHostVmList.isEmpty()) {
                totalVmList.addAll(overloadedHostVmList);

                LOGGER.info("{}: {}: The following Vms are selected for migrating out from overloaded {}: {}",
                    getDatacenter().getSimulation().clockStr(),
                    getDatacenter(),
                    overloadedHost,
                    overloadedHostVmList);
            }else {
                LOGGER.warn("{}: {}: No suitable Vm was found for migration out from overloaded {}!",
                    getDatacenter().getSimulation().clockStr(),
                    getDatacenter(),
                    overloadedHost);
            }

        });

        double totalExcessOfCpuMIPS = overloadedHostList.parallelStream()
            .filter(host -> host.getCpuPercentUtilization() > getOverUtilizationThreshold())
            .mapToDouble(host -> Math.abs(host.getTotalMipsCapacity() * getOverUtilizationThreshold() - host.getCpuMipsUtilization()))
            .sum();

        double totalExcessOfMemory = overloadedHostList.parallelStream()
            .filter(host -> host.getRam().getPercentUtilization() > getOverUtilizationThreshold())
            .mapToDouble(host -> Math.abs(host.getRam().getCapacity() * getOverUtilizationThreshold() - host.getRamUtilization()))
            .sum();

        double totalAccumulatedMips = 0;
        double totalAccumulatedMemory = 0;
        int lastSelectedUnderloadedHostId = -1;

        for (Host underloadedHost : underloadedHostList) {
            if (underloadedHost.getCpuPercentUtilization() <= getUnderUtilizationThreshold()) {
                totalAccumulatedMips +=
                    Math.abs(underloadedHost.getTotalMipsCapacity() * getOverUtilizationThreshold() - underloadedHost.getCpuMipsUtilization());
            }

            if (underloadedHost.getRam().getPercentUtilization() <= getUnderUtilizationThreshold()) {
                totalAccumulatedMemory +=
                    Math.abs(underloadedHost.getRam().getCapacity() * getOverUtilizationThreshold() - underloadedHost.getRamUtilization());
            }

            if (totalAccumulatedMips >= 2 * totalExcessOfCpuMIPS && totalAccumulatedMemory >= 2 * totalExcessOfMemory) {
                lastSelectedUnderloadedHostId = (int) underloadedHost.getId();
                break;
            }
        }

        if (lastSelectedUnderloadedHostId != -1) {
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
                }else {
                    LOGGER.warn("{}: {}: The underloaded {} is not ready to have any Vm migration!",
                        getDatacenter().getSimulation().clockStr(),
                        getDatacenter(),
                        underloadedHost);
                }

                if (underloadedHost.getId() == lastSelectedUnderloadedHostId) {
                    break;
                }
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
        return host.getCpuPercentUtilization() > getOverUtilizationThreshold() ||
            getHostVmsRamUtilization(host) > getOverUtilizationThreshold();
    }

    @Override
    public boolean isHostUnderloaded(Host host) {
        return host.getCpuPercentUtilization() <= getUnderUtilizationThreshold() ||
            getHostVmsRamUtilization(host) <= getUnderUtilizationThreshold();
    }

    @Override
    public double getOverUtilizationThreshold(Host host) {
        return getOverUtilizationThreshold();
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
                    host.getCpuPercentUtilization() <= getUnderUtilizationThreshold() ||
                    getHostVmsRamUtilization(host) > getOverUtilizationThreshold() ||
                    getHostVmsRamUtilization(host) <= getUnderUtilizationThreshold()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Gets the RAM utilization of the given host according to the current RAM utilization of its Vms
     *
     * @param host the host
     * @return the RAM utilization
     */
    private double getHostVmsRamUtilization(final Host host) {
        return host.getVmList().parallelStream()
            .mapToDouble(vm -> vm.getRam().getAllocatedResource())
            .average()
            .orElse(0);
    }

    /**
     * Requests the cloud broker to find some migration solutions from the entire of cloud federation environment.
     *
     * @param vmList the list of Vms
     */
    private Map<Vm, Host> requestExternalVmMigrationSolution(final List<Vm> vmList) {
        Map<Vm, Host> externalMigrationMap = new HashMap<>();

        if (!isCloudFederationActive()) {
            return externalMigrationMap;
        }

        DatacenterBrokerLiu datacenterBrokerLiu = (DatacenterBrokerLiu) getDatacenterPro().getCloudCoordinator().getCloudBroker();
        List<Map<Vm, Host>> solutionMapList = datacenterBrokerLiu
            .getMigrationSolutionMapList(getDatacenter(), vmList, true).stream()
            .map(DatacenterSolutionEntry::getSolution)
            .collect(Collectors.toList());

        if (!solutionMapList.isEmpty()) {
            externalMigrationMap = LIU_ACS.getBestSolution(solutionMapList, true, vmList);
        }

        return externalMigrationMap;
    }

    /**
     * Checks if any external cloud broker is available for this cloud provider or not.
     *
     * @return true if external brokers are available, false otherwise
     */
    private boolean isCloudFederationActive() {
        if (getDatacenterPro().getCloudCoordinator() == null) {
            return false;
        }else {
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
}
