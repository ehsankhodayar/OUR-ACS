package org.myPaper.acsAlgorithms.Liu;

import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.util.Conversion;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.additionalClasses.NormalizeZeroOne;
import org.myPaper.additionalClasses.SortMap;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public abstract class LiuAbstract implements Liu {
    private int MAXIMAL_ITERATION; //Maximal iteration
    private int NUMBER_OF_ANTS; //The number of ants
    private double q0; //is a constant in range [0,1] and is used to control the exploitation and exploration behaviors of a ant
    private double LOCAL_PHEROMONE_DECAY; //local pheromone decay (0 < p < 1)
    private double GLOBAL_PHEROMONE_DECAY; //global pheromone decay (0 < p0 < 1)
    private double beta; //is a predefined parameter that controls the relative importance of heuristic information (beta > 0)
    private double overutilizationThreshold;
    private Map<Vm, PheromoneInformationBetweenVmPairs> LAST_VM_PHEROMONE_INFORMATION_MAP;

    /**
     * OurAcs abstract class.
     * <p>
     * OurAcs, X.F., Zhan, Z.H., Deng, J.D., Li, Y., Gu, T. and Zhang, J., 2016. An energy efficient ant colony system
     * for virtual machine placement in cloud computing.
     * IEEE transactions on evolutionary computation, 22(1), pp.113-128.
     * </p>
     *
     * <p>
     * OurAcs, X.F., Zhan, Z.H. and Zhang, J., 2017. An energy aware unified ant colony system for dynamic virtual machine
     * placement in cloud computing. Energies, 10(5), p.609.
     * </p>
     *
     * @param maximalIteration maximum number of iterations
     * @param numberOfAnts     the number of ants in each iteration
     * @param q0               the q0 is constant in range [0,1] and is used to control the exploitation and exploration behaviors of the ant
     * @param lpd              local pheromone evaporation rate (0 < p < 1)
     * @param gld              global pheromone evaporation rate (0 < p0 < 1)
     * @param beta             a predefined parameter that controls the relative importance of heuristic information (beta > 0)
     */
    LiuAbstract(final int maximalIteration,
                final int numberOfAnts,
                final double q0,
                final double lpd,
                final double gld,
                final double beta) {
        MAXIMAL_ITERATION = maximalIteration;
        NUMBER_OF_ANTS = numberOfAnts;
        this.q0 = q0;
        LOCAL_PHEROMONE_DECAY = lpd;
        GLOBAL_PHEROMONE_DECAY = gld;
        this.beta = beta;
        overutilizationThreshold = 1;

        LAST_VM_PHEROMONE_INFORMATION_MAP = new HashMap<>();
    }

    @Override
    public int getMaximalIteration() {
        return MAXIMAL_ITERATION;
    }

    @Override
    public void setMaximalIteration(int maximalIteration) {
        MAXIMAL_ITERATION = maximalIteration;
    }

    @Override
    public int getNumberOfAnts() {
        return NUMBER_OF_ANTS;
    }

    @Override
    public void setNumberOfAnts(int numberOfAnts) {
        NUMBER_OF_ANTS = numberOfAnts;
    }

    @Override
    public double getQ0() {
        return q0;
    }

    @Override
    public void setQ0(double q0) {
        this.q0 = q0;
    }

    @Override
    public double getLocalPheromoneEvaporationRate() {
        return LOCAL_PHEROMONE_DECAY;
    }

    @Override
    public void setLocalPheromoneEvaporationRate(double localPheromoneEvaporationRate) {
        LOCAL_PHEROMONE_DECAY = localPheromoneEvaporationRate;
    }

    @Override
    public double getGlobalPheromoneEvaporationRate() {
        return GLOBAL_PHEROMONE_DECAY;
    }

    @Override
    public void setGlobalPheromoneEvaporationRate(double globalPheromoneEvaporationRate) {
        GLOBAL_PHEROMONE_DECAY = globalPheromoneEvaporationRate;
    }

    @Override
    public double getBeta() {
        return beta;
    }

    @Override
    public void setBeta(double beta) {
        this.beta = beta;
    }

    @Override
    public void throwIllegalState(String errorMsg, String callerName) {
        throw new IllegalStateException("OurAcs ACS Algorithm: " + callerName + " " + errorMsg + "!");
    }

    /**
     * Sets the initial pheromone value between VM pairs.
     *
     * @param vmList                           the list of VMs
     * @param vmPheromoneInformationVmPairsMap the pheromone map of VM pairs
     * @param size                             the number of VMs or hosts
     * @see #getInitialPheromoneValue(int)
     */
    protected void setInitialPheromoneLevel(List<Vm> vmList, Map<Vm, PheromoneInformationBetweenVmPairs> vmPheromoneInformationVmPairsMap, int size) {
        if (vmList.isEmpty()) {
            throwIllegalState("The given vmList could not be empty", "setInitialPheromoneLevel");
        }

        if (size <= 0) {
            throwIllegalState("The given size could not be equal or lesser than zero", "setInitialPheromoneLevel");
        }

        final double initialPheromoneValue = getInitialPheromoneValue(size);

        vmList.forEach(vm -> {
            PheromoneInformationBetweenVmPairs pheromoneInformation =
                new PheromoneInformationBetweenVmPairs(vm, vmList, initialPheromoneValue);

            if (LAST_VM_PHEROMONE_INFORMATION_MAP != null && !LAST_VM_PHEROMONE_INFORMATION_MAP.isEmpty()) {
                //Inherits the pheromone values from last time if to VM pairs were assigned to the same host in the last time

                if (LAST_VM_PHEROMONE_INFORMATION_MAP.containsKey(vm)) {
                    PheromoneInformationBetweenVmPairs previousPheromoneInformation = LAST_VM_PHEROMONE_INFORMATION_MAP.get(vm);

                    previousPheromoneInformation.getVmPheromoneMap().forEach((previousVmPair, previousPheromoneValue) -> {
                        if (vmList.contains(previousVmPair)) {
                            if (vm.getHost() == previousVmPair.getHost()) {
                                pheromoneInformation.updatePheromoneValue(previousVmPair, previousPheromoneValue);
                            }
                        }
                    });
                }

            }

            vmPheromoneInformationVmPairsMap.put(vm, pheromoneInformation);
        });
    }

    /**
     * Gets the initial pheromone value between each VM pair. The initial pheromone level will be calculated as 1/numberOfVms or 1/numberOfHosts.
     *
     * @param size number of VMs or available hosts
     * @return the initial pheromone value
     * @see #setInitialPheromoneLevel(List, Map, int)
     */
    private double getInitialPheromoneValue(int size) {
        if (size <= 0) {
            throwIllegalState("The number of VMs or hosts could not be equal or lesser than zero", "getInitialPheromoneValue");
        }

        return 1 / (double) size;
    }

    /**
     * Shuffles the given VM list.
     *
     * @param vmList the vm list which must be shuffled
     * @return a shuffled VM list
     */
    protected List<Vm> shuffleVmList(List<Vm> vmList) {
        List<Vm> shuffledObjectList = new ArrayList<>(vmList);
        Collections.shuffle(shuffledObjectList);
        return shuffledObjectList;
    }

    /**
     * Gets a list of suitable hosts for the given VM.
     *
     * @param vm               the target VM
     * @param hostNewVmListMap the host temporary VM list map
     * @param allowedHostList  list of allowed hosts at this datacenter
     * @return a list of suitable hosts
     */
    protected List<Host> getSuitableHostList(Vm vm, Map<Host, List<Vm>> hostNewVmListMap, List<Host> allowedHostList) {
        //List of suitable hosts for the given VM
        List<Host> suitableHostList = new ArrayList<>();

        for (Host host : allowedHostList) {
            //checks hosts in the allowed host list for the given VM
            double availablePes =
                getHostTotalAvailablePes(host, hostNewVmListMap.get(host)) - vm.getNumberOfPes();

            double availableMips =
                getHostTotalAvailableMIPS(host, hostNewVmListMap.get(host)) - vm.getTotalMipsCapacity();

            double availableMemory =
                getHostTotalAvailableMemory(host, hostNewVmListMap.get(host)) - vm.getRam().getCapacity();

            double availableStorage =
                getHostTotalAvailableStorage(host, hostNewVmListMap.get(host)) - vm.getStorage().getCapacity();

            double availableBw =
                getHostTotalAvailableBandwidth(host, hostNewVmListMap.get(host)) - vm.getBw().getCapacity();

            double currentMipsUtilization =
                ((double) getHostMipsUtilization(host, hostNewVmListMap.get(host)) + vm.getTotalCpuMipsUtilization())
                    / host.getTotalMipsCapacity();

            double currentRamUtilization =
                ((double) getHostMemoryUtilization(host, hostNewVmListMap.get(host)) + vm.getRam().getAllocatedResource())
                    / (double) host.getRam().getCapacity();

            if (availablePes >= 0 &&
                availableMips >= 0 &&
                currentMipsUtilization <= getOverutilizationThreshold() &&
                availableMemory >= 0 &&
                currentRamUtilization <= getOverutilizationThreshold() &&
                availableStorage >= 0 && availableBw >= 0) {
                suitableHostList.add(host);
            }
        }

        return suitableHostList;
    }

    /**
     * Gets host total number of reserved Pes + in use Pes.
     *
     * @param host          the target host
     * @param hostNewVmList the host temporary VM list
     * @return the number of available Pes at the given host
     */
    protected int getHostTotalAvailablePes(Host host, List<Vm> hostNewVmList) {
        List<Vm> vmList = getHostCurrentVmList(host, hostNewVmList);

        int totalNumberOfReservedPes = vmList.parallelStream()
            .filter(vm -> vm.getHost() != host || getRequestedVmList().contains(vm))
            .mapToInt(vm -> (int) vm.getNumberOfPes())
            .sum();

        int totalNumberOfReallocationVmsPes = host.getVmList().parallelStream()
            .filter(vm -> getRequestedVmList().contains(vm))
            .mapToInt((vm -> (int) vm.getNumberOfPes()))
            .sum();

        return host.getFreePesNumber() + totalNumberOfReallocationVmsPes - totalNumberOfReservedPes;
    }

    /**
     * Gets host total amount of reserved MIPS + in use MIPS.
     *
     * @param host          the target host
     * @param hostNewVmList the host temporary VM list
     * @return the amount of available MIPS at the given host
     */
    private int getHostTotalAvailableMIPS(Host host, List<Vm> hostNewVmList) {
        List<Vm> vmList = getHostCurrentVmList(host, hostNewVmList);

        int totalAmountOfReservedMIPS = vmList.parallelStream()
            .filter(vm -> vm.getHost() != host || getRequestedVmList().contains(vm))
            .mapToInt(vm -> (int) vm.getTotalMipsCapacity())
            .sum();

        int totalAmountOfReallocationVmsMIPS = host.getVmList().parallelStream()
            .filter(vm -> getRequestedVmList().contains(vm))
            .mapToInt(vm -> (int) vm.getTotalMipsCapacity())
            .sum();

        return (int) host.getTotalAvailableMips() + totalAmountOfReallocationVmsMIPS - totalAmountOfReservedMIPS;
    }

    /**
     * Gets host total amount of reserved memory + in use memory.
     *
     * @param host          the target host
     * @param hostNewVmList the host temporary VM list
     * @return the amount of available memory at the given host
     */
    protected int getHostTotalAvailableMemory(Host host, List<Vm> hostNewVmList) {
        List<Vm> vmList = getHostCurrentVmList(host, hostNewVmList);

        int totalAmountOfReservedMemory = vmList.parallelStream()
            .filter(vm -> vm.getHost() != host || getRequestedVmList().contains(vm))
            .mapToInt(vm -> (int) vm.getRam().getCapacity())
            .sum();

        int totalAmountOfReallocationVmsMemory = host.getVmList().parallelStream()
            .filter(vm -> getRequestedVmList().contains(vm))
            .mapToInt(vm -> (int) vm.getRam().getCapacity())
            .sum();

        return (int) host.getRam().getAvailableResource() + totalAmountOfReallocationVmsMemory - totalAmountOfReservedMemory;
    }

    /**
     * Gets host total amount of reserved storage + in use storage.
     *
     * @param host          the target host
     * @param hostNewVmList the host temporary VM list
     * @return the amount of available storage at the given host
     */
    private int getHostTotalAvailableStorage(Host host, List<Vm> hostNewVmList) {
        List<Vm> vmList = getHostCurrentVmList(host, hostNewVmList);

        int totalAmountOfReservedStorage = vmList.parallelStream()
            .filter(vm -> vm.getHost() != host || getRequestedVmList().contains(vm))
            .mapToInt(vm -> (int) vm.getStorage().getCapacity())
            .sum();

        int totalAmountOfReallocationVmsStorage = host.getVmList().parallelStream()
            .filter(vm -> getRequestedVmList().contains(vm))
            .mapToInt(vm -> (int) vm.getStorage().getCapacity())
            .sum();

        return (int) host.getStorage().getAvailableResource() + totalAmountOfReallocationVmsStorage - totalAmountOfReservedStorage;
    }

    /**
     * Gets host total amount of reserved bandwidth + in use bandwidth.
     *
     * @param host          the target host
     * @param hostNewVmList the host temporary VM list map
     * @return the amount of available bandwidth at the given host
     */
    private int getHostTotalAvailableBandwidth(Host host, List<Vm> hostNewVmList) {
        List<Vm> vmList = getHostCurrentVmList(host, hostNewVmList);

        int totalAmountOfReservedBw = vmList.parallelStream()
            .filter(vm -> vm.getHost() != host || getRequestedVmList().contains(vm))
            .mapToInt(vm -> (int) vm.getBw().getCapacity())
            .sum();

        int totalAmountOfReallocationVmsBw = host.getVmList().parallelStream()
            .filter(vm -> getRequestedVmList().contains(vm))
            .mapToInt(vm -> (int) vm.getBw().getCapacity())
            .sum();

        return (int) host.getBw().getAvailableResource() + totalAmountOfReallocationVmsBw - totalAmountOfReservedBw;
    }

    /**
     * Gets the host MIPS utilization according to its new Vm list
     *
     * @param host          the host
     * @param hostNewVmList the temporary Vm list
     * @return the host MIPS utilization
     */
    public int getHostMipsUtilization(Host host, List<Vm> hostNewVmList) {
        List<Vm> vmList = getHostCurrentVmList(host, hostNewVmList);

        int totalReservedMips = vmList.parallelStream()
            .filter(vm -> vm.getHost() != host || getRequestedVmList().contains(vm))
            .mapToInt(vm -> vm.isCreated() ? (int) vm.getTotalCpuMipsUtilization() :
                (int) (vm.getTotalMipsCapacity() * overutilizationThreshold))
            .sum();

        int totalAmountOfReallocationVmsMips = host.getVmList().parallelStream()
            .filter(vm -> getRequestedVmList().contains(vm))
            .mapToInt(vm -> (int) vm.getTotalCpuMipsUtilization())
            .sum();

        return (int) host.getCpuMipsUtilization() - totalAmountOfReallocationVmsMips + totalReservedMips;
    }

    /**
     * Gets the host Memory utilization according to its new Vm list
     *
     * @param host          the host
     * @param hostNewVmList the temporary Vm list
     * @return the host MIPS utilization
     */
    public int getHostMemoryUtilization(Host host, List<Vm> hostNewVmList) {
        List<Vm> vmList = getHostCurrentVmList(host, hostNewVmList);

        int totalReservedMemory = vmList.parallelStream()
            .filter(vm -> vm.getHost() != host || getRequestedVmList().contains(vm))
            .mapToInt(vm -> vm.isCreated() ? (int) vm.getRam().getAllocatedResource() :
                (int) (vm.getRam().getCapacity() * overutilizationThreshold))
            .sum();

        int totalAmountOfReallocationVmsMemory = host.getVmList().parallelStream()
            .filter(vm -> getRequestedVmList().contains(vm))
            .mapToInt(vm -> vm.isCreated() ? (int) vm.getRam().getAllocatedResource() : (int) (vm.getRam().getCapacity() * overutilizationThreshold))
            .sum();

        int totalMemoryUtilization = host.getVmList().parallelStream()
            .mapToInt(vm -> vm.isCreated() ? (int) vm.getRam().getAllocatedResource() : (int) (vm.getRam().getCapacity() * overutilizationThreshold))
            .sum();

        return totalMemoryUtilization - totalAmountOfReallocationVmsMemory + totalReservedMemory;
    }

    /**
     * Selects a suitable host for the given VM according to the construction rule.
     *
     * @param vm                        the target VM
     * @param hostNewVmListMap          the host new VM list
     * @param suitableHostList          a list of suitable hosts for the given VM
     * @param size                      the number of VMs or available hosts
     * @param vmPheromoneInformationMap the vmPheromoneInformationMap
     * @return a suitable host
     */
    protected Host selectHostForVmAccordingToConstructionRule(Vm vm,
                                                              Map<Host, List<Vm>> hostNewVmListMap,
                                                              List<Host> suitableHostList,
                                                              int size,
                                                              Map<Vm, PheromoneInformationBetweenVmPairs> vmPheromoneInformationMap) {
        if (suitableHostList.size() == 1) {
            return suitableHostList.get(0);
        }

        Random random = new Random();
        double q = random.nextDouble() * 1;

        if (q <= q0) {
            return Collections.max(suitableHostList,
                Comparator.comparing(host ->
                    (getVmPreference(vm, host, hostNewVmListMap.get(host), size, vmPheromoneInformationMap) *
                        Math.pow(getHostHeuristic(host, hostNewVmListMap.get(host), vm), beta))));
        } else {

            //the sum of multiplication of the VM preference and host heuristic
            final double smvphh = suitableHostList.parallelStream()
                .mapToDouble(host -> (getVmPreference(vm, host, hostNewVmListMap.get(host), size, vmPheromoneInformationMap) *
                    Math.pow(getHostHeuristic(host, hostNewVmListMap.get(host), vm), beta)))
                .sum();

            Map<Host, Double> probabilityMap = suitableHostList.parallelStream()
                .collect(Collectors.toMap(host -> host,
                    host -> getAssignmentProbability(vm, host, hostNewVmListMap.get(host), smvphh, size, vmPheromoneInformationMap)));

            Map<Host, Double> accumulatedSumMap = getAccumulatedSumMap(probabilityMap);
            return selectHostBasedOnRouletteWheelMap(accumulatedSumMap);
        }
    }

    /**
     * Gets the heuristic for the given host and VM pair.
     *
     * @param host          the target host
     * @param hostNewVmList the host VM list
     * @param vm            the target VM
     * @return the heuristic
     */
    protected abstract double getHostHeuristic(Host host, List<Vm> hostNewVmList, Vm vm);

    /**
     * Gets the VM preference value to the given host. Note the if no temporary VM is created in the given host,
     * it will consider the initial pheromone value.
     *
     * @param vm                        the target VM
     * @param host                      the target host
     * @param hostNewVmList             the host VM list
     * @param size                      the number of VMs or available hosts
     * @param vmPheromoneInformationMap the vmPheromoneInformationMap
     * @return the amount of VM preference
     */
    private double getVmPreference(Vm vm,
                                   Host host,
                                   List<Vm> hostNewVmList,
                                   int size,
                                   Map<Vm, PheromoneInformationBetweenVmPairs> vmPheromoneInformationMap) {
        List<Vm> hostVmList = getHostCurrentVmList(host, hostNewVmList);

        if (hostVmList.isEmpty()) {
            return getInitialPheromoneValue(size);
        } else {
            double totalPheromoneBetweenVmPairs = hostVmList.parallelStream()
                .filter(hostVm -> hostVm != vm)
                .mapToDouble(hostVm -> vmPheromoneInformationMap.get(hostVm).getPheromoneValue(vm))
                .sum();
            return totalPheromoneBetweenVmPairs / hostVmList.size();
        }
    }

    /**
     * Gets the VM assignment probability to the given host
     *
     * @param vm                        the target VM
     * @param host                      the target host
     * @param hostVmList                the host VM list
     * @param smvphh                    the sum of multiplication of the VM preference and host heuristic
     * @param size                      number of Vms or available hosts
     * @param vmPheromoneInformationMap the VM Pheromone information map
     * @return the assignment probability
     */
    private double getAssignmentProbability(Vm vm, Host host,
                                            List<Vm> hostVmList,
                                            double smvphh,
                                            int size,
                                            Map<Vm, PheromoneInformationBetweenVmPairs> vmPheromoneInformationMap) {
        if (smvphh == 0) {
            throwIllegalState("The sum of multiplication of Vm host preference and heuristic could not be zero",
                "getAssignmentProbability");
        }

        double multiplicationOfVmPreferenceAndHostHeuristic =
            getVmPreference(vm, host, hostVmList, size, vmPheromoneInformationMap) *
                Math.pow(getHostHeuristic(host, hostVmList, vm), beta);
        return multiplicationOfVmPreferenceAndHostHeuristic / smvphh;
    }

    /**
     * Performs local updating between VM pairs.
     *
     * @param solution                  the target solution
     * @param vmPheromoneInformationMap the VM information map
     */
    protected void performLocalPheromoneUpdating(Map<Vm, Host> solution, Map<Vm, PheromoneInformationBetweenVmPairs> vmPheromoneInformationMap) {
        Map<Host, List<Vm>> hostNewVmListMap = convertSolutionMapToHostNewVmListMap(solution);

        for (Map.Entry<Host, List<Vm>> hostVmListEntry : hostNewVmListMap.entrySet()) {
            List<Vm> vmList = hostVmListEntry.getValue();
            vmList.forEach(vm_i -> vmList.forEach(vm_j -> {
                if (vm_i != vm_j) {
                    double currentPheromoneValue = vmPheromoneInformationMap.get(vm_i).getPheromoneValue(vm_j);
                    double newPheromoneValue = (1 - LOCAL_PHEROMONE_DECAY) * currentPheromoneValue +
                        LOCAL_PHEROMONE_DECAY * getInitialPheromoneValue(solution.keySet().size());
                    vmPheromoneInformationMap.get(vm_i).updatePheromoneValue(vm_j, newPheromoneValue);
                }
            }));
        }
    }

    /**
     * Performs a global updating between the VM pairs.
     *
     * @param solution                  the
     * @param vmPheromoneInformationMap the vmPheromoneInformationMap
     */
    protected void performGlobalPheromoneUpdating(Map<Vm, Host> solution, Map<Vm, PheromoneInformationBetweenVmPairs> vmPheromoneInformationMap) {
        Map<Host, List<Vm>> hostNewVmListMap = convertSolutionMapToHostNewVmListMap(solution);

        for (Map.Entry<Host, List<Vm>> hostVmListEntry : hostNewVmListMap.entrySet()) {
            hostVmListEntry.getValue().forEach(vm_i -> hostVmListEntry.getValue().forEach(vm_j -> {
                if (vm_i != vm_j) {
                    double currentPheromoneValue = vmPheromoneInformationMap.get(vm_i).getPheromoneValue(vm_j);

                    double reinforcementPheromone =
                        (1 / (double) getSolutionNumberOfUsedHosts(solution)) + (1 / (getSolutionTotalResourceWastage(solution) + 1));

                    double newPheromoneValue = (1 - GLOBAL_PHEROMONE_DECAY) * currentPheromoneValue +
                        GLOBAL_PHEROMONE_DECAY * reinforcementPheromone;

                    vmPheromoneInformationMap.get(vm_i).updatePheromoneValue(vm_j, newPheromoneValue);
                }
            }));
        }
    }

    /**
     * Performs a local search to make an infeasible solution to a feasible one.
     *
     * @param solution the target infeasible solution
     */
    protected Map<Vm, Host> performOemLocalSearchOnSolution(Map<Vm, Host> solution) {
        Map<Host, List<Vm>> hostNewVmListMap = convertSolutionMapToHostNewVmListMap(solution);
        Map<Host, List<Vm>> temporaryHostVmListMap = new HashMap<>();

        List<Host> overloadedHostList = hostNewVmListMap.keySet().parallelStream()
            .filter(host -> isHostOverloaded(host, hostNewVmListMap.get(host)))
            .collect(Collectors.toList());

        createHostTemporaryVmListMap(solution, overloadedHostList, temporaryHostVmListMap);

        List<Host> notOverloadedHostList = hostNewVmListMap.keySet().parallelStream()
            .filter(host -> !isHostOverloaded(host, hostNewVmListMap.get(host)))
            .collect(Collectors.toList());

        createHostTemporaryVmListMap(solution, notOverloadedHostList, temporaryHostVmListMap);

        overloadedHostLoop:
        for (Host overloadedHost : overloadedHostList) {
            Map<Vm, Double> overloadedHostVmCpuMemoryAbsoluteDifferenceMap = new HashMap<>();

            temporaryHostVmListMap.get(overloadedHost).forEach(vm -> overloadedHostVmCpuMemoryAbsoluteDifferenceMap
                .put(vm, Math.abs(vm.getNumberOfPes() - Conversion.megaToGiga(vm.getRam().getCapacity()))));

            List<Vm> overloadedHostVmCpuMemoryAbsoluteDifferenceList =
                new ArrayList<>(SortMap.sortByValue(overloadedHostVmCpuMemoryAbsoluteDifferenceMap, true).keySet());

            for (Host notOverloadedHost : notOverloadedHostList) {
                Map<Vm, Double> notOverloadedHostVmCpuMemoryAbsoluteDifferenceMap = new HashMap<>();

                temporaryHostVmListMap.get(notOverloadedHost).forEach(vm -> notOverloadedHostVmCpuMemoryAbsoluteDifferenceMap
                    .put(vm, Math.abs(vm.getNumberOfPes() - Conversion.megaToGiga(vm.getRam().getCapacity()))));

                List<Vm> notOverloadedHostVmCpuMemoryAbsoluteDifferenceList =
                    new ArrayList<>(SortMap.sortByValue(notOverloadedHostVmCpuMemoryAbsoluteDifferenceMap, false).keySet());

                List<Vm> overloadedHostExchangedVmList = new ArrayList<>();
                List<Vm> notOverloadedHostExchangedVmList = new ArrayList<>();

                overloadedHostVmsLoop:
                for (Vm overloadedHostVm : overloadedHostVmCpuMemoryAbsoluteDifferenceList) {
                    if (!overloadedHostVm.isCreated() && !getRequestedVmList().contains(overloadedHostVm)) {
                        //Some VMs such as temporary VMs are not created yet
                        continue;
                    }

                    temporaryHostVmListMap.get(overloadedHost).remove(overloadedHostVm);
                    temporaryHostVmListMap.get(notOverloadedHost).add(overloadedHostVm);

                    if (isHostOverloaded(notOverloadedHost, temporaryHostVmListMap.get(notOverloadedHost))) {
                        temporaryHostVmListMap.get(overloadedHost).add(overloadedHostVm);
                        temporaryHostVmListMap.get(notOverloadedHost).remove(overloadedHostVm);

                        for (Vm notOverloadedHostVm : notOverloadedHostVmCpuMemoryAbsoluteDifferenceList) {
                            if (!notOverloadedHostVm.isCreated() && !getRequestedVmList().contains(notOverloadedHostVm)) {
                                //Some VMs such as temporary VMs are not created yet
                                continue;
                            }

                            temporaryHostVmListMap.get(overloadedHost).remove(overloadedHostVm);
                            temporaryHostVmListMap.get(overloadedHost).add(notOverloadedHostVm);
                            temporaryHostVmListMap.get(notOverloadedHost).remove(notOverloadedHostVm);
                            temporaryHostVmListMap.get(notOverloadedHost).add(overloadedHostVm);

                            if (!isHostOverloaded(notOverloadedHost, temporaryHostVmListMap.get(notOverloadedHost))) {
                                if (!isHostOverloaded(overloadedHost, temporaryHostVmListMap.get(overloadedHost))) {
                                    continue overloadedHostLoop;
                                } else {
                                    overloadedHostExchangedVmList.add(notOverloadedHostVm);
                                    notOverloadedHostExchangedVmList.add(overloadedHostVm);
                                    continue overloadedHostVmsLoop;
                                }
                            } else {
                                temporaryHostVmListMap.get(overloadedHost).add(overloadedHostVm);
                                temporaryHostVmListMap.get(overloadedHost).remove(notOverloadedHostVm);
                                temporaryHostVmListMap.get(notOverloadedHost).add(notOverloadedHostVm);
                                temporaryHostVmListMap.get(notOverloadedHost).remove(overloadedHostVm);
                            }
                        }
                    } else {
                        if (isHostOverloaded(overloadedHost, temporaryHostVmListMap.get(overloadedHost))) {
                            notOverloadedHostExchangedVmList.add(overloadedHostVm);
                        } else {
                            continue overloadedHostLoop;
                        }
                    }
                }

                if (isHostOverloaded(overloadedHost, temporaryHostVmListMap.get(overloadedHost)) ||
                    isHostOverloaded(notOverloadedHost, temporaryHostVmListMap.get(notOverloadedHost))) {
                    //Reset temporaryHostVmListMap

                    overloadedHostExchangedVmList.forEach(vm -> {
                        temporaryHostVmListMap.get(overloadedHost).remove(vm);
                        temporaryHostVmListMap.get(notOverloadedHost).add(vm);
                    });

                    notOverloadedHostExchangedVmList.forEach(vm -> {
                        temporaryHostVmListMap.get(notOverloadedHost).remove(vm);
                        temporaryHostVmListMap.get(overloadedHost).add(vm);
                    });
                }
            }
        }

        return convertHostNewVmListMapToSolutionMap(temporaryHostVmListMap);
    }

    /**
     * Creates a host temporary VM list map according to the current VM list of hosts and also their new VM list map. The
     * result will be saved at the temporaryHostVmListMap.
     *
     * @param solution               the solution
     * @param hostList               list of needed hosts
     * @param temporaryHostVmListMap the temporary host VM list map
     */
    private void createHostTemporaryVmListMap(Map<Vm, Host> solution, List<Host> hostList, Map<Host, List<Vm>> temporaryHostVmListMap) {
        hostList.forEach(host -> {
            List<Vm> temporaryVmList = convertSolutionMapToHostNewVmListMap(solution).get(host);
            List<Vm> totalVmList = getHostCurrentVmList(host, temporaryVmList);
            temporaryHostVmListMap.put(host, new ArrayList<>(totalVmList));
        });
    }

    @Override
    public Map<Vm, Host> getSolutionWithoutMigrationMap(Map<Vm, Host> solution) {
        List<Vm> migrationVmList = new ArrayList<>(getMigrationMapOfSolution(solution).keySet());

        return solution.entrySet().parallelStream()
            .filter(vmHostEntry -> !migrationVmList.contains(vmHostEntry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Map<Vm, Host> getMigrationMapOfSolution(Map<Vm, Host> solution) {
        return solution.entrySet().parallelStream()
            .filter(vmHostEntry -> vmHostEntry.getKey().isCreated())
            .filter(vmHostEntry -> vmHostEntry.getKey().getHost() != vmHostEntry.getValue())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Checks if the given host is overloaded or not.
     *
     * @param host            the target host
     * @param temporaryVmList the host temporary VM list
     * @return true if the host is overloaded, false otherwise
     */
    protected boolean isHostOverloaded(Host host, List<Vm> temporaryVmList) {
        double currentMipsUtilization =
            (double) getHostMipsUtilization(host, temporaryVmList) / host.getTotalMipsCapacity();
        double currentRamUtilization =
            (double) getHostMemoryUtilization(host, temporaryVmList) / (double) host.getRam().getCapacity();

        return (getHostTotalAvailablePes(host, temporaryVmList) < 0) ||
            (getHostTotalAvailableMIPS(host, temporaryVmList) < 0) ||
            (getHostTotalAvailableMemory(host, temporaryVmList) < 0) ||
            (getHostTotalAvailableStorage(host, temporaryVmList) < 0) ||
            (getHostTotalAvailableBandwidth(host, temporaryVmList) < 0) ||
            (currentMipsUtilization > getOverutilizationThreshold()) ||
            (currentRamUtilization > getOverutilizationThreshold());
    }

    /**
     * Gets the total resource wastage of the given solution.
     *
     * @param solution the target solution
     * @return the total resource wastage
     */
    protected double getSolutionTotalResourceWastage(Map<Vm, Host> solution) {
        Map<Host, List<Vm>> hostNewVmList = convertSolutionMapToHostNewVmListMap(solution);

        double totalWastage = hostNewVmList.keySet().parallelStream()
            .mapToDouble(host -> getHostTotalMipsWastage(host, hostNewVmList.get(host)) + getHostTotalMemoryWastage(host, hostNewVmList.get(host)))
            .sum();

        totalWastage = NormalizeZeroOne.normalize(totalWastage, hostNewVmList.size() * 2, 0);

        return totalWastage;
    }

    /**
     * Gets the host total MIPS wastage.
     *
     * @param host            the host
     * @param temporaryVmList the list of Vms that are going to be assigned to the given host
     * @return the total MIPS wastage
     */
    protected double getHostTotalMipsWastage(final Host host, final List<Vm> temporaryVmList) {
        return (host.getTotalMipsCapacity() - getHostMipsUtilization(host, temporaryVmList)) /
            host.getTotalMipsCapacity();
    }

    /**
     * Gets the host total Memory wastage.
     *
     * @param host            the host
     * @param temporaryVmList the list of Vms that are going to be assigned to the given host
     * @return the total memory wastage
     */
    protected double getHostTotalMemoryWastage(final Host host, final List<Vm> temporaryVmList) {
        return (double) (host.getRam().getCapacity() - getHostMemoryUtilization(host, temporaryVmList)) /
            (double) host.getRam().getCapacity();
    }

    /**
     * Converts the solution map to the hostNewVmListMap.
     *
     * @param solution the target solution
     * @return hostNewVmListMap
     */
    protected Map<Host, List<Vm>> convertSolutionMapToHostNewVmListMap(Map<Vm, Host> solution) {
        Map<Host, List<Vm>> hostNewVmListMap = new HashMap<>();

        for (Map.Entry<Vm, Host> vmHostEntry : solution.entrySet()) {
            hostNewVmListMap.putIfAbsent(vmHostEntry.getValue(), new ArrayList<>());
            hostNewVmListMap.get(vmHostEntry.getValue()).add(vmHostEntry.getKey());
        }

        return hostNewVmListMap;
    }

    /**
     * Converts the hostNewVmListMap to the solution map.
     *
     * @param hostNewVmListMap the target hostNewVmListMap
     * @return the solution
     */
    protected Map<Vm, Host> convertHostNewVmListMapToSolutionMap(final Map<Host, List<Vm>> hostNewVmListMap) {
        Map<Vm, Host> solution = new HashMap<>();
        for (Map.Entry<Host, List<Vm>> hostListEntry : hostNewVmListMap.entrySet()) {
            for (Vm vm : hostListEntry.getValue()) {
                solution.put(vm, hostListEntry.getKey());
            }
        }

        return solution;
    }

    /**
     * Checks if the given solution is feasible or not. In other words, it checks that are all hosts in the given solution
     * out of overloaded status or not.
     *
     * @param solution the target solution
     * @return true if the solution is feasible, false, otherwise
     */
    protected boolean isSolutionFeasible(Map<Vm, Host> solution) {
        Map<Host, List<Vm>> hostNewVmListMap = convertSolutionMapToHostNewVmListMap(solution);

        for (Map.Entry<Host, List<Vm>> hostListMapEntry : hostNewVmListMap.entrySet()) {
            Host host = hostListMapEntry.getKey();

            if (isHostOverloaded(host, hostNewVmListMap.get(host))) {
                return false;
            }
        }

        return true;
    }

    /**
     * Gets the number of used hosts for the target VM list in the given solution.
     *
     * @param solution the target solution
     * @return the number of used hosts in the given solution
     */
    protected int getSolutionNumberOfUsedHosts(final Map<Vm, Host> solution) {
        Map<Host, List<Vm>> hostNewVmListMap = convertSolutionMapToHostNewVmListMap(solution);

        return hostNewVmListMap.size();
    }

    /**
     * Gets an accumulated sum map according to the given probability distribution.
     *
     * @param probabilityMap the probability distribution map
     * @return the accumulated sum map
     */
    protected Map<Host, Double> getAccumulatedSumMap(Map<Host, Double> probabilityMap) {
        if (probabilityMap.isEmpty()) {
            throwIllegalState("The probability map could not be empty", "getRouletteWheelMap");
        }

        Map<Host, Double> sortedProbabilityMap = SortMap.sortByValue(probabilityMap, false);
        Map<Host, Double> rouletteWheelMap = new LinkedHashMap<>();
        double accumulatedSum = roundDouble.apply(sortedProbabilityMap.values().parallelStream().mapToDouble(value -> value).sum(), 6);

        for (int i = 0; i < sortedProbabilityMap.size(); i++) {
            double sumProbability = 0;
            Host currentHost = (Host) sortedProbabilityMap.keySet().toArray()[i];
            for (int j = i; j < sortedProbabilityMap.size(); j++) {
                Host host = (Host) sortedProbabilityMap.keySet().toArray()[j];
                sumProbability += sortedProbabilityMap.get(host);
            }

            sumProbability = roundDouble.apply(sumProbability, 6);
            rouletteWheelMap.put(currentHost, normalizeBetweenZeroAndOne(sumProbability, accumulatedSum, 0));
        }

        return SortMap.sortByValue(rouletteWheelMap, true);
    }

    /**
     * Selects a host according to the given accumulated sum map.
     *
     * @param rouletteWheelMap roulette wheel map
     * @return a host
     */
    protected Host selectHostBasedOnRouletteWheelMap(Map<Host, Double> rouletteWheelMap) {
        if (rouletteWheelMap.isEmpty()) {
            throwIllegalState("The accumulated sum map could not be empty", "selectHostBasedOnRouletteWheelMap");
        }

        Random random = new Random();
        double randomDouble = random.nextDouble() * 1;
        double previousThreshold = 0;

        for (Map.Entry<Host, Double> hostProbability : rouletteWheelMap.entrySet()) {
            if (previousThreshold == 0) {
                if (randomDouble >= previousThreshold && randomDouble <= hostProbability.getValue()) {
                    return hostProbability.getKey();
                } else {
                    previousThreshold = hostProbability.getValue();
                }
            } else {
                if (randomDouble > previousThreshold && randomDouble <= hostProbability.getValue()) {
                    return hostProbability.getKey();
                } else {
                    previousThreshold = hostProbability.getValue();
                }
            }
        }

        throwIllegalState("No host was found according to the given roulette wheel map", "selectHostBasedOnRouletteWheelMap");
        return Host.NULL;
    }

    /**
     * Normalizes a number in the range of 0 and 1.
     *
     * @param nonNormalizeNumber the non normalized number
     * @param max                the maximum value in that range
     * @param min                the minimum value in that range
     * @return the normalized number in the range of 0 and 1
     */
    private double normalizeBetweenZeroAndOne(double nonNormalizeNumber, double max, double min) {
        return (nonNormalizeNumber - min) / (max - min);
    }

    BiFunction<Double, Integer, Double> roundDouble = (value, places) -> {
        double a = 1;
        for (int i = 0; i < places; i++) {
            a *= 10;
        }
        return (double) Math.round(value * a) / a;
    };

    @Override
    public double getOverutilizationThreshold() {
        return overutilizationThreshold;
    }

    @Override
    public void setOverutilizationThreshold(final double threshold) {
        if (threshold <= 0 || threshold > 1) {
            throwIllegalState("The threshold must be in scale (0-1]", "setOverutilizationThreshold");
        }

        overutilizationThreshold = threshold;
    }

    @Override
    public abstract List<Vm> getRequestedVmList();

    /**
     * Cleans the given solution by removing the VMs that are already Created and are not in the {@link #getRequestedVmList()}.
     * Use this method when the solution is returned from the {@link #performOemLocalSearchOnSolution(Map)}.
     *
     * @param solution the solution
     * @return the cleaned solution
     */
    protected Map<Vm, Host> cleanSolution(Map<Vm, Host> solution) {
        //Clean the solution
        Map<Vm, Host> cleanedSolution = new HashMap<>(solution);
        List<Vm> extraVmMappings = new ArrayList<>();
        for (Map.Entry<Vm, Host> vmHostEntry : cleanedSolution.entrySet()) {
            Vm sourceVM = vmHostEntry.getKey();
            Host targetHost = vmHostEntry.getValue();

            if (sourceVM.getHost() == targetHost && sourceVM.isCreated() && !getRequestedVmList().contains(sourceVM)) {
                extraVmMappings.add(sourceVM);
            } else if (sourceVM.getHost() != targetHost && sourceVM.isCreated() && !getRequestedVmList().contains(sourceVM)) {
                boolean isHostNeededByAnyRequestedVms = cleanedSolution.entrySet().parallelStream()
                    .filter(vmHostEntry1 -> vmHostEntry1.getKey() != sourceVM)
                    .filter(vmHostEntry1 -> vmHostEntry1.getKey().getHost() != vmHostEntry1.getValue())
                    .anyMatch(vmHostEntry1 -> getRequestedVmList().contains(vmHostEntry1.getKey()) && vmHostEntry1.getValue() == sourceVM.getHost());

                if (!isHostNeededByAnyRequestedVms) {
                    //Removes the Vm migrations that are not necessary

                    extraVmMappings.add(sourceVM);
                }
            }
        }

        extraVmMappings.forEach(cleanedSolution::remove);

        return cleanedSolution;
    }

    /**
     * Copies the given pheromone information map to a new map
     *
     * @param pheromoneInformationMap the pheromone information map
     * @return a new copy of the given pheromone information map (deep copy)
     */
    protected Map<Vm, PheromoneInformationBetweenVmPairs> copyPheromoneInformation(final Map<Vm, PheromoneInformationBetweenVmPairs>
                                                                                         pheromoneInformationMap) {

        Map<Vm, PheromoneInformationBetweenVmPairs> newMap = new HashMap<>();

        pheromoneInformationMap.forEach((vm, PheromoneInformationBetweenVmPairs) -> {
            double initialPheromoneLevel = PheromoneInformationBetweenVmPairs.getInitialPheromoneValue();
            Map<Vm, Double> pheromoneHostMap = PheromoneInformationBetweenVmPairs.getHostPheromoneMap();
            newMap.put(vm, new PheromoneInformationBetweenVmPairs(vm, initialPheromoneLevel, pheromoneHostMap));
        });

        return newMap;
    }

    @Override
    public abstract Optional<Map<Vm, Host>> getBestSolution(final List<Vm> vmList, final Datacenter datacenter, final List<Host> allowedHostList);

    protected void setLastVmPheromoneInformationMap(final Map<Vm, PheromoneInformationBetweenVmPairs> lastVmPheromoneInformationMap) {
        if (lastVmPheromoneInformationMap == null || lastVmPheromoneInformationMap.isEmpty()) {
            throwIllegalState("The last Vm pheromone information map could not be null or empty",
                "setLastVmPheromoneInformationMap");
        }

        LAST_VM_PHEROMONE_INFORMATION_MAP = lastVmPheromoneInformationMap;
    }

    /**
     * Gets the host combined Vm list which is the combination of current host Vm list that are not moved out yet and also
     * the given temporary Vm list.
     *
     * @param host            the host
     * @param temporaryVmList the list of temporary Vms that might not be part of current host Vm list that are not move out.
     * @return the host current Vm list
     */
    protected List<Vm> getHostCurrentVmList(final Host host, final List<Vm> temporaryVmList) {
        List<Vm> vmList = new ArrayList<>();

        if (temporaryVmList != null && !temporaryVmList.isEmpty()) {
            vmList.addAll(temporaryVmList);
        }

        List<Vm> hostRemainingVmList = host.getVmList().parallelStream()
            .filter(vm -> temporaryVmList == null || temporaryVmList.isEmpty() || !temporaryVmList.contains(vm))
            .filter(vm -> !getRequestedVmList().contains(vm))
            .collect(Collectors.toList());

        vmList.addAll(hostRemainingVmList);

        return vmList;
    }
}
