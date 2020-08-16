package org.myPaper.acsAlgorithms.OurAcsAlgorithm;

import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.acsAlgorithms.DatacenterSolutionEntry;
import org.myPaper.additionalClasses.SortMap;
import org.myPaper.datacenter.DatacenterPro;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * An ant colony system (ACS) algorithm that sees the VM placement problem as a combinatorial optimization problem and
 * solves both initial VM placement and VM consolidation problems by considering 5 different objectives. The corresponding
 * objectives are presented as follow:
 * <P> Objective 1: tries to minimize the total energy consumption by considering the power usage IT infrastructures and also
 * the overhead energy (such as the cooling systems). </P>
 * <P> Objective 2: minimizing the total carbon emission of the IT infrastructures and the overhead energy. </P>
 * <P> Objective 3: minimizing the total resource wastage (CPU MIPS utilization + RAM utilization). </P>
 * <P> Objective 4: minimizing the number of Vm migrations if the VMs are consolidating. </P>
 * <P> Objective 5: minimizing the total costs ($) consisting the total energy cost + the total carbon emission tax </P>
 * <p></p>
 * If you are using this algorithm please cite the following paper:
 */
public class OurAcs {
    final int G;//the number of generations
    final int A;//the the number of ants
    final int BETA;//is a predefined parameter that controls the relative importance of heuristic information (beta > 0)
    final double q0;//is a constant in range [0,1] and is used to control the exploitation and exploration behaviors of a ant
    final double PHEROMONE_DECAY;//the pheromone decay
    final double OVER_UTILIZATION_THRESHOLD;//the CPU over-utilization threshold

    /**
     * @see #getRequestedVmList()
     */
    List<Vm> requestedVmList;

    DatacenterPro datacenter;

    Map<Vm, Host> lastGenerationBestSolution;


    /**
     * An ant colony system (ACS) algorithm that sees the VM placement problem as a combinatorial optimization problem and
     * solves both initial VM placement and VM consolidation problems by considering 5 different objectives.
     *
     * @param g            the number of generations
     * @param a            the the number of ants
     * @param beta         is a predefined parameter that controls the relative importance of heuristic information (beta > 0)
     * @param q0           is a constant in range [0,1] and is used to control the exploitation and exploration behaviors of a ant
     * @param p            the pheromone decay
     * @param ovuThreshold the CPU over-utilization threshold
     */
    public OurAcs(final int g,
                  final int a,
                  final int beta,
                  final double q0,
                  final double p,
                  final double ovuThreshold) {
        G = g;
        A = a;
        BETA = beta;
        this.q0 = q0;
        PHEROMONE_DECAY = p;
        OVER_UTILIZATION_THRESHOLD = ovuThreshold;

        requestedVmList = new ArrayList<>();
    }

    /**
     * Runs our ACS Algorithm to find suitable hosts for the given VM list in the target datacenter. Note that if the
     * algorithm could not find any suitable solution (VM-Host mapping) for covering all the Vms in the list, it will return
     * back an empty solution. In other words, if all the VMs are not able to be created in the given datacenter, an empty
     * solution list will be returned by our ACS Algorithm.
     *
     * @param vmList          a list of VMs that must be mapped to a new suitable list of hosts
     * @param allowedHostList the list of allowed hosts at the given datacenter
     * @param datacenter      the target datacenter
     */
    private void runOurAcs(final List<Vm> vmList, final List<Host> allowedHostList, final DatacenterPro datacenter) {
        if (vmList == null || vmList.isEmpty() || allowedHostList == null || allowedHostList.isEmpty()) {
            throwIllegalState("The given vmList or allowedHostList could not be null or empty", "runOurAcs");
        }

        if (datacenter == Datacenter.NULL) {
            throwIllegalState("The given datacenter could not be null", "runOurAcs");
        }

        requestedVmList = vmList;
        lastGenerationBestSolution = new HashMap<>();
        this.datacenter = datacenter;

        KneePointSelectionPolicy kneePointSelectionPolicy = new KneePointSelectionPolicy(requestedVmList);
        List<DatacenterSolutionEntry> externalArchive = new ArrayList<>();

        double initialPheromoneValue = 1 / (double) allowedHostList.size();
        Map<Vm, PheromoneInformationBetweenVmHostPairs> pheromoneInformationMap = new HashMap<>();
        setInitialPheromoneValue(vmList, allowedHostList, pheromoneInformationMap, initialPheromoneValue);

        for (int generation = 0; generation < G; generation++) {
            List<Map<Vm, Host>> solutionMapList = runAnts(vmList, allowedHostList, pheromoneInformationMap);

            if (solutionMapList.isEmpty()) {
                if (!lastGenerationBestSolution.isEmpty()) {
                    //Choose the generation best solution according to the minimum power consumption policy
                    performGlobalPheromoneUpdating(lastGenerationBestSolution, pheromoneInformationMap);
                }

                continue;
            }

            List<DatacenterSolutionEntry> newDatacenterSolutionEntryList = solutionMapList.stream()
                .map(solution -> new DatacenterSolutionEntry(datacenter, solution))
                .collect(Collectors.toList());
            externalArchive.addAll(newDatacenterSolutionEntryList);
            externalArchive = kneePointSelectionPolicy.getNonDominatedSortation(externalArchive);

            //Choose the generation best solution according to the minimum power consumption policy
            lastGenerationBestSolution = kneePointSelectionPolicy.getKneePoint(externalArchive, false);
            performGlobalPheromoneUpdating(lastGenerationBestSolution, pheromoneInformationMap);
        }

    }

    /**
     * @param vmList                  the list of VMs that are looking for a list of suitable hosts
     * @param allowedHostList         the list of allowed hosts
     * @param pheromoneInformationMap the pheromone map between VM-Host pairs
     * @return a list of feasible solutions if available
     */
    private List<Map<Vm, Host>> runAnts(final List<Vm> vmList,
                                        final List<Host> allowedHostList,
                                        final Map<Vm, PheromoneInformationBetweenVmHostPairs> pheromoneInformationMap) {
        List<Map<Vm, Host>> solutionMapList = new ArrayList<>();
        Map<Vm, PheromoneInformationBetweenVmHostPairs> localVmPheromoneInformationMap = copyPheromoneInformation(pheromoneInformationMap);

        //Starting the ants
        for (int ant = 0; ant < A; ant++) {

            //Shuffle the Vm list
            List<Vm> shuffleVmList = shuffleVmList(vmList);

            //The new added VMs to hosts will be saved at this map
            Map<Host, List<Vm>> hostTemporaryVmListMap = new HashMap<>();

            //Iterating the VM list
            for (Vm vm : shuffleVmList) {
                //Finding a list of suitable hosts for this VM
                List<Host> suitableHostList =
                    getSuitableHostList(vm, hostTemporaryVmListMap, allowedHostList);

                if (suitableHostList.isEmpty()) {
                    continue;
                }

                suitableHostList.forEach(host -> hostTemporaryVmListMap.put(host, new ArrayList<>()));

                Host targetHost =
                    selectHostForVmAccordingToConstructionRule(vm, hostTemporaryVmListMap, suitableHostList, localVmPheromoneInformationMap);
                hostTemporaryVmListMap.get(targetHost).add(vm);
            }

            Map<Vm, Host> currentAntSolution = Objects.requireNonNull(convertHostTemporaryVmListMapToSolutionMap(hostTemporaryVmListMap));
            performLocalPheromoneUpdating(currentAntSolution, localVmPheromoneInformationMap);

            if (currentAntSolution.keySet().size() == vmList.size()) {
                solutionMapList.add(currentAntSolution);
            }

        }

        return solutionMapList;
    }

    /**
     * Gets the list of VMs are requested from the ACS algorithm to find a solution for them.
     *
     * @return the requested VM list
     */
    public List<Vm> getRequestedVmList() {
        return requestedVmList;
    }

    /**
     * Throws a new illegal state exception including the given error message.
     *
     * @param errorMsg   the error message
     * @param callerName the name of the method where the error was happened
     */
    private void throwIllegalState(String errorMsg, String callerName) {
        throw new IllegalStateException("Our ACS Algorithm: " + callerName + " " + errorMsg + "!");
    }

    /**
     * Sets the initial pheromone value between VM and host pairs.
     *
     * @param vmList                  the list of VMs
     * @param hostList                the list of hosts
     * @param pheromoneInformationMap the pheromone information map
     * @param value                   the initial pheromone value
     */
    private void setInitialPheromoneValue(final List<Vm> vmList,
                                          final List<Host> hostList,
                                          final Map<Vm, PheromoneInformationBetweenVmHostPairs> pheromoneInformationMap,
                                          final double value) {
        if (vmList.isEmpty()) {
            throwIllegalState("The given vmList could not be empty", "setInitialPheromoneLevel");
        }

        Map<Vm, PheromoneInformationBetweenVmHostPairs> pheromoneMap = vmList.parallelStream()
            .map(vm -> new PheromoneInformationBetweenVmHostPairs(vm, value, hostList))
            .collect(Collectors.toMap(PheromoneInformationBetweenVmHostPairs::getVm, pheromone -> pheromone));

        pheromoneInformationMap.clear();
        pheromoneInformationMap.putAll(pheromoneMap);
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
    private int getHostMipsUtilization(Host host, List<Vm> hostNewVmList) {
        List<Vm> vmList = getHostCurrentVmList(host, hostNewVmList);

        int totalReservedMips = vmList.parallelStream()
            .filter(vm -> vm.getHost() != host || getRequestedVmList().contains(vm))
            .mapToInt(vm -> vm.isCreated() ? (int) vm.getTotalCpuMipsUtilization() :
                (int) (vm.getTotalMipsCapacity() * OVER_UTILIZATION_THRESHOLD))
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
            .mapToInt(vm -> vm.isCreated() && vm.getRam().getAllocatedResource() != 0 ? (int) vm.getRam().getAllocatedResource() :
                (int) (vm.getRam().getCapacity() * OVER_UTILIZATION_THRESHOLD))
            .sum();

        int totalAmountOfReallocationVmsMemory = host.getVmList().parallelStream()
            .filter(vm -> getRequestedVmList().contains(vm))
            .mapToInt(vm -> vm.isCreated() ? (int) vm.getRam().getAllocatedResource() : (int) (vm.getRam().getCapacity() * OVER_UTILIZATION_THRESHOLD))
            .sum();

        int totalMemoryUtilization = host.getVmList().parallelStream()
            .mapToInt(vm -> vm.isCreated() ? (int) vm.getRam().getAllocatedResource() : (int) (vm.getRam().getCapacity() * OVER_UTILIZATION_THRESHOLD))
            .sum();

        return totalMemoryUtilization - totalAmountOfReallocationVmsMemory + totalReservedMemory;
    }

    /**
     * Gets a list of suitable hosts for the given VM.
     *
     * @param vm               the target VM
     * @param hostNewVmListMap the host temporary VM list map
     * @param allowedHostList  list of allowed hosts at this datacenter
     * @return a list of suitable hosts
     */
    private List<Host> getSuitableHostList(Vm vm, Map<Host, List<Vm>> hostNewVmListMap, List<Host> allowedHostList) {
        //List of suitable hosts for the given VM
        List<Host> suitableHostList = new ArrayList<>();

        for (Host host : allowedHostList) {
            //checks hosts in the allowed host list for the given VM
            int availablePes =
                (int) (getHostTotalAvailablePes(host, hostNewVmListMap.get(host)) - vm.getNumberOfPes());

            int availableMips =
                (int) (getHostTotalAvailableMIPS(host, hostNewVmListMap.get(host)) - vm.getTotalMipsCapacity());

            int availableMemory =
                (int) (getHostTotalAvailableMemory(host, hostNewVmListMap.get(host)) - vm.getRam().getCapacity());

            int availableStorage =
                (int) (getHostTotalAvailableStorage(host, hostNewVmListMap.get(host)) - vm.getStorage().getCapacity());

            int availableBw =
                (int) (getHostTotalAvailableBandwidth(host, hostNewVmListMap.get(host)) - vm.getBw().getCapacity());

            double currentMipsUtilization =
                (double) getHostMipsUtilization(host, hostNewVmListMap.get(host)) + vm.getTotalCpuMipsUtilization()
                    / host.getTotalMipsCapacity();

            if (availablePes >= 0 &&
                availableMips >= 0 &&
                currentMipsUtilization <= OVER_UTILIZATION_THRESHOLD &&
                availableMemory >= 0 &&
                availableStorage >= 0 && availableBw >= 0) {
                suitableHostList.add(host);
            }
        }

        return suitableHostList;
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
     * Gets the total resource wastage of the given solution.
     *
     * @param solution the solution
     * @return the total resource wastage
     */
    protected double getSolutionTotalResourceWastage(Map<Vm, Host> solution) {
        Map<Host, List<Vm>> hostNewVmList = convertSolutionMapToHostTemporaryVmListMap(solution);

        double totalWastage = 0;
        for (Host host : hostNewVmList.keySet()) {
            double hostMipsWastage =
                getHostTotalMipsWastage(host, hostNewVmList.get(host));

            double hostMemoryWastage =
                getHostTotalMemoryWastage(host, hostNewVmList.get(host));

            totalWastage += hostMipsWastage + hostMemoryWastage;
        }

        return totalWastage;
    }

    /**
     * Gets the Vm heuristic for the given host
     *
     * @param vm                  the Vm
     * @param host                the host
     * @param hostTemporaryVmList the temporary Vm list of the given host
     * @return the Vm heuristic for the given host
     */
    private double getHostHeuristic(final Vm vm, final Host host, final List<Vm> hostTemporaryVmList) {
        List<Vm> hostTemporaryVmListCopy = new ArrayList<>(hostTemporaryVmList);
        hostTemporaryVmListCopy.add(vm);

        final double cpuWastage = getHostTotalMipsWastage(host, hostTemporaryVmListCopy);
        final double memoryWastage = getHostTotalMemoryWastage(host, hostTemporaryVmListCopy);

        if (cpuWastage > 1 || memoryWastage > 1 || cpuWastage < 0 || memoryWastage < 0) {
            throwIllegalState("The CPU or Memory wastage must be >= 0 && < 1", "getHostHeuristic");
        }

        return 1 / (cpuWastage + memoryWastage + 1);
    }

    /**
     * Gets the VM assignment probability to the target host
     *
     * @param vm                      the target VM
     * @param targetHost              the target host
     * @param hostVmList              the host temporary Vm list
     * @param smpdh                   the sum of multiplication of the pheromone deposition and heuristic
     * @param pheromoneInformationMap the Pheromone information map
     * @return the assignment probability
     */
    private double getAssignmentProbability(Vm vm, Host targetHost,
                                            List<Vm> hostVmList,
                                            double smpdh,
                                            Map<Vm, PheromoneInformationBetweenVmHostPairs> pheromoneInformationMap) {
        if (smpdh == 0) {
            throwIllegalState("The sum of multiplication of Vm host pheromone value and heuristic could not be zero",
                "getAssignmentProbability");
        }

        double multiplicationOfPheromoneAndHeuristic =
            pheromoneInformationMap.get(vm).getPheromoneValue(targetHost) * Math.pow(getHostHeuristic(vm, targetHost, hostVmList), BETA);

        return multiplicationOfPheromoneAndHeuristic / smpdh;
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

        for (int i = 0; i < sortedProbabilityMap.size(); i++) {
            double sumProbability = 0;
            Host currentHost = (Host) sortedProbabilityMap.keySet().toArray()[i];
            for (int j = i; j < sortedProbabilityMap.size(); j++) {
                Host host = (Host) sortedProbabilityMap.keySet().toArray()[j];
                sumProbability += sortedProbabilityMap.get(host);
            }

            sumProbability = roundDouble.apply(sumProbability, 6);
            rouletteWheelMap.put(currentHost, sumProbability);
        }

        return SortMap.sortByValue(rouletteWheelMap, true);
    }

    /**
     * Selects a suitable host for the given VM according to the construction rule.
     *
     * @param vm                      the target VM
     * @param hostTemporaryVmListMap  the host temporary VM list
     * @param suitableHostList        a list of suitable hosts for the given VM
     * @param pheromoneInformationMap the vmPheromoneInformationMap
     * @return a suitable host
     */
    protected Host selectHostForVmAccordingToConstructionRule(Vm vm,
                                                              Map<Host, List<Vm>> hostTemporaryVmListMap,
                                                              List<Host> suitableHostList,
                                                              Map<Vm, PheromoneInformationBetweenVmHostPairs> pheromoneInformationMap) {
        if (suitableHostList.size() == 1) {
            return suitableHostList.get(0);
        }

        Random random = new Random();
        double q = random.nextDouble() * 1;

        if (q <= q0) {
            return Collections.max(suitableHostList,
                Comparator.comparing(targetHost ->
                    (pheromoneInformationMap.get(vm).getPheromoneValue(targetHost) *
                        Math.pow(getHostHeuristic(vm, targetHost, hostTemporaryVmListMap.get(targetHost)), BETA))));
        } else {

            //the sum of multiplication of the pheromone deposition and heuristic
            final double smpdh = suitableHostList.parallelStream()
                .mapToDouble(targetHost -> pheromoneInformationMap.get(vm).getPheromoneValue(targetHost) *
                    Math.pow(getHostHeuristic(vm, targetHost, hostTemporaryVmListMap.get(targetHost)), BETA))
                .sum();

            Map<Host, Double> probabilityMap = suitableHostList.parallelStream()
                .collect(Collectors.toMap(host -> host, host ->
                    getAssignmentProbability(vm, host, hostTemporaryVmListMap.get(host), smpdh, pheromoneInformationMap)));

            Map<Host, Double> rouletteWheelMap = getAccumulatedSumMap(probabilityMap);
            return selectHostBasedOnRouletteWheelMap(rouletteWheelMap);
        }
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
     * Converts the solution map to the host temporary Vm list map.
     *
     * @param solution the target solution
     * @return host temporary Vm list map
     */
    protected Map<Host, List<Vm>> convertSolutionMapToHostTemporaryVmListMap(Map<Vm, Host> solution) {
        Map<Host, List<Vm>> hostTemporaryVmListMap = new HashMap<>();

        for (Map.Entry<Vm, Host> vmHostEntry : solution.entrySet()) {
            Vm vm = vmHostEntry.getKey();
            Host host = vmHostEntry.getValue();

            hostTemporaryVmListMap.putIfAbsent(host, new ArrayList<>());
            hostTemporaryVmListMap.get(host).add(vm);
        }

        return hostTemporaryVmListMap;
    }

    /**
     * Converts the host temporary Vm list map to the solution map.
     *
     * @param hostTemporaryVmListMap the host temporary Vm list map
     * @return the solution
     */
    protected Map<Vm, Host> convertHostTemporaryVmListMapToSolutionMap(final Map<Host, List<Vm>> hostTemporaryVmListMap) {
        Map<Vm, Host> solution = new HashMap<>();
        for (Map.Entry<Host, List<Vm>> hostListEntry : hostTemporaryVmListMap.entrySet()) {
            for (Vm vm : hostListEntry.getValue()) {
                solution.put(vm, hostListEntry.getKey());
            }
        }

        return solution;
    }

    /**
     * Performs local updating between VM-Host pairs.
     *
     * @param solution                the target solution
     * @param pheromoneInformationMap the pheromone information map
     */
    protected void performLocalPheromoneUpdating(Map<Vm, Host> solution, Map<Vm, PheromoneInformationBetweenVmHostPairs> pheromoneInformationMap) {
        for (Map.Entry<Vm, Host> vmHostEntry : solution.entrySet()) {
            Vm vm = vmHostEntry.getKey();
            Host host = vmHostEntry.getValue();

            double currentPheromoneValue = pheromoneInformationMap.get(vm).getPheromoneValue(host);
            double newPheromoneValue = (1 - PHEROMONE_DECAY) * currentPheromoneValue +
                PHEROMONE_DECAY * pheromoneInformationMap.get(vm).getInitialPheromoneValue();

            pheromoneInformationMap.get(vm).updatePheromoneValue(host, newPheromoneValue);
        }
    }

    /**
     * Performs a global updating between the VM-Host pairs.
     *
     * @param solution                the solution
     * @param pheromoneInformationMap the pheromone information map
     */
    protected void performGlobalPheromoneUpdating(Map<Vm, Host> solution, Map<Vm, PheromoneInformationBetweenVmHostPairs> pheromoneInformationMap) {
        for (Map.Entry<Vm, Host> vmHostEntry : solution.entrySet()) {
            Vm vm = vmHostEntry.getKey();
            Host host = vmHostEntry.getValue();

            double currentPheromoneValue = pheromoneInformationMap.get(vm).getPheromoneValue(host);
            double reinforcementValue = (1 / (getSolutionTotalResourceWastage(solution) + 1));
            double newPheromoneValue = (1 - PHEROMONE_DECAY) * currentPheromoneValue + PHEROMONE_DECAY * reinforcementValue;
            pheromoneInformationMap.get(vm).updatePheromoneValue(host, newPheromoneValue);
        }
    }

    BiFunction<Double, Integer, Double> roundDouble = (value, places) -> {
        double a = 1;
        for (int i = 0; i < places; i++) {
            a *= 10;
        }
        return (double) Math.round(value * a) / a;
    };

    /**
     * Shuffles the given VM list.
     *
     * @param vmList the vm list which must be shuffled
     * @return a shuffled VM list
     */
    private List<Vm> shuffleVmList(List<Vm> vmList) {
        List<Vm> shuffledObjectList = new ArrayList<>(vmList);
        Collections.shuffle(shuffledObjectList);
        return shuffledObjectList;
    }

    /**
     * Copies the given pheromone information map to a new map
     *
     * @param pheromoneInformationMap the pheromone information map
     * @return a new copy of the given pheromone information map (deep copy)
     */
    private Map<Vm, PheromoneInformationBetweenVmHostPairs> copyPheromoneInformation(final Map<Vm, PheromoneInformationBetweenVmHostPairs>
                                                                                         pheromoneInformationMap) {

        Map<Vm, PheromoneInformationBetweenVmHostPairs> newMap = new HashMap<>();

        pheromoneInformationMap.forEach((vm, pheromoneInformationBetweenVmHostPairs) -> {
            double initialPheromoneLevel = pheromoneInformationBetweenVmHostPairs.getInitialPheromoneValue();
            Map<Host, Double> pheromoneHostMap = pheromoneInformationBetweenVmHostPairs.getHostPheromoneMap();
            newMap.put(vm, new PheromoneInformationBetweenVmHostPairs(vm, initialPheromoneLevel, pheromoneHostMap));
        });

        return newMap;
    }

    /**
     * Gets the best generated solution.
     *
     * @param vmList          the list of Vms which want to be created or migrated.
     * @param datacenter      the datacenter
     * @param allowedHostList the list of allowed hosts
     * @return the best solution if available, empty solution otherwise
     */
    public Optional<Map<Vm, Host>> getBestSolution(final List<Vm> vmList, final DatacenterPro datacenter, final List<Host> allowedHostList) {
        runOurAcs(vmList, allowedHostList, datacenter);
        return Optional.of(lastGenerationBestSolution);
    }
}
