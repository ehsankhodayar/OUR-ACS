package org.myPaper.acsAlgorithms.Liu;

import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.*;
import java.util.stream.Collectors;

public class Liu2016 extends LiuAbstract {
    /**
     * @see #getRequestedVmList()
     */
    private List<Vm> requestedVmList;

    /**
     * Order Exchange and Migration (OEM) Ant Colony System (ACS) Algorithm.
     * <p>
     * OurAcs, X.F., Zhan, Z.H., Deng, J.D., Li, Y., Gu, T. and Zhang, J., 2016. An energy efficient ant colony system
     * for virtual machine placement in cloud computing.
     * IEEE transactions on evolutionary computation, 22(1), pp.113-128.
     * </p>
     *
     * @param maximalIteration maximum number of iterations
     * @param numberOfAnts     the number of ants in each iteration
     * @param q0               the q0 is constant in range [0,1] and is used to control the exploitation and exploration behaviors of the ant
     * @param lpd              local pheromone decay (0 < p < 1)
     * @param gpd              global pheromone decay (0 < p0 < 1)
     * @param beta             a predefined parameter that controls the relative importance of heuristic information (beta > 0)
     */
    public Liu2016(final int maximalIteration,
                   final int numberOfAnts,
                   final double q0,
                   final double lpd,
                   final double gpd,
                   final double beta) {
        super(maximalIteration, numberOfAnts, q0, lpd, gpd, beta);
    }

    /**
     * Runs the OEMACS Algorithm to find suitable hosts for the given VM list in the target datacenter. Note that if the
     * algorithm could not find a suitable solution (VM-Host mapping) for covering all the Vms in the list, it will return
     * back an empty solution. In other words, if all the VMs are not able to be created in the given datacenter, an empty
     * solution map will be returned by the OEMACS Algorithm.
     * Before using the returned solution, you should check the migration list provided by {@link #getMigrationMapOfSolution(Map)} if any migration is
     * needed.
     *
     * @param vmList          a list of VMs that must be mapped to a new suitable list of hosts
     * @param datacenter      the target datacenter that the VM list is going to be created
     * @param allowedHostList the list of allowed hosts at the given datacenter
     * @return a new solution which is constructed by a list of VMs and hosts.
     */
    private Map<Vm, Host> runOemAcsAlgorithm(final List<Vm> vmList, final Datacenter datacenter, List<Host> allowedHostList) {
        if (vmList.isEmpty() || allowedHostList.isEmpty()) {
            throwIllegalState("The given vmList or allowedHostList could not be empty", "runOemAcsAlgorithm");
        }

        if (datacenter == Datacenter.NULL) {
            throwIllegalState("The given datacenter could not be null", "runOemAcsAlgorithm");
        }

        Map<Vm, Host> globalBestSolutionMap = new HashMap<>();
        requestedVmList = vmList;

        Map<Vm, PheromoneInformationBetweenVmPairs> pheromoneInformationVmPairsMap = new HashMap<>(); //VM pheromone information Map

        List<Vm> totalVmList = new ArrayList<>(vmList);
        allowedHostList.forEach(host -> {
            //filters repeated VMs (VMs on overloaded and underutilized hosts)
            List<Vm> hostVmList = host.getVmList().parallelStream()
                .filter(vm -> !vmList.contains(vm))
                .collect(Collectors.toList());

            totalVmList.addAll(hostVmList);
        });

        setInitialPheromoneLevel(totalVmList, pheromoneInformationVmPairsMap, vmList.size());
        int M_min = vmList.size(); //The maximum number of allowed hosts for the given VM list

        //Staring the iterations
        for (int iteration = 1; iteration <= getMaximalIteration(); iteration++) {
            Map<Vm, Host> localBestSolution =
                runAnts(M_min, vmList, allowedHostList, pheromoneInformationVmPairsMap, globalBestSolutionMap);

            if (isSolutionFeasible(localBestSolution)) {
                globalBestSolutionMap = localBestSolution;
                M_min = getSolutionNumberOfUsedHosts(localBestSolution);
            } else {
                //Runs a local search
                Map<Vm, Host> localSearchSolution = performOemLocalSearchOnSolution(localBestSolution);

                if (isSolutionFeasible(localSearchSolution)) {
                    localBestSolution = cleanSolution(localSearchSolution);
                    globalBestSolutionMap = getSolutionWithoutMigrationMap(localBestSolution);
                    M_min = getSolutionNumberOfUsedHosts(localBestSolution);
                }
            }

            performGlobalPheromoneUpdating(localBestSolution, pheromoneInformationVmPairsMap);
        }

        return globalBestSolutionMap;
    }

    /**
     * Runs the population to construct their solution and returns  the bes local solution as an output.
     *
     * @param M_min                            the maximum number of allowed hosts for the given VM list
     * @param vmList                           the list of VMs that are looking for a list of suitable hosts
     * @param allowedHostList                  list of allowed hosts at the given datacenter
     * @param vmPheromoneInformationVmPairsMap the pheromone map between VM pairs
     * @param globalBestSolutionMap            the current global best solution map
     * @return the local best solution (VM-Host map)
     */
    private Map<Vm, Host> runAnts(final int M_min,
                                  final List<Vm> vmList,
                                  final List<Host> allowedHostList,
                                  final Map<Vm, PheromoneInformationBetweenVmPairs> vmPheromoneInformationVmPairsMap,
                                  final Map<Vm, Host> globalBestSolutionMap) {
        Map<Vm, Host> localBestSolution = globalBestSolutionMap;
        Map<Vm, PheromoneInformationBetweenVmPairs> localVmPheromoneInformationMap = copyPheromoneInformation(vmPheromoneInformationVmPairsMap);

        //Starting the ants
        for (int ant = 1; ant <= getNumberOfAnts(); ant++) {
            /*The M_t is always one server lesser than M_min
            which means each ant should try to find a list of suitable hosts at least one less than M_min
             */
            final int M_t = M_min > 1 ? M_min - 1 : M_min;

            //Shuffling the VM list
            List<Vm> shuffledVmList = shuffleVmList(vmList);

            //The new added VMs to hosts will be saved at this map
            Map<Host, List<Vm>> hostNewVmListMap = new HashMap<>();

            //It shows the generated solution by this ant obeyed both CPU and Memory (Ram) constraints or not
            boolean cpuAndMemoryConstraints = true;

            //Iterating the VM list
            for (Vm vm : shuffledVmList) {
                //Finding a list of suitable hosts for this VM
                List<Host> suitableHostList =
                    getSuitableHostList(vm, hostNewVmListMap, allowedHostList);

                suitableHostList.subList(0, Math.min(suitableHostList.size(), M_t));

                //Checks if the suitable host list is empty
                if (suitableHostList.isEmpty()) {
                    cpuAndMemoryConstraints = false;
                    Host suitableOverloadedHost =
                        findASuitableOverloadedHost(vm, hostNewVmListMap, allowedHostList);
                    hostNewVmListMap.putIfAbsent(suitableOverloadedHost, new ArrayList<>());
                    hostNewVmListMap.get(suitableOverloadedHost).add(vm);
                    continue;
                }

                suitableHostList.forEach(host -> hostNewVmListMap.putIfAbsent(host, new ArrayList<>()));

                Host host = selectHostForVmAccordingToConstructionRule(vm,
                    hostNewVmListMap,
                    suitableHostList,
                    vmList.size(),
                    localVmPheromoneInformationMap);
                hostNewVmListMap.get(host).add(vm);
            }

            Map<Vm, Host> newSolution = convertHostNewVmListMapToSolutionMap(hostNewVmListMap);

            //Selects the current best local solution
            localBestSolution = evaluateFitnessOfSolution(localBestSolution, newSolution, M_t, cpuAndMemoryConstraints);
            performLocalPheromoneUpdating(localBestSolution, localVmPheromoneInformationMap);
        }

        return localBestSolution;
    }

    private Host findASuitableOverloadedHost(final Vm vm,
                                             final Map<Host, List<Vm>> hostNewVmListMap,
                                             final List<Host> allowedHostList) {
        if (allowedHostList.size() == 1) {
            return allowedHostList.get(0);
        }

        //List of suitable overloaded hosts for the given VM
        Map<Host, Double> hostOverUtilizationMap = new HashMap<>();

        //checks hosts in the allowed host list for the given VM
        for (Host host : allowedHostList) {
            double totalPesOver = Math.abs(getHostTotalAvailablePes(host, hostNewVmListMap.get(host)) - vm.getNumberOfPes()) /
                (double) host.getNumberOfPes();
            double totalMemoryOver = Math.abs(getHostTotalAvailableMemory(host, hostNewVmListMap.get(host)) - vm.getRam().getCapacity()) /
                (double) host.getRam().getCapacity();

            double totalOverloaded = totalPesOver + totalMemoryOver;

            hostOverUtilizationMap.put(host, totalOverloaded);
        }

        Random random = new Random();
        double q = random.nextDouble() * 1;

        if (q <= getQ0()) {
            //Selects a host with the minimum amount of overloaded
            return Collections.min(hostOverUtilizationMap.entrySet(), Map.Entry.comparingByValue()).getKey();
        } else {
            //Selects a host based on the roulette wheel selection according to the probability distribution

            //Calculates the total amount of over utilization by all hosts
            final double totalOverUtilization = hostOverUtilizationMap.values().parallelStream().mapToDouble(value -> value).sum();

            //Construction the probability map
            Map<Host, Double> hostProbabilityMap = new LinkedHashMap<>();
            for (Map.Entry<Host, Double> hostProbabilityEntry : hostOverUtilizationMap.entrySet()) {
                double probability = 1 - (hostProbabilityEntry.getValue() / totalOverUtilization);
                hostProbabilityMap.put(hostProbabilityEntry.getKey(), probability);
            }

            Map<Host, Double> accumulatedSumMap = getAccumulatedSumMap(hostProbabilityMap);
            return selectHostBasedOnRouletteWheelMap(accumulatedSumMap);
        }
    }

    /**
     * Gets the heuristic for the given host and VM pair.
     *
     * @param host          the target host
     * @param hostNewVmList the host temporary VM list map
     * @param vm            the target VM
     * @return the heuristic
     */
    @Override
    protected double getHostHeuristic(final Host host, final List<Vm> hostNewVmList, final Vm vm) {
        List<Vm> temporaryVmList = new ArrayList<>(hostNewVmList);
        temporaryVmList.add(vm);

        final double cpuWastage = getHostTotalMipsWastage(host, temporaryVmList);
        final double memoryWastage = getHostTotalMemoryWastage(host, temporaryVmList);

        if (cpuWastage > 1 || memoryWastage > 1) {
            throwIllegalState("The CPU or Memory wastage could not be 100% or more", "getHostHeuristic");
        }

        final double heuristic = (1 - Math.abs(cpuWastage - memoryWastage)) / (Math.abs(cpuWastage) + Math.abs(memoryWastage) + 1);

        return vm.getHost() == host ? 5 * heuristic : heuristic;
    }

    /**
     * Evaluates the fitness of the given solution.
     *
     * @param currentBestSolution the best solution at the current iteration (local best solution)
     * @param newSolution         the new solution
     * @param M_t                 the maximum number of hosts
     * @param cmc                 cpu and memory constraints
     * @return the current best solution
     */
    private Map<Vm, Host> evaluateFitnessOfSolution(final Map<Vm, Host> currentBestSolution,
                                                    final Map<Vm, Host> newSolution,
                                                    final int M_t,
                                                    final boolean cmc) {
        Map<Vm, Host> newBestSolution;

        if (currentBestSolution.isEmpty()) {
            return newSolution;
        }

        if (newSolution.equals(currentBestSolution)) {
            return currentBestSolution;
        }

        int numberOfActiveHostsInNewSolution;

        if (cmc) {
            numberOfActiveHostsInNewSolution = getSolutionNumberOfUsedHosts(newSolution);
        } else {
            numberOfActiveHostsInNewSolution = M_t + 1;
        }

        final List<Host> hostListBestSolution = new ArrayList<>();
        for (Map.Entry<Vm, Host> vmHostEntry : currentBestSolution.entrySet()) {
            if (!hostListBestSolution.contains(vmHostEntry.getValue())) {
                hostListBestSolution.add(vmHostEntry.getValue());
            }
        }

        if (numberOfActiveHostsInNewSolution < hostListBestSolution.size()) {
            newBestSolution = newSolution;
        } else if (numberOfActiveHostsInNewSolution > hostListBestSolution.size()) {
            newBestSolution = currentBestSolution;
        } else {
            double currentBestSolutionResourceWastage = getSolutionTotalResourceWastage(currentBestSolution);
            double newSolutionResourceWastage = getSolutionTotalResourceWastage(newSolution);

            if (newSolutionResourceWastage < currentBestSolutionResourceWastage) {
                newBestSolution = newSolution;
            } else {
                newBestSolution = currentBestSolution;
            }
        }

        return newBestSolution;
    }

    @Override
    public List<Vm> getRequestedVmList() {
        return requestedVmList;
    }

    @Override
    public Optional<Map<Vm, Host>> getBestSolution(List<Vm> vmList, Datacenter datacenter, List<Host> allowedHostList) {
        Map<Vm, Host> solutionMap = runOemAcsAlgorithm(vmList, datacenter, allowedHostList);

        return Optional.of(solutionMap);
    }

    @Override
    public Map<Vm, Host> getBestSolution(final List<Map<Vm, Host>> solutionList, boolean nonDominatedSorting, List<Vm> vmList) {
        return null;
    }
}
