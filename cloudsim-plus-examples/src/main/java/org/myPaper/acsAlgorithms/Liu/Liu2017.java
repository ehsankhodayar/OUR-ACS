package org.myPaper.acsAlgorithms.Liu;

import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.*;
import java.util.stream.Collectors;

public class Liu2017 extends LiuAbstract {
    private final int NA; //Size of external Archive

    /**
     * @see #getRequestedVmList()
     */
    private List<Vm> requestedVmList;

    /**
     * Unified Ant Colony System (UACS) Algorithm.
     *
     * <p>
     * OurAcs, X.F., Zhan, Z.H. and Zhang, J., 2017. An energy aware unified ant colony system for dynamic virtual machine
     * placement in cloud computing. Energies, 10(5), p.609.
     * </p>
     *
     * @param maximalIteration         maximum number of iterations
     * @param numberOfAnts             the number of ants in each iteration
     * @param q0                       the q0 is a constant in range [0,1] and is used to control the exploitation and exploration behaviors of a ant
     * @param lpd                      local pheromone decay (0 < p < 1)
     * @param gpd                      global pheromone decay (0 < p0 < 1)
     * @param beta                     a predefined parameter that controls the relative importance of heuristic information (beta > 0)
     * @param overutilizationThreshold the over-utilization threshold of hosts' CPUs in range (0-1]
     * @param na                       size of external archive
     */
    public Liu2017(int maximalIteration,
                   int numberOfAnts,
                   double q0,
                   double lpd,
                   double gpd,
                   double beta,
                   double overutilizationThreshold,
                   int na) {
        super(maximalIteration, numberOfAnts, q0, lpd, gpd, beta);
        setOverutilizationThreshold(overutilizationThreshold);
        NA = na;
    }

    /**
     * Runs the UACS Algorithm to find suitable hosts for the given VM list in the target datacenter. Note that if the
     * algorithm could not find any suitable solution (VM-Host mapping) for covering all the Vms in the list, it will return
     * back an empty solution. In other words, if all the VMs are not able to be created in the given datacenter, an empty
     * solution list will be returned by the UACS Algorithm.
     * Before using the returned solutions, you should check the migration list provided by {@link #getMigrationMapOfSolution(Map)}
     * in each solution if any migration is needed.
     *
     * @param vmList          a list of VMs that must be mapped to a new suitable list of hosts
     * @param datacenter      the target datacenter that the VM list is going to be created
     * @param allowedHostList the list of allowed hosts at the given datacenter
     * @return a new solution which is constructed by a list of VMs and hosts.
     */
    private List<Map<Vm, Host>> runUacsAlgorithm(final List<Vm> vmList, final Datacenter datacenter, List<Host> allowedHostList) {
        if (vmList.isEmpty() || allowedHostList.isEmpty()) {
            throwIllegalState("The given vmList or allowedHostList could not be empty", "runUacsAlgorithm");
        }

        if (datacenter == Datacenter.NULL) {
            throwIllegalState("The given datacenter could not be null", "runUacsAlgorithm");
        }

        requestedVmList = vmList;

        List<Map<Vm, Host>> externalArchive = new ArrayList<>();

        Map<Vm, PheromoneInformationBetweenVmPairs> pheromoneInformationVmPairsMap = new HashMap<>(); //VM pheromone information Map
        List<Vm> totalVmList = new ArrayList<>(vmList);
        allowedHostList.forEach(host -> {
            //filters repeated VMs (VMs on overloaded and underutilized hosts)
            List<Vm> hostVmList = host.getVmList().parallelStream()
                .filter(vm -> !vmList.contains(vm))
                .collect(Collectors.toList());

            totalVmList.addAll(hostVmList);
        });
        setInitialPheromoneLevel(totalVmList, pheromoneInformationVmPairsMap, allowedHostList.size());

        int M_min = allowedHostList.size(); //The maximum number of allowed hosts for the given VM list

        for (int iteration = 1; iteration <= getMaximalIteration(); iteration++) {
            int M_g = M_min > 1 ? M_min - 1 : 1; //The maximum number of allowed hosts for the given VM list in this iteration (or generation)
            List<Map<Vm, Host>> solutionMapList = runAnts(M_g, vmList, allowedHostList, pheromoneInformationVmPairsMap);

            if (solutionMapList.isEmpty()) {
                if (!externalArchive.isEmpty()) {
                    //Choose the generation best solution according to the minimum power consumption policy
                    Map<Vm, Host> generationBestSolution = Objects.requireNonNull(getSolutionsWithMinimumPowerConsumption(externalArchive)).get(0);
                    performGlobalPheromoneUpdating(generationBestSolution, pheromoneInformationVmPairsMap);
                }

                continue;
            }

            externalArchive.addAll(solutionMapList);
            externalArchive = sortNonDominatedSolutions(externalArchive);
            externalArchiveSizeController(externalArchive);

            //Choose the generation best solution according to the minimum power consumption policy
            Map<Vm, Host> generationBestSolution = Objects.requireNonNull(getSolutionsWithMinimumPowerConsumption(solutionMapList)).get(0);
            M_min = getSolutionNumberOfUsedHosts(generationBestSolution);
            performGlobalPheromoneUpdating(generationBestSolution, pheromoneInformationVmPairsMap);
        }

        setLastVmPheromoneInformationMap(pheromoneInformationVmPairsMap);
        return externalArchive;
    }

    /**
     * Runs the population to construct their solutions.
     *
     * @param M_g                              the maximum number of allowed hosts for the given VM list in this generation
     * @param vmList                           the list of VMs that are looking for a list of suitable hosts
     * @param allowedHostList                  the list of allowed hosts at the given datacenter
     * @param vmPheromoneInformationVmPairsMap the pheromone map between VM pairs
     * @return a list of feasible solutions
     */
    private List<Map<Vm, Host>> runAnts(final int M_g,
                                        final List<Vm> vmList,
                                        final List<Host> allowedHostList,
                                        final Map<Vm, PheromoneInformationBetweenVmPairs> vmPheromoneInformationVmPairsMap) {
        List<Map<Vm, Host>> solutionMapList = new ArrayList<>();
        Map<Vm, PheromoneInformationBetweenVmPairs> localVmPheromoneInformationMap = copyPheromoneInformation(vmPheromoneInformationVmPairsMap);

        //Starting the ants
        for (int ant = 1; ant <= getNumberOfAnts(); ant++) {
            /*The M_g is always one server lesser than M_min
            which means each ant should try to find a list of suitable hosts at least one less than M_min
             */

            Map<Vm, Host> currentAntSolutionMap;

            //The new added VMs to hosts will be saved at this map
            Map<Host, List<Vm>> hostNewVmListMap = new HashMap<>();

            //Iterating the VM list
            for (Vm vm : vmList) {
                //Finding a list of suitable hosts for this VM
                List<Host> suitableHostList =
                    getSuitableHostList(vm, hostNewVmListMap, allowedHostList);

                suitableHostList.subList(0, Math.min(suitableHostList.size(), M_g));

                List<Host> finalHostList;

                if (suitableHostList.isEmpty()) {
                    finalHostList = new ArrayList<>(allowedHostList);
                } else {
                    finalHostList = suitableHostList;
                }

                finalHostList.forEach(host -> hostNewVmListMap.putIfAbsent(host, new ArrayList<>()));

                Host host = selectHostForVmAccordingToConstructionRule(vm,
                    hostNewVmListMap,
                    finalHostList,
                    allowedHostList.size(),
                    localVmPheromoneInformationMap);
                hostNewVmListMap.get(host).add(vm);
            }

            currentAntSolutionMap = convertHostNewVmListMapToSolutionMap(hostNewVmListMap);
            performLocalPheromoneUpdating(currentAntSolutionMap, localVmPheromoneInformationMap);

            if (!isSolutionFeasible(currentAntSolutionMap)) {
                Map<Vm, Host> localSearchSolutionMap = performOemLocalSearchOnSolution(currentAntSolutionMap);
                localSearchSolutionMap = cleanSolution(localSearchSolutionMap);

                if (!isSolutionFeasible(localSearchSolutionMap)) {
                    currentAntSolutionMap = makeSolutionFeasibleWithMoreHosts(currentAntSolutionMap, allowedHostList);

                    if (isSolutionFeasible(currentAntSolutionMap)) {
                        solutionMapList.add(currentAntSolutionMap);
                    }
                } else {
                    solutionMapList.add(localSearchSolutionMap);
                }
            } else {
                solutionMapList.add(currentAntSolutionMap);
            }
        }

        return solutionMapList;
    }

    @Override
    protected double getHostHeuristic(Host host, List<Vm> hostNewVmList, Vm vm) {
        List<Vm> temporaryVmList = new ArrayList<>(hostNewVmList);
        temporaryVmList.add(vm);

        final double cpuWastage = getHostTotalMipsWastage(host, temporaryVmList);
        final double memoryWastage = getHostTotalMemoryWastage(host, temporaryVmList);

        if (cpuWastage > 1 || memoryWastage > 1) {
            throwIllegalState("The CPU or Memory wastage could not be 100% or more", "getHostHeuristic");
        }

        if (!isHostOverloaded(host, temporaryVmList)) {
            return 1 / (Math.abs(cpuWastage) + Math.abs(memoryWastage) + 1);
        } else {
            return 2 - Math.abs(cpuWastage) - Math.abs(memoryWastage);
        }
    }

    /**
     * If more hosts are available, tries to Make an infeasible solution feasible with providing more hosts
     * which has become infeasible again after a local search.
     *
     * @param solution        the target solution
     * @param allowedHostList list of allowed hosts at the given datacenter
     * @return a feasible solution if possible, the original solution otherwise
     */
    private Map<Vm, Host> makeSolutionFeasibleWithMoreHosts(final Map<Vm, Host> solution, final List<Host> allowedHostList) {
        Map<Host, List<Vm>> hostNewVmListMap = convertSolutionMapToHostNewVmListMap(solution);

        List<Host> notSelectedHosts = new ArrayList<>(allowedHostList);
        notSelectedHosts.removeAll(hostNewVmListMap.keySet());

        //The given solution is not able to switch to a feasible solution
        if (notSelectedHosts.isEmpty()) {
            return solution;
        }

        //Source host temporary new VM list assignment
        Map<Host, List<Vm>> sourceHostTemporaryNewVmList = new HashMap<>();

        //destination host temporary new VM list assignment
        Map<Host, List<Vm>> destinationHostTemporaryNewVmList = new HashMap<>();

        HostLoop:
        for (Map.Entry<Host, List<Vm>> hostVmListEntry : hostNewVmListMap.entrySet()) {
            Host sourceHost = hostVmListEntry.getKey();

            if (!isHostOverloaded(sourceHost, hostNewVmListMap.get(sourceHost))) {
                continue;
            }

            List<Vm> sourceHostNewVmList = new ArrayList<>(hostVmListEntry.getValue());

            sourceHostTemporaryNewVmList.putIfAbsent(sourceHost, new ArrayList<>(sourceHostNewVmList));

            VmLoop:
            for (Vm vm : sourceHostNewVmList) {
                for (Host destinationHost : notSelectedHosts) {
                    destinationHostTemporaryNewVmList.putIfAbsent(destinationHost, new ArrayList<>());

                    if (isHostOverloaded(destinationHost, destinationHostTemporaryNewVmList.get(destinationHost))) {
                        continue;
                    }

                    destinationHostTemporaryNewVmList.get(destinationHost).add(vm);

                    if (isHostOverloaded(destinationHost, destinationHostTemporaryNewVmList.get(destinationHost))) {
                        destinationHostTemporaryNewVmList.get(destinationHost).remove(vm);
                        continue;
                    }

                    sourceHostTemporaryNewVmList.get(sourceHost).remove(vm);

                    if (!isHostOverloaded(sourceHost, sourceHostTemporaryNewVmList.get(sourceHost))) {
                        continue HostLoop;
                    } else {
                        continue VmLoop;
                    }
                }
            }

            //The given solution is not able to switch into a feasible solution
            return solution;
        }

        Map<Vm, Host> newSolution = new HashMap<>(solution);

        destinationHostTemporaryNewVmList.forEach((host, vmList) -> vmList.forEach(vm -> newSolution.replace(vm, host)));

        return newSolution;
    }

    /**
     * Sorts the given solutions according to the non-dominated sortation policy.
     *
     * @param solutionList the list of solutions
     * @return non-dominated solutions in the first front (Pareto front)
     */
    private List<Map<Vm, Host>> sortNonDominatedSolutions(List<Map<Vm, Host>> solutionList) {
        Map<Map<Vm, Host>, Double> solutionPowerConsumptionMap = new HashMap<>();
        Map<Map<Vm, Host>, Integer> solutionNumberOfMigrationMap = new HashMap<>();

        solutionList.forEach(solution -> {
            solutionPowerConsumptionMap.put(solution, getSolutionTotalPowerConsumption(solution));
            solutionNumberOfMigrationMap.put(solution, getMigrationMapOfSolution(solution).size());
        });

        List<Map<Vm, Host>> nonDominatedSolutionsInFirstFront = new ArrayList<>();

        SourceLoop:
        for (Map<Vm, Host> sourceSolution : solutionList) {

            if (nonDominatedSolutionsInFirstFront.parallelStream()
                .anyMatch(solution -> solution.equals(sourceSolution))) {
                continue ;
            }
            for (Map<Vm, Host> targetSolution : solutionList) {
                if (sourceSolution.equals(targetSolution)) {
                    continue ;
                }

                if (solutionPowerConsumptionMap.get(targetSolution) <= solutionPowerConsumptionMap.get(sourceSolution) &&
                    solutionNumberOfMigrationMap.get(targetSolution) <= solutionNumberOfMigrationMap.get(sourceSolution)) {

                    if (solutionPowerConsumptionMap.get(targetSolution) < solutionPowerConsumptionMap.get(sourceSolution) ||
                        solutionNumberOfMigrationMap.get(targetSolution) < solutionNumberOfMigrationMap.get(sourceSolution)) {
                        continue SourceLoop;
                    }
                }
            }

            nonDominatedSolutionsInFirstFront.add(sourceSolution);
        }

        return nonDominatedSolutionsInFirstFront;
    }

    /**
     * Controls the size of external archive according to the {@link #NA}
     *
     * @param externalArchive the external archive list
     */
    private void externalArchiveSizeController(List<Map<Vm, Host>> externalArchive) {
        if (externalArchive.size() > NA) {
            int extraSize = externalArchive.size() - NA;
            List<Map<Vm, Host>> newExternalArchive = new ArrayList<>(externalArchive);
            List<Map<Vm, Host>> selectedSolutions = new ArrayList<>();
            List<Map<Vm, Host>> minPowerSolutionsMap = Objects.requireNonNull(getSolutionsWithMinimumPowerConsumption(externalArchive));
            List<Map<Vm, Host>> minMigrationSolutionsMap = Objects.requireNonNull(getSolutionsWithMinimumVmMigrations(externalArchive));

            if (minPowerSolutionsMap.size() + minMigrationSolutionsMap.size() >= externalArchive.size()) {
                return;
            }

            for (int i = 0; i < externalArchive.size(); i++) {
                Collections.shuffle(newExternalArchive);
                Map<Vm, Host> solution = newExternalArchive.get(0);
                newExternalArchive.remove(solution);

                if (!minPowerSolutionsMap.contains(solution) &&
                    !minMigrationSolutionsMap.contains(solution)) {
                    selectedSolutions.add(solution);

                    if (selectedSolutions.size() >= extraSize) {
                        break;
                    }
                }
            }

            externalArchive.removeAll(selectedSolutions);
        }
    }

    /**
     * Gets the total power consumption of the given solution in Watt-S.
     *
     * @param solution the target solution
     * @return the total power consumption in Watt-S
     */
    private double getSolutionTotalPowerConsumption(final Map<Vm, Host> solution) {
        Map<Host, List<Vm>> hostNewVmListMap = convertSolutionMapToHostNewVmListMap(solution);

        return hostNewVmListMap.keySet().parallelStream()
            .mapToDouble(host -> host.getPowerModel().getPower(getHostNewCpuUtilization(host, solution)))
            .sum();
    }

    /**
     * Gets the host's new CPU utilization according to its current mips usage and the new VM list.
     *
     * @param host     the target host
     * @param solution the new list of VMs for the target host which are not created yet
     * @return the new CPU utilization of the target host in scale [0-1]
     */
    private double getHostNewCpuUtilization(final Host host, final Map<Vm, Host> solution) {
        //The new CPU utilization of the given host
        List<Vm> temporaryVmLit = convertSolutionMapToHostNewVmListMap(solution).get(host);
        double utilization =
            getHostMipsUtilization(host, temporaryVmLit) / host.getTotalMipsCapacity();

        //Round the host's overall CPU utilization to the nearest floating pont (up to 4 places)
        utilization = roundDouble.apply(utilization, 4);

        //Checks if the calculated utilization is in the standard range [0-1] or not
        if (utilization > 1 || utilization < 0) {
            throwIllegalState("The host CPU utilization must be between 0 and 1 but " + utilization + " is given"
                , "getHostNewCpuUtilization");
        }

        return utilization;
    }

    /**
     * Gets the solutions with minimum power consumption. Note that all the returned solutions have the same amount of
     * power consumption.
     *
     * @param solutionList list of solutions
     * @return list of solutions with the minimum power consumption
     */
    private List<Map<Vm, Host>> getSolutionsWithMinimumPowerConsumption(final List<Map<Vm, Host>> solutionList) {
        if (solutionList.isEmpty()) {
            return null;
        }

        List<Map<Vm, Host>> selectedSolutions = new ArrayList<>();

        Map<Map<Vm, Host>, Double> solutionPowerConsumptionMap = new HashMap<>();
        solutionList.forEach(solution -> solutionPowerConsumptionMap.put(solution, getSolutionTotalPowerConsumption(solution)));

        double minimumPowerConsumption = Collections.min(solutionPowerConsumptionMap.entrySet(), Map.Entry.comparingByValue()).getValue();

        solutionPowerConsumptionMap.forEach((solution, powerConsumption) -> {
            if (powerConsumption <= minimumPowerConsumption) {
                selectedSolutions.add(solution);
            }
        });

        return selectedSolutions;
    }

    /**
     * Gets the solutions with minimum number of VM migrations. Note that all the returned solutions have the same number of
     * VM migrations.
     *
     * @param solutionList list of solutions
     * @return list of solutions with the minimum number of VM migrations
     */
    private List<Map<Vm, Host>> getSolutionsWithMinimumVmMigrations(final List<Map<Vm, Host>> solutionList) {
        if (solutionList.isEmpty()) {
            return null;
        }

        List<Map<Vm, Host>> selectedSolutions = new ArrayList<>();

        Map<Map<Vm, Host>, Integer> solutionNumberOfVmMigrationsMap = new HashMap<>();
        solutionList.forEach(solution -> solutionNumberOfVmMigrationsMap.put(solution, getMigrationMapOfSolution(solution).size()));

        int minimumNumberOfVmMigrations = Collections.min(solutionNumberOfVmMigrationsMap.entrySet(), Map.Entry.comparingByValue()).getValue();

        solutionNumberOfVmMigrationsMap.forEach((solution, powerConsumption) -> {
            if (powerConsumption <= minimumNumberOfVmMigrations) {
                selectedSolutions.add(solution);
            }
        });

        return selectedSolutions;
    }

    @Override
    public List<Vm> getRequestedVmList() {
        return requestedVmList;
    }

    @Override
    public Optional<Map<Vm, Host>> getBestSolution(final List<Vm> vmList, final Datacenter datacenter, final List<Host> allowedHostList) {
        List<Map<Vm, Host>> nonDominatedSolutionMapList = runUacsAlgorithm(vmList, datacenter, allowedHostList);
        if (nonDominatedSolutionMapList.isEmpty()) {
            return Optional.empty();
        }

        Map<Vm, Host> bestSolution = getBestSolution(nonDominatedSolutionMapList, false, getRequestedVmList());

        return Optional.of(bestSolution);
    }

    @Override
    public Map<Vm, Host> getBestSolution(List<Map<Vm, Host>> solutionList, boolean nonDominatedSorting, List<Vm> vmList) {
        Map<Vm, Host> bestSolution;

        requestedVmList = vmList;

        if (nonDominatedSorting) {
            solutionList = sortNonDominatedSolutions(solutionList);
        }

        List<Map<Vm, Host>> solutionsWithMinimumPowerConsumptionList =
            Objects.requireNonNull(getSolutionsWithMinimumPowerConsumption(solutionList));

        if (solutionsWithMinimumPowerConsumptionList.size() > 1) {
            bestSolution =
                Objects.requireNonNull(getSolutionsWithMinimumVmMigrations(solutionsWithMinimumPowerConsumptionList)).get(0);
        } else {
            bestSolution = solutionsWithMinimumPowerConsumptionList.get(0);
        }

        return bestSolution;
    }
}
