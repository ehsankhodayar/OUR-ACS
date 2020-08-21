package org.myPaper.acsAlgorithms.OurAcsAlgorithm;

import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.acsAlgorithms.DatacenterSolutionEntry;
import org.myPaper.additionalClasses.NormalizeZeroOne;
import org.myPaper.datacenter.DatacenterPro;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class KneePointSelectionPolicy {
    /**
     * @see #getRequestedVmList()
     */
    private final List<Vm> requestedVmList;

    public KneePointSelectionPolicy(final List<Vm> vmList) {
        requestedVmList = vmList;
    }

    /**
     * Selects a solution according to the knee point selection policy.
     *
     * Intuitively, a knee point in the Pareto optimal front refers to the solution with the maximum marginal rates of return,
     * which means that a small improvement in one objective of such a solution is accompanied by a severe degradation in at least another.
     * <p>
     * Zhang, X., Tian, Y. and Jin, Y., 2014. A knee point-driven evolutionary algorithm for many-objective optimization.
     * IEEE Transactions on Evolutionary Computation, 19(6), pp.761-776.
     * </p>
     * Note that we choose the knee point based on the solution with the highest hypervolume.
     *
     * @param datacenterSolutionListMap the list of different solutions at different datacenters
     * @param nonDominatedSortation set true if a non dominated sortation is needed
     * @return the knee point
     */
    public Map<Vm, Host> getKneePoint(final List<DatacenterSolutionEntry> datacenterSolutionListMap, final boolean nonDominatedSortation) {
        //Reference Point
        double referencePint = 1;

        //Do a non dominated sortation and get the pareto front
        List<DatacenterSolutionEntry> nonDominatedSolutions;
        if (nonDominatedSortation) {
            nonDominatedSolutions = getNonDominatedSortation(datacenterSolutionListMap);
        }else {
            nonDominatedSolutions = datacenterSolutionListMap;
        }

        if (nonDominatedSolutions.size() == 1) {
            return nonDominatedSolutions.get(0).getSolution();
        };

        double minimumIncreaseInPowerConsumption = 0;

        double maximumIncreaseInPowerConsumption = 3600_000;//1 KWh

        //Carbon footprint
        double minimumIncreaseInCarbonFootprint = 0;

        double maximumIncreaseCarbonFootprint = datacenterSolutionListMap.parallelStream()
            .map(DatacenterSolutionEntry::getDatacenter)
            .mapToDouble(datacenter -> getDatacenterPro(datacenter).getTotalCarbonFootprint(maximumIncreaseInPowerConsumption / 3600))
            .max()
            .orElse(0) + 1;

        //Active hosts
        int minimumNumberOfActiveHosts = 0;
        int maximumNumberOfActiveHosts = datacenterSolutionListMap.parallelStream()
            .map(DatacenterSolutionEntry::getSolution)
            .mapToInt(solution -> convertSolutionMapToHostTemporaryVmListMap(solution).keySet().size())
            .max()
            .orElse(0) + 1;

        //The number of Vm migrations
        int minimumNumberOfVmMigrations = 0;
        int maximumNumberOfVmMigrations = getMaximumNumberOfVmMigrationsAccordingToSolutionSet(datacenterSolutionListMap) + 1;

        //Total cost
        double minimumIncreaseInCost = 0;

        double maximumIncreaseInCost = datacenterSolutionListMap.parallelStream()
            .map(DatacenterSolutionEntry::getDatacenter)
            .mapToDouble(datacenter ->
                getDatacenterPro(datacenter).getTotalEnergyCost(maximumIncreaseInPowerConsumption / 300) +
                getDatacenterPro(datacenter).getTotalCarbonFootprint(maximumIncreaseInPowerConsumption / 3600))
            .max()
            .orElse(0) + 1;

        //Record the hypervolume of each solution in the following map
        Map<Map<Vm, Host>, Double> solutionHypervolumeMap = new HashMap<>();
        for (DatacenterSolutionEntry datacenterSolutionEntry : nonDominatedSolutions) {
            Datacenter datacenter = datacenterSolutionEntry.getDatacenter();
            Map<Vm, Host> solution = datacenterSolutionEntry.getSolution();

            double solutionTotalPowerConsumption = getSolutionTotalIncreasePowerConsumption(solution, datacenter);

            double totalPowerConsumptionNormalized =
                NormalizeZeroOne.normalize(solutionTotalPowerConsumption, maximumIncreaseInPowerConsumption, minimumIncreaseInPowerConsumption);
            double totalCarbonFootprintNormalized =
                NormalizeZeroOne.normalize(getTotalCarbonEmission(solutionTotalPowerConsumption, datacenter),
                    maximumIncreaseCarbonFootprint, minimumIncreaseInCarbonFootprint);
            double numberOfActiveHostsNormalized =
                NormalizeZeroOne.normalize(getSolutionNumberOfActiveHosts(solution),
                    maximumNumberOfActiveHosts, minimumNumberOfActiveHosts);
            double numberOfVmMigrationsNormalized =
                NormalizeZeroOne.normalize(getMigrationMapOfSolution(solution).size(), maximumNumberOfVmMigrations, minimumNumberOfVmMigrations);
            double totalCostNormalized =
                NormalizeZeroOne.normalize(getTotalCost(solutionTotalPowerConsumption, datacenter), maximumIncreaseInCost, minimumIncreaseInCost);

            double hypervolume = (referencePint - totalPowerConsumptionNormalized) *
                (referencePint - totalCarbonFootprintNormalized) *
                (referencePint - numberOfActiveHostsNormalized) *
                (referencePint - numberOfVmMigrationsNormalized) *
                (referencePint - totalCostNormalized);

            solutionHypervolumeMap.put(solution, hypervolume);
        }

        //Return the solution with the highest amount of hypervolume
        return Collections.max(solutionHypervolumeMap.entrySet(), Map.Entry.comparingByValue()).getKey();
    }

    /**
     * Sorts the given solutions according to the non-dominated sortation policy. It considers different objective
     * functions for its sortation. Note that this method only returns the pareto front (non-dominated optimal solutions).
     *
     * @param datacenterSolutionListMap the list of different solutions at different datacenters
     * @return non-dominated solutions in the first front (Pareto front)
     */
    public List<DatacenterSolutionEntry> getNonDominatedSortation(final List<DatacenterSolutionEntry> datacenterSolutionListMap) {
        //The It infrastructures power consumption + datacenter overhead power consumption
        Map<DatacenterSolutionEntry, Double> solutionIncreaseInPowerConsumptionMap = new HashMap<>();

        //The carbon footprint of power consumption
        Map<DatacenterSolutionEntry, Double> solutionCarbonFootprintMap = new HashMap<>();

        //Total number of active hosts at the target data center by the given solution
        Map<DatacenterSolutionEntry, Integer> solutionNumberOfActiveHostsMap = new HashMap<>();

        //Total number of solution's Vm migrations
        Map<DatacenterSolutionEntry, Integer> solutionNumberOfMigrationMap = new HashMap<>();

        //The energy cost ($) + the carbon tax ($)
        Map<DatacenterSolutionEntry, Double> solutionTotalCost = new HashMap<>();

        for (DatacenterSolutionEntry datacenterSolutionEntry : datacenterSolutionListMap) {
            Datacenter datacenter = datacenterSolutionEntry.getDatacenter();
            Map<Vm, Host> solution = datacenterSolutionEntry.getSolution();

            double solutionTotalIncreaseInPowerConsumption = getSolutionTotalIncreasePowerConsumption(solution, datacenter);

            solutionIncreaseInPowerConsumptionMap.put(datacenterSolutionEntry, solutionTotalIncreaseInPowerConsumption);
            solutionCarbonFootprintMap.put(datacenterSolutionEntry, getTotalCarbonEmission(solutionTotalIncreaseInPowerConsumption, datacenter));
            solutionNumberOfActiveHostsMap.put(datacenterSolutionEntry, getSolutionNumberOfActiveHosts(solution));
            solutionNumberOfMigrationMap.put(datacenterSolutionEntry, getMigrationMapOfSolution(solution).size());
            solutionTotalCost.put(datacenterSolutionEntry, getTotalCost(solutionTotalIncreaseInPowerConsumption, datacenter));
        }

        List<DatacenterSolutionEntry> nonDominatedSolutionsInFirstFront = new ArrayList<>();

        SourceLoop:
        for (DatacenterSolutionEntry entrySource : datacenterSolutionListMap) {
            Map<Vm, Host> sourceSolution = entrySource.getSolution();

            if (nonDominatedSolutionsInFirstFront.parallelStream()
                .anyMatch(datacenterSolutionEntry -> datacenterSolutionEntry.getSolution().equals(sourceSolution))) {
                continue ;
            }

            for (DatacenterSolutionEntry entryTarget : datacenterSolutionListMap) {
                Map<Vm, Host> targetSolution = entryTarget.getSolution();

                if (sourceSolution.equals(targetSolution)) {
                    continue ;
                }

                if (solutionIncreaseInPowerConsumptionMap.get(entryTarget) <= solutionIncreaseInPowerConsumptionMap.get(entrySource) &&
                    solutionCarbonFootprintMap.get(entryTarget) <= solutionCarbonFootprintMap.get(entrySource) &&
                    solutionNumberOfActiveHostsMap.get(entryTarget) <= solutionNumberOfActiveHostsMap.get(entrySource) &&
                    solutionNumberOfMigrationMap.get(entryTarget) <= solutionNumberOfMigrationMap.get(entrySource) &&
                    solutionTotalCost.get(entryTarget) <= solutionTotalCost.get(entrySource)) {

                    if (solutionIncreaseInPowerConsumptionMap.get(entryTarget) < solutionIncreaseInPowerConsumptionMap.get(entrySource) ||
                        solutionCarbonFootprintMap.get(entryTarget) < solutionCarbonFootprintMap.get(entrySource) ||
                        solutionNumberOfActiveHostsMap.get(entryTarget) < solutionNumberOfActiveHostsMap.get(entrySource) ||
                        solutionNumberOfMigrationMap.get(entryTarget) < solutionNumberOfMigrationMap.get(entrySource) ||
                        solutionTotalCost.get(entryTarget) < solutionTotalCost.get(entrySource)) {
                        //The target solution dominates the source solution
                        continue SourceLoop;
                    }
                }
            }

            //None of the solutions dominate the source solution
            nonDominatedSolutionsInFirstFront.add(entrySource);
        }

        return nonDominatedSolutionsInFirstFront;
    }

    /**
     * Gets the total increase in power consumption of the given solution in Watt-S. It consists the total power consumption of IT
     * infrastructures by the given hosts and the datacenter's overhead power consumption.
     *
     * @param solution the solution
     * @param datacenter the datacenter
     * @return the total power consumption in Watt-S (IT infrastructures' power consumption + datacenter's overhead power consumption)
     */
    public double getSolutionTotalIncreasePowerConsumption(final Map<Vm, Host> solution, Datacenter datacenter) {
        Map<Host, List<Vm>> hostNewVmListMap = convertSolutionMapToHostTemporaryVmListMap(solution);

        double currentITPowerConsumption = getSolutionCurrentITPowerConsumption(solution);
        double newITPowerConsumption = hostNewVmListMap.keySet().parallelStream()
            .mapToDouble(host -> host.getPowerModel().getPower(getHostNewCpuUtilization(host, solution)))
            .sum();

        double extraITPowerConsumption = newITPowerConsumption - currentITPowerConsumption;

        double solutionOverhead = extraITPowerConsumption * (getDatacenterPro(datacenter).getDatacenterDynamicPUE(extraITPowerConsumption) - 1);

        return extraITPowerConsumption + solutionOverhead;
    }

    /**
     * Gets the solution current power consumption (solution without temporary Vms) in Watt-Sec.
     *
     * @param solution the solution
     * @return the solution current power consumption in Watt-Sec
     */
    private double getSolutionCurrentITPowerConsumption(final Map<Vm, Host> solution) {
        Map<Host, List<Vm>> hostNewVmListMap = convertSolutionMapToHostTemporaryVmListMap(solution);

        return hostNewVmListMap.keySet().parallelStream()
            .mapToDouble(host -> host.isActive() ? host.getPowerModel().getPower() : 10)
            .sum();
    }

    /**
     * Gets the Maximum number of Vm migrations that a solution in the solution set consists.
     *
     * @param solutionEntryList the solution set
     * @return the maximum number of Vm migrations
     */
    private int getMaximumNumberOfVmMigrationsAccordingToSolutionSet(final List<DatacenterSolutionEntry> solutionEntryList) {
        int max = 0;

        for (DatacenterSolutionEntry datacenterSolutionEntry : solutionEntryList) {
            int numberOfMigrations = (int) datacenterSolutionEntry.getSolution().entrySet().parallelStream()
                .filter(vmHostEntry -> vmHostEntry.getKey().getHost() != Host.NULL)
                .filter(vmHostEntry -> vmHostEntry.getKey().getHost() != vmHostEntry.getValue())
                .count();

            max = Math.max(max, numberOfMigrations);
        }

        return max;
    }

    /**
     * Gets the total carbon emission in ton.
     *
     * @param powerConsumption the power consumption in Watt-Sec
     * @param datacenter the datacenter
     * @return the total amount of carbon emission in ton
     */
    private double getTotalCarbonEmission(final double powerConsumption, Datacenter datacenter) {
        DatacenterPro datacenterPro = (DatacenterPro) datacenter;
        double energyConsumption = powerConsumption / 3600;//The energy consumption in Watt-h

        return datacenterPro.getTotalCarbonFootprint(energyConsumption);
    }

    /**
     * Gets solution the total number of active hosts at the target datacenter.
     *
     * @param solution the solution
     * @return the number of active hosts
     */
    private int getSolutionNumberOfActiveHosts(final Map<Vm, Host> solution) {
        List<Host> newHostList = new ArrayList<>(convertSolutionMapToHostTemporaryVmListMap(solution).keySet());

        return newHostList.size();
    }

    /**
     * Gets the migration map of the given solution.
     *
     * @param solution the target solution
     * @return the migration map
     */
    private Map<Vm, Host> getMigrationMapOfSolution(final Map<Vm, Host> solution) {
        return solution.entrySet().parallelStream()
            .filter(vmHostEntry -> vmHostEntry.getKey().isCreated())
            .filter(vmHostEntry -> vmHostEntry.getKey().getHost() != vmHostEntry.getValue())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Gets the total cost in Dollars according to the given power consumption and carbon footprint.
     *
     * @param powerConsumption the power consumption in Watt-Sec
     * @param datacenter
     * @return the total cost in Dollars
     */
    private double getTotalCost(final double powerConsumption, Datacenter datacenter) {
        DatacenterPro datacenterPro = (DatacenterPro) datacenter;

        double energyConsumption = powerConsumption / 3600;//In Watt-h
        double energyCost = datacenterPro.getTotalEnergyCost(energyConsumption);
        double carbonTax = datacenterPro.getTotalCarbonTax(energyConsumption);

        return energyCost + carbonTax;
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
     * Gets the host's new CPU utilization according to its current mips usage and the new VM list.
     *
     * @param host     the target host
     * @param solution the new list of VMs for the target host which are not created yet
     * @return the new CPU utilization of the target host in scale [0-1]
     */
    protected double getHostNewCpuUtilization(final Host host, final Map<Vm, Host> solution) {
        //The new CPU utilization of the given host
        List<Vm> temporaryVmLit = convertSolutionMapToHostTemporaryVmListMap(solution).get(host);
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
            .mapToInt(vm -> vm.isCreated() ? (int) vm.getTotalCpuMipsUtilization() : (int) vm.getTotalMipsCapacity())
            .sum();

        int totalAmountOfReallocationVmsMips = host.getVmList().parallelStream()
            .filter(vm -> getRequestedVmList().contains(vm))
            .mapToInt(vm -> (int) vm.getTotalCpuMipsUtilization())
            .sum();

        return (int) host.getCpuMipsUtilization() - totalAmountOfReallocationVmsMips + totalReservedMips;
    }

    /**
     * Gets the list of VMs are requested from the ACS algorithm to find a solution for them.
     *
     * @return the list of all VM creation requests
     */
    private List<Vm> getRequestedVmList() {
        return requestedVmList;
    }

    /**
     * Throws a new illegal state exception including the given error message.
     *
     * @param errorMsg   the error message
     * @param callerName the name of the method where the error was happened
     */
    private void throwIllegalState(String errorMsg, String callerName) {
        throw new IllegalStateException("Knee point selection policy: " + callerName + " " + errorMsg + "!");
    }

    BiFunction<Double, Integer, Double> roundDouble = (value, places) -> {
        double a = 1;
        for (int i = 0; i < places; i++) {
            a *= 10;
        }
        return (double) Math.round(value * a) / a;
    };

    /**
     * Gets the host combined Vm list which is the combination of current host Vm list that are not moved out yet and also
     * the given temporary Vm list.
     *
     * @param host            the host
     * @param temporaryVmList the list of temporary Vms that might not be part of current host Vm list that are not move out.
     * @return the host current Vm list
     */
    private List<Vm> getHostCurrentVmList(final Host host, final List<Vm> temporaryVmList) {
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

    private DatacenterPro getDatacenterPro(Datacenter datacenter) {
        return (DatacenterPro) datacenter;
    }
}
