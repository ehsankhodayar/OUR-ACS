package org.myPaper.acsAlgorithms.OurAcsAlgorithm;

import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.acsAlgorithms.DatacenterSolutionEntry;
import org.myPaper.datacenter.DatacenterPowerSupplyOverheadPowerAware;
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

        //Power consumption
        double minimumPowerConsumption = nonDominatedSolutions.stream()
            .map(datacenterSolutionEntry -> (DatacenterPro) datacenterSolutionEntry.getDatacenter())
            .mapToDouble(datacenterPro -> datacenterPro.getPowerSupplyOverheadPowerAware().getMinimumTotalPowerConsumption())
            .sum();
        double maximumPowerConsumption = nonDominatedSolutions.stream()
            .map(datacenterSolutionEntry -> (DatacenterPro) datacenterSolutionEntry.getDatacenter())
            .mapToDouble(datacenterPro -> datacenterPro.getPowerSupplyOverheadPowerAware().getMaximumTotalPowerConsumption())
            .sum();;

        //Carbon footprint
        double minimumCarbonFootprint = nonDominatedSolutions.stream()
            .map(datacenterSolutionEntry -> (DatacenterPro) datacenterSolutionEntry.getDatacenter())
            .mapToDouble(DatacenterPro::getMinimumPossibleCarbonFootprint)
            .sum();
        double maximumCarbonFootprint = nonDominatedSolutions.stream()
            .map(datacenterSolutionEntry -> (DatacenterPro) datacenterSolutionEntry.getDatacenter())
            .mapToDouble(DatacenterPro::getMaximumPossibleCarbonFootprint)
            .sum();;

        //Active hosts
        int minimumNumberOfActiveHosts = 0;
        int maximumNumberOfActiveHosts = nonDominatedSolutions.stream()
            .mapToInt(datacenterSolutionEntry -> datacenterSolutionEntry.getDatacenter().getHostList().size())
            .sum();

        //The number of Vm migrations
        int minimumNumberOfVmMigrations = 0;
        int maximumNumberOfVmMigrations = getRequestedVmList().size();

        //Total cost
        double totalMinimumCost = nonDominatedSolutions.stream()
            .map(datacenterSolutionEntry -> (DatacenterPro) datacenterSolutionEntry.getDatacenter())
            .mapToDouble(DatacenterPro::getMinimumPossibleCost)
            .sum();
        double totalMaximumCost = nonDominatedSolutions.stream()
            .map(datacenterSolutionEntry -> (DatacenterPro) datacenterSolutionEntry.getDatacenter())
            .mapToDouble(DatacenterPro::getMaximumPossibleCost)
            .sum();

        //Record the hypervolume of each solution in the following map
        Map<Map<Vm, Host>, Double> solutionHypervolumeMap = new HashMap<>();

        for (DatacenterSolutionEntry datacenterSolutionEntry : nonDominatedSolutions) {
            Datacenter datacenter = datacenterSolutionEntry.getDatacenter();
            Map<Vm, Host> solution = datacenterSolutionEntry.getSolution();

            double solutionTotalPowerConsumption = getSolutionTotalPowerConsumption(solution, datacenter);

            double totalPowerConsumptionNormalized =
                normalizeBetweenZeroAndOne(solutionTotalPowerConsumption, maximumPowerConsumption, minimumPowerConsumption);
            double totalCarbonFootprintNormalized =
                normalizeBetweenZeroAndOne(getTotalCarbonEmission(solutionTotalPowerConsumption, datacenter),
                    maximumCarbonFootprint, minimumCarbonFootprint);
            double numberOfActiveHostsNormalized =
                normalizeBetweenZeroAndOne(getSolutionNumberOfActiveHosts(solution, datacenter),
                    maximumNumberOfActiveHosts, minimumNumberOfActiveHosts);
            double numberOfVmMigrationsNormalized =
                normalizeBetweenZeroAndOne(getMigrationMapOfSolution(solution).size(), maximumNumberOfVmMigrations, minimumNumberOfVmMigrations);
            double totalCostNormalized =
                normalizeBetweenZeroAndOne(getTotalCost(solutionTotalPowerConsumption, datacenter), totalMaximumCost, totalMinimumCost);

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
        Map<DatacenterSolutionEntry, Double> solutionPowerConsumptionMap = new HashMap<>();

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

            double solutionTotalPowerConsumption = getSolutionTotalPowerConsumption(solution, datacenter);

            solutionPowerConsumptionMap.put(datacenterSolutionEntry, solutionTotalPowerConsumption);
            solutionCarbonFootprintMap.put(datacenterSolutionEntry, getTotalCarbonEmission(solutionTotalPowerConsumption, datacenter));
            solutionNumberOfActiveHostsMap.put(datacenterSolutionEntry, getSolutionNumberOfActiveHosts(solution, datacenter));
            solutionNumberOfMigrationMap.put(datacenterSolutionEntry, getMigrationMapOfSolution(solution).size());
            solutionTotalCost.put(datacenterSolutionEntry, getTotalCost(solutionTotalPowerConsumption, datacenter));
        }

        List<DatacenterSolutionEntry> nonDominatedSolutionsInFirstFront = new ArrayList<>();

        SourceLoop:
        for (DatacenterSolutionEntry entrySource : datacenterSolutionListMap) {
            Datacenter sourceDatacenter = entrySource.getDatacenter();
            Map<Vm, Host> sourceSolution = entrySource.getSolution();

            for (DatacenterSolutionEntry entryTarget : datacenterSolutionListMap) {
                Datacenter targetDatacenter = entryTarget.getDatacenter();
                Map<Vm, Host> targetSolution = entryTarget.getSolution();

                if (sourceDatacenter != targetDatacenter) {
                    if (solutionPowerConsumptionMap.get(entryTarget) <= solutionPowerConsumptionMap.get(entrySource) &&
                        solutionCarbonFootprintMap.get(entryTarget) <= solutionCarbonFootprintMap.get(entrySource) &&
                        solutionNumberOfActiveHostsMap.get(entryTarget) <= solutionNumberOfActiveHostsMap.get(entrySource) &&
                        solutionNumberOfMigrationMap.get(entryTarget) <= solutionNumberOfMigrationMap.get(entrySource) &&
                        solutionTotalCost.get(entryTarget) <= solutionTotalCost.get(entrySource)) {

                        if (solutionPowerConsumptionMap.get(entryTarget) < solutionPowerConsumptionMap.get(entrySource) ||
                            solutionCarbonFootprintMap.get(entryTarget) < solutionCarbonFootprintMap.get(entrySource) ||
                            solutionNumberOfActiveHostsMap.get(entryTarget) < solutionNumberOfActiveHostsMap.get(entrySource) ||
                            solutionNumberOfMigrationMap.get(entryTarget) < solutionNumberOfMigrationMap.get(entrySource) ||
                            solutionTotalCost.get(entryTarget) < solutionTotalCost.get(entrySource)) {
                            //The target solution dominates the source solution
                            continue SourceLoop;
                        }
                    }
                }
            }

            //None of the solutions dominate the source solution
            nonDominatedSolutionsInFirstFront.add(entrySource);
        }

        return nonDominatedSolutionsInFirstFront;
    }

    /**
     * Gets the total power consumption of the given solution in Watt-S. It consists the total power consumption of IT
     * infrastructures by the given hosts and the datacenter's overhead power consumption.
     *
     * @param solution the solution
     * @param datacenter the datacenter
     * @return the total power consumption in Watt-S (IT infrastructures' power consumption + datacenter's overhead power consumption)
     */
    public double getSolutionTotalPowerConsumption(final Map<Vm, Host> solution, Datacenter datacenter) {
        Map<Host, List<Vm>> hostNewVmListMap = convertSolutionMapToHostTemporaryVmListMap(solution);

        DatacenterPowerSupplyOverheadPowerAware powerSupply = (DatacenterPowerSupplyOverheadPowerAware) datacenter.getPowerSupply();

        double ITPowerConsumption = hostNewVmListMap.keySet().parallelStream()
            .mapToDouble(host -> host.getPowerModel().getPower(getHostNewCpuUtilization(host, solution)))
            .sum();

        double overheadPowerConsumption = powerSupply.getOverheadPowerConsumption(ITPowerConsumption, 0);

        return ITPowerConsumption + overheadPowerConsumption;
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
     * @param datacenter the datacenter
     * @return the number of active hosts
     */
    private int getSolutionNumberOfActiveHosts(final Map<Vm, Host> solution, Datacenter datacenter) {
        List<Host> newHostList = convertSolutionMapToHostTemporaryVmListMap(solution).keySet().parallelStream()
            .collect(Collectors.toList());

        List<Host> currentHostList = datacenter.getHostList().parallelStream()
            .filter(Host::isActive)
            .filter(host -> !newHostList.contains(host))
            .collect(Collectors.toList());

        return newHostList.size() + currentHostList.size();
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
            .mapToInt(vm -> vm.isCreated() && vm.getTotalCpuMipsUtilization() != 0 ? (int) vm.getTotalCpuMipsUtilization() :
                (int) vm.getTotalMipsCapacity())
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
}
