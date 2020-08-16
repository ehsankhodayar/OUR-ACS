package org.myPaper.acsAlgorithms.Liu;

import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface Liu {
    /**
     * Gets the maximal iteration (or the maximum number of generations).
     *
     * @return the maximal iteration
     */
    int getMaximalIteration();

    /**
     * Sets the maximal iteration (or the maximum number of generations).
     *
     * @param maximalIteration the maximum number of generations
     */
    void setMaximalIteration(final int maximalIteration);

    /**
     * Gets the maximum number of ants (or populations).
     *
     * @return the maximum number of ants
     */
    int getNumberOfAnts();

    /**
     * Sets the maximum number of ants (or populations).
     *
     * @param numberOfAnts the maximum number of ants
     */
    void setNumberOfAnts(final int numberOfAnts);

    /**
     * Gets the q0.
     * It is used to control the exploitation and exploration behaviors of the ant (0 <= q0 <= 1).
     *
     * @return the q0
     */
    double getQ0();

    /**
     * Sets the q0.
     * It is used to control the exploitation and exploration behaviors of the ant (0 <= q0 <= 1).
     *
     * @param q0 the q0 parameter
     */
    void setQ0(final double q0);

    /**
     * Gets the local pheromone evaporation rate (0 < p < 1).
     *
     * @return the local pheromone evaporation rate
     */
    double getLocalPheromoneEvaporationRate();

    /**
     * Sets the local pheromone evaporation rate (0 < p < 1).
     *
     * @param localPheromoneEvaporationRate the local pheromone evaporation rate parameter
     */
    void setLocalPheromoneEvaporationRate(final double localPheromoneEvaporationRate);

    /**
     * Gets the global pheromone evaporation rate (0 < p0 < 1).
     *
     * @return the global pheromone evaporation rate
     */
    double getGlobalPheromoneEvaporationRate();

    /**
     * Sets the global pheromone evaporation rate (0 < p0 < 1).
     *
     * @param globalPheromoneEvaporationRate the local pheromone evaporation rate parameter
     */
    void setGlobalPheromoneEvaporationRate(final double globalPheromoneEvaporationRate);

    /**
     * Sets the beta parameter.
     * It is a predefined parameter that controls the relative importance of heuristic information (beta > 0)
     *
     * @return the beta parameter
     */
    double getBeta();

    /**
     * Gets the beta parameter.
     * It is a predefined parameter that controls the relative importance of heuristic information (beta > 0)
     *
     * @param beta the beta parameter
     */
    void setBeta(final double beta);

    /**
     * Throws a new illegal state exception including the given error message.
     *
     * @param errorMsg   the error message
     * @param callerName the name of the method where the error was happened
     */
    void throwIllegalState(final String errorMsg, final String callerName);

    /**
     * Gets the solution without its migration map. Not that it is useful when a local search is done.
     *
     * @param solution the target solution
     * @return the solution without migration map
     * @see #getMigrationMapOfSolution(Map)
     */
    Map<Vm, Host> getSolutionWithoutMigrationMap(final Map<Vm, Host> solution);

    /**
     * Gets the migration map of the given solution. Note that it is useful when a local search is done.
     *
     * @param solution the target solution
     * @return the migration map
     * @see #getSolutionWithoutMigrationMap(Map)
     */
    Map<Vm, Host> getMigrationMapOfSolution(final Map<Vm, Host> solution);

    /**
     * Gets the over-utilization threshold of hosts' CPUs.
     *
     * @return the over-utilization threshold
     */
    double getOverutilizationThreshold();

    /**
     * Sets the over-utilization threshold of hosts' CPUs and Memory.
     *
     * @param threshold the over-utilization threshold in range (0-1]
     */
    void setOverutilizationThreshold(final double threshold);

    /**
     * Gets the list of VMs are requested from the ACS algorithm to find a solution for them.
     *
     * @return the list of all VM creation requests
     */
    List<Vm> getRequestedVmList();

    /**
     * Gets the best generated solution.
     *
     * @param vmList the list of Vms which want to be created or migrated.
     * @param datacenter the datacenter
     * @param allowedHostList the list of allowed hosts
     * @return the best solution if available, empty solution otherwise
     */
    Optional<Map<Vm, Host>> getBestSolution(final List<Vm> vmList, final Datacenter datacenter, final List<Host> allowedHostList);

    /**
     * Gets the best solution among the given solution list.
     *
     * @param solutionList the list of solutions
     * @param nonDominatedSorting set true if a non dominated sorting is required
     * @param vmList the list of requested Vms
     * @return the best solution
     */
    Map<Vm, Host> getBestSolution(final List<Map<Vm, Host>> solutionList, final boolean nonDominatedSorting, List<Vm> vmList);
}
