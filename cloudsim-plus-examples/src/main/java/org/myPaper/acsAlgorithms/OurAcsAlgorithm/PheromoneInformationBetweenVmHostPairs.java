package org.myPaper.acsAlgorithms.OurAcsAlgorithm;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PheromoneInformationBetweenVmHostPairs {
    /**
     * @see #getVm()
     */
    private final Vm VM;

    /**
     * @see #getInitialPheromoneValue()
     */
    private final double INITIAL_PHEROMONE_VALUE;

    /**
     * @see #getHostPheromoneMap()
     */
    private final Map<Host, Double> HOST_PHEROMONE_MAP;

    /**
     * Gets the initial pheromone value between the Vm and other hosts.
     *
     * @return the initial pheromone value
     */
    public double getInitialPheromoneValue() {
        return INITIAL_PHEROMONE_VALUE;
    }

    /**
     * This class records the pheromone information of a specific VM and other host pairs (VM, host).
     *
     * @param vm the main VM
     * @param ipv the initial pheromone value between the given VM and the given host list
     * @param hostList the list of hosts that pheromone trails are spraying between them and the given VM
     */
    public PheromoneInformationBetweenVmHostPairs(final Vm vm, final double ipv, final List<Host> hostList) {
        VM = Objects.requireNonNull(vm);
        INITIAL_PHEROMONE_VALUE = ipv;
        HOST_PHEROMONE_MAP = new HashMap<>();

        createPheromoneMatrix(Objects.requireNonNull(hostList));
    }

    /**
     * This class records the pheromone information of a specific VM and other host pairs (VM, host) according to a predefined
     * pheromone map.
     *
     * @param vm the main VM
     * @param ipv the initial pheromone value between the given VM and the given host list
     * @param hostPheromoneMap the host pheromone map
     */
    public PheromoneInformationBetweenVmHostPairs(final Vm vm, final double ipv, final Map<Host, Double> hostPheromoneMap) {
        VM = Objects.requireNonNull(vm);
        INITIAL_PHEROMONE_VALUE = ipv;
        HOST_PHEROMONE_MAP = new HashMap<>();

        hostPheromoneMap.forEach(HOST_PHEROMONE_MAP::put);
    }

    /**
     * Gets the VM.
     *
     * @return the main VM
     */
    public Vm getVm() {
        return VM;
    }

    /**
     * Creates the pheromone information matrix between the VM and its hosts pairs.
     *
     * @param hostList the list of VMs are joining to the main VM
     */
    private void createPheromoneMatrix(final List<Host> hostList) {
        if (hostList.isEmpty()) {
            throw new IllegalStateException("The given host list could not be empty or null!");
        }

        hostList.forEach(host -> {
            if (!HOST_PHEROMONE_MAP.containsKey(host)) {
                HOST_PHEROMONE_MAP.put(host, INITIAL_PHEROMONE_VALUE);
            }
        });
    }

    /**
     * Gets the pheromone map of connected hosts to this VM
     *
     * @return host pheromone map
     */
    public Map<Host, Double> getHostPheromoneMap() {
        return HOST_PHEROMONE_MAP;
    }

    /**
     * Gets the pheromone value between the VM and the target host.
     *
     * @param targetHost the target host
     * @return the pheromone value between the VM and the target host
     */
    public double getPheromoneValue(final Host targetHost) {
        if (!getHostPheromoneMap().containsKey(targetHost)) {
            throw new IllegalStateException("The is no pheromone deposition between the VM and the target host!");
        }

        return getHostPheromoneMap().get(targetHost);
    }

    /**
     * Updates the current pheromone Value between the VM and the target host.
     *
     * @param targetHost         the target host
     * @param pheromoneValue the new amount of pheromone value
     */
    public void updatePheromoneValue(final Host targetHost, final double pheromoneValue) {
        if (!HOST_PHEROMONE_MAP.containsKey(targetHost)) {
            throw new IllegalStateException("The is no pheromone deposition between the VM and the target host!");
        }

        HOST_PHEROMONE_MAP.replace(targetHost, pheromoneValue);
    }
}
