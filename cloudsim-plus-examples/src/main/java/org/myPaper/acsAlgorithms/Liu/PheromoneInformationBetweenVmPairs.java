package org.myPaper.acsAlgorithms.Liu;

import org.cloudbus.cloudsim.vms.Vm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PheromoneInformationBetweenVmPairs {
    /**
     * @see #getVM()
     */
    private final Vm VM;

    /**
     * @see #getInitialPheromoneValue()
     */
    private final double INITIAL_PHEROMONE_VALUE;

    /**
     * @see #getVmPheromoneMap()
     * @see #getPheromoneValue(Vm)
     */
    private final Map<Vm, Double> vmPheromoneMap;

    /**
     * This class records the pheromone information of a specific VM and its other VM pairs (VM, VM). Note that this class could
     * not save the pheromone information between a VM and other hosts (VM, Host).
     *
     * @param vm                    the main VM
     * @param vmList                the list of VMs which are joining to the main VM
     * @param initialPheromoneValue the initial pheromone value between the main VM and its pairs
     */
    public PheromoneInformationBetweenVmPairs(final Vm vm,
                                              final List<Vm> vmList,
                                              final double initialPheromoneValue) {
        VM = vm;
        INITIAL_PHEROMONE_VALUE = initialPheromoneValue;
        vmPheromoneMap = new HashMap<>();
        createPheromoneMatrix(vmList);
    }

    /**
     * This class records the pheromone information of a specific VM and other VM pairs (VM, VM) according to a predefined
     * pheromone map.
     *
     * @param vm                    the main VM
     * @param initialPheromoneValue the initial pheromone value between the main VM and its pairs
     * @param vmPheromoneMap        the VM pheromone map
     */
    public PheromoneInformationBetweenVmPairs(final Vm vm, final double initialPheromoneValue, final Map<Vm, Double> vmPheromoneMap) {
        VM = vm;
        INITIAL_PHEROMONE_VALUE = initialPheromoneValue;
        this.vmPheromoneMap = new HashMap<>();

        vmPheromoneMap.forEach(this.vmPheromoneMap::put);
    }

    /**
     * Gets the main VM.
     *
     * @return the main VM
     */
    private Vm getVM() {
        return VM;
    }

    /**
     * Gets the initial pheromone value between the Vm and other Vms.
     *
     * @return the initial pheromone value
     */
    public double getInitialPheromoneValue() {
        return INITIAL_PHEROMONE_VALUE;
    }

    /**
     * Creates the pheromone information matrix between the main VM and its VM pairs.
     *
     * @param vmList the list of VMs are joining to the main VM
     */
    private void createPheromoneMatrix(final List<Vm> vmList) {
        vmList.forEach(vm -> {
            if (getVM() != vm) {
                vmPheromoneMap.put(vm, INITIAL_PHEROMONE_VALUE);
            }
        });
    }

    /**
     * Gets the pheromone map of connected pairs to this VM
     *
     * @return VM pheromone map
     */
    public Map<Vm, Double> getVmPheromoneMap() {
        return vmPheromoneMap;
    }

    /**
     * Gets the pheromone value between the main VM and another target VM.
     *
     * @param targetVm the target VM
     * @return the pheromone value between two VM pairs
     */
    public double getPheromoneValue(final Vm targetVm) {
        if (getVM() == targetVm) {
            throw new IllegalStateException("There is no pheromone value between a VM and its self!");
        }

        if (!vmPheromoneMap.containsKey(targetVm)) {
            throw new IllegalStateException("The is no pheromone deposition between the source VM and the target VM!");
        }

        return vmPheromoneMap.get(targetVm);
    }

    /**
     * Gets the pheromone map of connected Vms to this VM
     *
     * @return VM pheromone map
     */
    public Map<Vm, Double> getHostPheromoneMap() {
        return vmPheromoneMap;
    }

    /**
     * Updates the current pheromone Value between the main VM and its pair.
     *
     * @param target         the target VM
     * @param pheromoneValue the new amount of pheromone value
     */
    public void updatePheromoneValue(final Vm target, final double pheromoneValue) {
        vmPheromoneMap.replace(target, pheromoneValue);
    }
}
