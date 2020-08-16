package org.myPaper.vm.instances;

import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.vm.VmInstanceAbstract;

/**
 * To create a new custom VM instance use this class.
 */
public class CustomVMInstance extends VmInstanceAbstract {
    private final int NUMBER_OF_PES;
    private final int MIPS;
    private final double MEMORY;

    /**
     * Creates a new VM instance base on the given configuration.
     * @param numberOfPes
     * @param mipsCapacity
     * @param memory
     */
    public CustomVMInstance(final int numberOfPes, final int mipsCapacity, final double memory) {
        NUMBER_OF_PES = numberOfPes;
        MIPS = mipsCapacity;
        MEMORY = memory;
    }

    @Override
    protected int getCpu() {
        return NUMBER_OF_PES;
    }

    @Override
    protected int getMIPS() {
        return MIPS;
    }

    @Override
    protected double getMemory() {
        return MEMORY;
    }

    @Override
    public Vm createVm() {
        return newVm();
    }
}
