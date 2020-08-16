package org.myPaper.vm;

import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;

/**
 * Each data center contains the following predefined
 * vm instances (according to
 * <a href="https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-types.html">amazon web service instance types</a>):
 *
 * <table>
 *     <thead>
 *         <tr>
 *             <td>Type</td>
 *             <td>CPU</td>
 *             <td>MIPS</td>
 *             <td>Memory (GB)</td>
 *         </tr>
 *     </thead>
 *     <tbody>
 *         <tr>
 *             <td>a1_medium</td>
 *             <td>1</td>
 *             <td>2500</td>
 *             <td>2</td>
 *         </tr>
 *         <tr>
 *             <td>c4_large</td>
 *             <td>2</td>
 *             <td>2500</td>
 *             <td>3.75</td>
 *         </tr>
 *         <tr>
 *             <td>c4_xlarge</td>
 *             <td>4</td>
 *             <td>2500</td>
 *             <td>7.5</td>
 *         </tr>
 *         <tr>
 *             <td>c4_2xlarge</td>
 *             <td>8</td>
 *             <td>2700</td>
 *             <td>15</td>
 *         </tr>
 *         <tr>
 *             <td>c4_4xlarge</td>
 *             <td>16</td>
 *             <td>2700</td>
 *             <td>30</td>
 *         </tr>
 *     </tbody>
 * </table>
 *
 * @see org.myPaper.vm.instances.CustomVMInstance
 */
public abstract class VmInstanceAbstract {

    protected static final int MIPS_1 = 2500;
    protected static final int MIPS_2 = 2700;

    private static int vmId = 0;

    /**
     * Gets a new ID for assigning to a new VM.
     *
     * @return new VM ID
     */
    public int getNewId() {
        return ++vmId;
    }

    /**
     * Converts GB or Gb to MB or Mb
     *
     * @param G GB or Gb
     * @return MB or Mb
     */
    protected double convertGigaToMega(double G) {
        return G * 1024;
    }

    /**
     * Creates a new VM base on the given configuration.
     *
     * @return a new VM
     */
    protected Vm newVm() {
        Vm vm = new VmSimple(getMIPS(), getCpu(), new CloudletSchedulerSpaceShared())
            .setRam((long) getMemory());

        vm.setId(getNewId());
//        vm.getUtilizationHistory().enable();

        return vm;
    }

    /**
     * Gets the required number of Pes.
     *
     * @return number of required Pes
     */
    protected abstract int getCpu();

    /**
     * Gets the required MIPS capacity.
     *
     * @return MIPS capacity
     */
    protected abstract int getMIPS();

    /**
     * Gets the required amount of memory/RAM (GB).
     *
     * @return memory capacity
     */
    protected abstract double getMemory();

    /**
     * Gets a new VM instance base on the given configuration.
     *
     * @return a new VM instance
     */
    public abstract Vm createVm();
}
