package org.myPaper.vm.instances;

import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.vm.VmInstanceAbstract;

public class VmInstance5_C4_4xLarge extends VmInstanceAbstract {

    @Override
    protected int getCpu() {
        return 16;
    }

    @Override
    protected int getMIPS() {
        return MIPS_2;
    }

    @Override
    protected double getMemory() {
        return convertGigaToMega(30);
    }

    /**
     * Instance Type: c4.4xlarge <a href="https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Instances.html">aws</a><br/>
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
     *             <td>c4.4xlarge</td>
     *             <td>16</td>
     *             <td>{@link VmInstanceAbstract#MIPS_1}</td>
     *             <td>30</td>
     *         </tr>
     *     </tbody>
     * </table>
     * @return new vm instance
     */
    @Override
    public Vm createVm() {
        return newVm();
    }
}
