package org.myPaper.vm.instances;

import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.vm.VmInstanceAbstract;

public class VmInstance4_C4_2xLarge extends VmInstanceAbstract {

    @Override
    protected int getCpu() {
        return 8;
    }

    @Override
    protected int getMIPS() {
        return MIPS_2;
    }

    @Override
    protected double getMemory() {
        return convertGigaToMega(15);
    }

    /**
     * Instance Type: c4.2xlarge <a href="https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Instances.html">aws</a><br/>
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
     *             <td>c4.2xlarge</td>
     *             <td>8</td>
     *             <td>{@link VmInstanceAbstract#MIPS_1}</td>
     *             <td>15</td>
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
