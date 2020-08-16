package org.myPaper.vm.instances;

import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.vm.VmInstanceAbstract;

public class VmInstance3_C4_xLarge extends VmInstanceAbstract {

    @Override
    protected int getCpu() {
        return 4;
    }

    @Override
    protected int getMIPS() {
        return MIPS_1;
    }

    @Override
    protected double getMemory() {
        return convertGigaToMega(7.5);
    }

    /**
     * Instance Type: c4.xlarge <a href="https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Instances.html">aws</a><br/>
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
     *             <td>c4.xlarge</td>
     *             <td>4</td>
     *             <td>{@link VmInstanceAbstract#MIPS_1}</td>
     *             <td>7.5</td>
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
