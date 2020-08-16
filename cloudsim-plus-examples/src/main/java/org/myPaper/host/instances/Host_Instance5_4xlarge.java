package org.myPaper.host.instances;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.power.models.PowerModel;
import org.cloudbus.cloudsim.power.models.PowerModelSpecPowerHpProLiantMl110G3PentiumD930;
import org.cloudbus.cloudsim.resources.Pe;
import org.myPaper.host.HostTypeAbstract;

import java.util.List;

public class Host_Instance5_4xlarge extends HostTypeAbstract {

    @Override
    protected List<Pe> getPeList() {
        return createPe(16, getMIPS());
    }

    @Override
    protected int getMIPS() {
        return MIPS_2;
    }

    @Override
    protected double getMemory() {
        return convertGigaToMega(32);
    }

    @Override
    public PowerModel getPowerModel() {
        return new PowerModelSpecPowerHpProLiantMl110G3PentiumD930();
    }

    /**
     * <table>
     *     <thead>
     *         <tr>
     *             <td>Type</td>
     *             <td>PEs</td>
     *             <td>MIPS</td>
     *             <td>Memory (GB)</td>
     *             <td>Power Model</td>
     *         </tr>
     *     </thead>
     *     <tbody>
     *         <tr>
     *             <td>5</td>
     *             <td>16</td>
     *             <td>{@link HostTypeAbstract#MIPS_2}</td>
     *             <td>32</td>
     *             <td>{@link PowerModelSpecPowerHpProLiantMl110G3PentiumD930()}</td>
     *         </tr>
     *     </tbody>
     * </table>
     *
     * @return a new host (type 5)
     */
    @Override
    public Host getHost() {
        return newHost();
    }
}
