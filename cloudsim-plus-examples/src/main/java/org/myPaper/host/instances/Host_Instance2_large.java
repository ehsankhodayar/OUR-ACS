package org.myPaper.host.instances;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.power.models.PowerModel;
import org.cloudbus.cloudsim.power.models.PowerModelSpecPowerHpProLiantMl110G4Xeon3040;
import org.cloudbus.cloudsim.resources.Pe;
import org.myPaper.host.HostTypeAbstract;

import java.util.List;

public class Host_Instance2_large extends HostTypeAbstract {

    @Override
    protected List<Pe> getPeList() {
        return createPe(2, getMIPS());
    }

    @Override
    protected int getMIPS() {
        return MIPS_1;
    }

    @Override
    protected double getMemory() {
        return convertGigaToMega(4);
    }

    @Override
    public PowerModel getPowerModel() {
        return new PowerModelSpecPowerHpProLiantMl110G4Xeon3040();
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
     *             <td>2</td>
     *             <td>2</td>
     *             <td>{@link HostTypeAbstract#MIPS_1}</td>
     *             <td>4</td>
     *             <td>{@link PowerModelSpecPowerHpProLiantMl110G4Xeon3040()}</td>
     *         </tr>
     *     </tbody>
     * </table>
     *
     * @return a new host (type 2)
     */
    @Override
    public Host getHost() {
        return newHost();
    }
}
