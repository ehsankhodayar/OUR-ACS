package org.myPaper.host;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.power.models.*;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerSpaceShared;

import java.util.ArrayList;
import java.util.List;

public abstract class HostTypeAbstract {
    protected static final int MIPS_1 = 2500;
    protected static final int MIPS_2 = 2700;

    protected static long vmId;

    /**
     * Converts GB or Gb to MB or Mb
     * @param G GB or Gb
     * @return MB or Mb
     */
    protected double convertGigaToMega(double G){
        return G*1024;
    }

    /**
     * Creates a new list of Pes.
     * @param numberOfPes
     * @param MIPS
     * @return Pes list
     */
    protected List<Pe> createPe(final int numberOfPes, final int MIPS){
        List<Pe> peList = new ArrayList<>();

        for (int i=0 ; i<numberOfPes ; i++){
            peList.add(new PeSimple(MIPS));
        }

        return peList;
    }

    /**
     * Creates a new Host base on the given configuration.
     * @return a new host
     */
    protected Host newHost(){
        Host host = new HostSimple((long) getMemory(),
            (long) getBw(),
            (long) getStorage(),
            getPeList());

        host.setId(vmId++);
//        host.enableStateHistory();
        host.setVmScheduler(new VmSchedulerSpaceShared());
        host.setPowerModel(getPowerModel());

        return host;
    }

    /**
     * Gets list of Pes.
     * @return Pe list
     */
    protected abstract List<Pe> getPeList();

    /**
     * Gets MIPS capacity.
     * @return MIPS capacity
     */
    protected abstract int getMIPS();

    /**
     * Gets Memory capacity (GB).
     * @return memory capacity
     */
    protected abstract double getMemory();

    /**
     * Gets storage capacity (MB).
     * @return storage capacity
     */
    protected double getStorage(){
        return convertGigaToMega(1000);
    };

    /**
     * Gets bandwidth capacity (Mbs/sec).
     * @return
     */
    protected double getBw(){
        return convertGigaToMega(1000);
    };

    /**
     * Gets host power model.
     * @return power model
     */
    protected abstract PowerModel getPowerModel();

    /**
     * Gets a new host base on the given configuration.
     * @return a new host
     */
    public abstract Host getHost();
}
