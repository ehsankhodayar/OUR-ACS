package org.myPaper.host.instances;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.power.models.PowerModel;
import org.cloudbus.cloudsim.resources.Pe;
import org.myPaper.host.HostTypeAbstract;

import java.util.ArrayList;
import java.util.List;

public class CustomHostType extends HostTypeAbstract {
    private final List<Pe> PE_LIST;
    private final double MEMORY;
    private final double STORAGE;
    private final double BW;
    private final PowerModel POWER_MODEL;

    public CustomHostType(List<Pe> peList, double memory, double storage, double bandwidth,
                          PowerModel powerModel) {
        PE_LIST = new ArrayList<>(peList);
        MEMORY = memory;
        STORAGE = storage;
        BW = bandwidth;
        POWER_MODEL = powerModel;
    }

    @Override
    protected List<Pe> getPeList() {
        return PE_LIST;
    }

    @Override
    protected int getMIPS() {
        return 0;
    }

    @Override
    protected double getMemory() {
        return MEMORY;
    }

    @Override
    protected double getStorage() {
        return STORAGE;
    }

    @Override
    protected double getBw() {
        return BW;
    }

    @Override
    public PowerModel getPowerModel() {
        return POWER_MODEL;
    }

    @Override
    public Host getHost() {
        return newHost();
    }
}
