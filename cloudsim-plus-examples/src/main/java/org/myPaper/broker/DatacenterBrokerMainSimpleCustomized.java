package org.myPaper.broker;

import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.acsAlgorithms.DatacenterSolutionEntry;

import java.util.ArrayList;
import java.util.List;

public class DatacenterBrokerMainSimpleCustomized extends DatacenterBrokerMain {

    /**
     * Index of the last VM selected from the {@link #getVmExecList()}
     * to run some Cloudlet.
     */
    private int lastSelectedVmIndex;

    /**
     * Index of the last Datacenter selected to place some VM.
     */
    private int lastSelectedDcIndex;

    /**
     * Creates a DatacenterBroker giving a specific name.
     * Subclasses usually should provide this constructor and
     * and overloaded version that just requires the {@link CloudSim} parameter.
     *
     * @param simulation the CloudSim instance that represents the simulation the Entity is related to
     * @param name       the DatacenterBroker name
     */
    public DatacenterBrokerMainSimpleCustomized(CloudSim simulation, String name) {
        this(simulation, name, new ArrayList<>());
    }

    /**
     * Creates a DatacenterBroker giving a specific name.
     * Subclasses usually should provide this constructor and
     * and overloaded version that just requires the {@link CloudSim} parameter.
     *
     * @param simulation the CloudSim instance that represents the simulation the Entity is related to
     * @param name       the DatacenterBroker name
     * @param datacenterList list of connected datacenters to this broker
     */
    public DatacenterBrokerMainSimpleCustomized(CloudSim simulation, String name, List<Datacenter> datacenterList) {
        super(simulation, name, datacenterList);
        this.lastSelectedVmIndex = -1;
        this.lastSelectedDcIndex = -1;
    }

    @Override
    protected Datacenter defaultDatacenterMapper(Datacenter lastDatacenter, Vm vm) {
        if (getDatacenterList().isEmpty()) {
            throw new IllegalStateException("You don't have any Datacenter created.");
        }

        if (lastDatacenter != Datacenter.NULL) {
            return getDatacenterList().get(lastSelectedDcIndex);
        }

        /*If all Datacenter were tried already Datacenter.NULL to indicate there isn't a suitable Datacenter to place waiting VMs.*/
        if (getDatacenterList().indexOf(vm.getLastTriedDatacenter()) == getDatacenterList().size() - 1) {
            return Datacenter.NULL;
        }

        if (isExternalDatacenter(getDatacenterList().get(getDatacenterList().indexOf(vm.getLastTriedDatacenter()) + 1))) {
            List<Host> allowedHosts = getAllowedHostList(getDatacenterList().get(getDatacenterList().indexOf(vm.getLastTriedDatacenter()) + 1));
            for (Host host : allowedHosts) {
                if (host.isSuitableForVm(vm)) {
                    vm.setHost(host);
                    return getDatacenterList().get(getDatacenterList().indexOf(vm.getLastTriedDatacenter()) + 1);
                }
            }

            return Datacenter.NULL;
        }

        return getDatacenterList().get(++lastSelectedDcIndex);
    }

    @Override
    protected Vm defaultVmMapper(Cloudlet cloudlet) {
        if (cloudlet.isBoundToVm()) {
            if (cloudlet.getStatus() == Cloudlet.Status.FAILED_RESOURCE_UNAVAILABLE ||
                cloudlet.getStatus() == Cloudlet.Status.FAILED) {
                cloudlet.setStatus(Cloudlet.Status.INSTANTIATED);
            }

            if (cloudlet.getVm().isFailed()) {
                cloudlet.getVm().setSubmissionDelay(0);
            }
            return cloudlet.getVm();
        }

        if (getVmExecList().isEmpty()) {
            return Vm.NULL;
        }

        /*If the cloudlet isn't bound to a specific VM or the bound VM was not created,
        cyclically selects the next VM on the list of created VMs.*/
        lastSelectedVmIndex = ++lastSelectedVmIndex % getVmExecList().size();
        return getVmFromCreatedList(lastSelectedVmIndex);
    }

    @Override
    public List<DatacenterSolutionEntry> getMigrationSolutionMapList(Datacenter sourceDatacenter, List<Vm> vmList, boolean selfDatacenters) {
        return null;
    }
}
