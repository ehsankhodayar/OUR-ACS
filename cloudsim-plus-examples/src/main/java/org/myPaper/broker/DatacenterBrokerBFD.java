package org.myPaper.broker;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.acsAlgorithms.DatacenterSolutionEntry;

import java.util.*;

public class DatacenterBrokerBFD extends DatacenterBrokerMain {
    /**
     * Creates a DatacenterBroker giving a specific name.
     * Subclasses usually should provide this constructor and
     * and overloaded version that just requires the {@link CloudSim} parameter.
     *
     * @param simulation     the CloudSim instance that represents the simulation the Entity is related to
     * @param name           the DatacenterBroker name
     * @param datacenterList list of connected datacenters to this broker
     */
    public DatacenterBrokerBFD(CloudSim simulation, String name, List<Datacenter> datacenterList) {
        super(simulation, name, datacenterList);
    }

    @Override
    protected boolean requestDatacenterToCreateWaitingVms(final boolean isFallbackDatacenter) {
        if (getVmWaitingList().isEmpty()) {
            return true;
        }

        if (getProviderDatacenters().isEmpty()) {
            throw new IllegalStateException("You don't have any Datacenter created.");
        }

        //Look for suitable resources inside the datacenters of current cloud provider
        LOGGER.info("{}: {} is trying to find suitable resources for allocating to the new Vm creation requests inside the available datacenters.",
            getSimulation().clockStr(),
            this);

//        final Comparator<Host> activeComparator = Comparator.comparing(Host::isActive).reversed();
        final Comparator<Host> comparator = Comparator.comparingLong(Host::getFreePesNumber);

        List<Vm> vmList = new ArrayList<>(getVmWaitingList());

        vmLoop:
        for (Vm vm : getVmWaitingList()) {
            for (Datacenter datacenter : getDatacenterList()) {
                List<Host> hostList = new ArrayList<>(getAllowedHostList(datacenter));
                Collections.shuffle(hostList);
                Optional<Host> selectedHost = hostList.parallelStream()
                    .filter(host -> host.isSuitableForVm(vm))
                    .min(comparator);

                if (selectedHost.isPresent() && selectedHost.get() != Host.NULL) {
                    vm.setHost(selectedHost.get());
                    selectedHost.get().createTemporaryVm(vm);
                    vmList.remove(vm);
                    this.vmCreationRequests += requestVmCreation(datacenter, isFallbackDatacenter, vm);

                    continue vmLoop;
                }
            }
        }

        if (vmList.isEmpty()) {
            LOGGER.info("{}: {} has found suitable resources for all the new Vm creation requests inside the available datacenters.",
                getSimulation().clockStr(),
                getName());

            return true;
        } else {
            LOGGER.warn("{}: {} could not find suitable resources for all the new Vm creation requests inside the available datacenters!",
                getSimulation().clockStr(),
                getName());

            failVms(vmList);

            return false;
        }
    }

    @Override
    public List<DatacenterSolutionEntry> getMigrationSolutionMapList(Datacenter sourceDatacenter, List<Vm> vmList, boolean selfDatacenters) {
        return null;
    }
}
