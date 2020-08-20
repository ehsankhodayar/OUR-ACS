package org.myPaper.broker;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.acsAlgorithms.DatacenterSolutionEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DatacenterBrokerMainFFD extends DatacenterBrokerMain {

    /**
     * Creates a DatacenterBroker giving a specific name.
     * Subclasses usually should provide this constructor and
     * and overloaded version that just requires the {@link CloudSim} parameter.
     *
     * @param simulation the CloudSim instance that represents the simulation the Entity is related to
     * @param name       the DatacenterBroker name
     */
    public DatacenterBrokerMainFFD(CloudSim simulation, String name) {
        this(simulation, name, new ArrayList<>());
    }

    /**
     * Creates a DatacenterBroker giving a specific name.
     * Subclasses usually should provide this constructor and
     * and overloaded version that just requires the {@link CloudSim} parameter.
     *
     * @param simulation     the CloudSim instance that represents the simulation the Entity is related to
     * @param name           the DatacenterBroker name
     * @param datacenterList list of connected datacenters to this broker
     */
    public DatacenterBrokerMainFFD(CloudSim simulation, String name, List<Datacenter> datacenterList) {
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
        LOGGER.info("{}: {} is trying to find suitable resources for allocating to the new Vm creation requests inside the datacenters of the " +
                "current cloud provider.",
            getSimulation().clockStr(),
            getName());

        List<Vm> vmList = new ArrayList<>(getVmWaitingList());

        vmLoop:
        for (Vm vm : getVmWaitingList()) {
            for (Datacenter datacenter : getProviderDatacenters()) {

                for (Host host : getAllowedHostList(datacenter)) {
                    if (host.isSuitableForVm(vm)) {
                        vm.setHost(host);
                        host.createTemporaryVm(vm);
                        vmList.remove(vm);
                        this.vmCreationRequests += requestVmCreation(datacenter, isFallbackDatacenter, vm);

                        continue vmLoop;
                    }
                }
            }
        }

        if (vmList.isEmpty()) {
            LOGGER.info("{}: {} has found suitable resources for all the new Vm creation requests inside the datacenters of the " +
                    "current cloud provider.",
                getSimulation().clockStr(),
                getName());

            return true;
        } else {
            LOGGER.warn("{}: {} could not find suitable resources for all the new Vm creation requests inside the datacenters " +
                    "of the current cloud provider!",
                getSimulation().clockStr(),
                getName());

            //Look for suitable resources inside the federated datacenters
            List<Datacenter> federatedDcList = getFederatedDatacenters();

            if (federatedDcList.isEmpty()) {
                failVms(vmList);

                return false;
            } else {
                LOGGER.info("{}: {} is trying to federate some of the new Vm creation requests.",
                    getSimulation().clockStr(),
                    getName());

                vmLoop:
                for (Vm vm : vmList) {
                    for (Datacenter federatedDc : federatedDcList) {

                        for (Host host : getAllowedHostList(federatedDc)) {
                            if (host.isSuitableForVm(vm)) {
                                LOGGER.info("{}: {} has found a suitable resource for a new Vm creation request inside " +
                                        "the federated environment.",
                                    getSimulation().clockStr(),
                                    getName());

                                vm.setHost(host);
                                host.createTemporaryVm(vm);
                                vmList.remove(vm);
                                this.vmCreationRequests += requestVmCreation(federatedDc, isFallbackDatacenter, vm);

                                continue vmLoop;
                            }
                        }
                    }
                }

                if (vmList.isEmpty()) {
                    return true;
                }else {
                    LOGGER.warn("{}: {} could not find any suitable resource for some of the new Vm creation requests " +
                            "inside the federated environment!",
                        getSimulation().clockStr(),
                        getName());

                    failVms(vmList);

                    return false;
                }
            }
        }
    }

    @Override
    public List<DatacenterSolutionEntry> getMigrationSolutionMapList(Datacenter sourceDatacenter, List<Vm> vmList, boolean selfDatacenters) {
        return null;
    }
}
