package org.myPaper.broker;

import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.events.CloudSimEvent;
import org.cloudbus.cloudsim.core.events.SimEvent;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.acsAlgorithms.DatacenterSolutionEntry;
import org.myPaper.coordinator.CloudCoordinator;
import org.myPaper.datacenter.DatacenterPro;

import java.util.*;
import java.util.stream.Collectors;

public abstract class DatacenterBrokerMain extends DatacenterBrokerAbstractCustomized {
    public static final DatacenterBrokerMain NULL = null;
    /**
     * @see #getProviderDatacenters()
     */
    private final List<Datacenter> providerDatacenters;

    /**
     * @see #addNewCoordinator(CloudCoordinator)
     * @see #removeCoordinator(CloudCoordinator)
     * @see #getCloudCoordinatorList()
     */
    private final List<CloudCoordinator> cloudCoordinatorList;

    /**
     * Creates a DatacenterBroker giving a specific name.
     * Subclasses usually should provide this constructor and
     * and overloaded version that just requires the {@link CloudSim} parameter.
     *
     * @param simulation the CloudSim instance that represents the simulation the Entity is related to
     * @param name       the DatacenterBroker name
     * @param datacenterList list of connected datacenters to this broker
     */
    public DatacenterBrokerMain(CloudSim simulation, String name, List<Datacenter> datacenterList) {
        super(simulation, name);

        Collections.shuffle(datacenterList);

        providerDatacenters = datacenterList;
        cloudCoordinatorList = new ArrayList<>();
    }

    @Override
    protected Datacenter defaultDatacenterMapper(Datacenter lastDatacenter, Vm vm) {
        return null;
    }

    @Override
    protected Vm defaultVmMapper(Cloudlet cloudlet) {
        if (cloudlet.isBoundToVm()) {
            return cloudlet.getVm();
        }else {
            return Vm.NULL;
        }
    }

    @Override
    public List<Datacenter> getDatacenterList() {
        if (!providerDatacenters.isEmpty()) {
            if (cloudCoordinatorList.isEmpty()) {
                return providerDatacenters;
            }else {
                List<Datacenter> providerDatacenterList = new ArrayList<>(providerDatacenters);

                final List<Datacenter> federatedDatacenterList = cloudCoordinatorList.stream()
                    .flatMap(cloudCoordinator -> cloudCoordinator.getFederatedDatacenterList().stream())
                    .collect(Collectors.toList());
                providerDatacenterList.addAll(federatedDatacenterList);

                return providerDatacenterList;
            }
        } else {
            return new ArrayList<>(getSimulation().getCloudInfoService().getDatacenterList());
        }
    }

    /**
     * Gets the list of allowed hosts for this broker at the given datacenter.
     *
     * @param datacenter the traget datacenter
     * @return the list of allowed hosts at the given datacenter
     */
    protected List<Host> getAllowedHostList(final Datacenter datacenter) {
        if (providerDatacenters.contains(datacenter)) {
            return datacenter.getHostList();
        }else {
            DatacenterPro datacenterPro = getDatacenterPro(datacenter);
            CloudCoordinator cloudCoordinator = datacenterPro.getCloudCoordinator();

            return cloudCoordinator.getListOfSharedInfrastructures(this, datacenter);
        }
    }

    /**
     * Checks if the given datacenter is from the federated environment or not.
     *
     * @param datacenter the target datacenter
     * @return true if the given datacenter is not part of this cloud provider, false otherwise.
     */
    protected boolean isExternalDatacenter(Datacenter datacenter) {
        return !providerDatacenters.contains(datacenter);
    }

    /**
     * Adds a new external cloud coordinator to the cloud broker.
     *
     * @param cloudCoordinator a new cloud coordinator in the federated environment
     * @see #removeCoordinator(CloudCoordinator)
     * @see #getCloudCoordinatorList()
     */
    public void addNewCoordinator(CloudCoordinator cloudCoordinator) {
        if (providerDatacenters.isEmpty()) {
            throw new IllegalStateException("Cloud coordinator only works in the federated environment.");
        }

        cloudCoordinatorList.add(Objects.requireNonNull(cloudCoordinator));
    }

    /**
     * Removes an external cloud coordinator from available cloud coordinator at this cloud broker.
     *
     * @param cloudCoordinator the target cloud coordinator
     * @see #addNewCoordinator(CloudCoordinator)
     * @see #getCloudCoordinatorList()
     */
    public void removeCoordinator(CloudCoordinator cloudCoordinator) {
        cloudCoordinatorList.remove(Objects.requireNonNull(cloudCoordinator));
    }

    /**
     * Gets the list of allowed cloud coordinators for this provider
     *
     * @return the lis of cloud coordinators
     * @see #addNewCoordinator(CloudCoordinator)
     */
    public List<CloudCoordinator> getCloudCoordinatorList() {
        return cloudCoordinatorList;
    }

    /**
     * Gets the datacenterPro
     * @param datacenter datacenter object
     * @return datacenterPro
     */
    protected DatacenterPro getDatacenterPro(Datacenter datacenter) {
        return (DatacenterPro) datacenter;
    }

    protected boolean resourceReservationController(Vm vm, Host host) {
        final List<Vm> neighborVmList = getVmWaitingList().stream()
            .filter(neighborVm -> neighborVm.getHost() == host).collect(Collectors.toList());

        if (neighborVmList.isEmpty()) {
            return true;
        }

        double totalReservedPes = 0;
        double totalReservedMemory = 0;
        double totalReservedStorage = 0;
        double totalReservedBw = 0;

        for (Vm neighborVm : neighborVmList) {
            totalReservedPes += neighborVm.getNumberOfPes();
            totalReservedMemory += neighborVm.getRam().getCapacity();
            totalReservedStorage += neighborVm.getStorage().getCapacity();
            totalReservedBw += neighborVm.getBw().getCapacity();
        }

        double availablePes = host.getFreePesNumber() - totalReservedPes - vm.getNumberOfPes();
        double availableMemory = host.getRam().getAvailableResource() - totalReservedMemory - vm.getRam().getCapacity();
        double availableStorage = host.getStorage().getAvailableResource() - totalReservedStorage - vm.getStorage().getCapacity();
        double availableBw = host.getBw().getAvailableResource() - totalReservedBw - vm.getBw().getCapacity();

        return availablePes >= 0 && availableMemory >= 0 && availableStorage >= 0 && availableBw >= 0;
    }

    /**
     * Gets the list of possible migration map list. Use this method when the data centers' Vm allocation policies
     * supports the Vm consolidation approach.
     *
     * @param sourceDatacenter the datacenter
     * @param vmList the requested Vm list
     * @param selfDatacenters set true if the provider's datacenters should be considered (except the given one), false otherwise
     * @return the list of possible migration map for the given Vm list in each available datacenter in the cloud federation environment
     */
    public abstract List<DatacenterSolutionEntry> getMigrationSolutionMapList(final Datacenter sourceDatacenter,
                                                                              final List<Vm> vmList,
                                                                              boolean selfDatacenters);

    /**
     * Gets the list of cloud provider's datacenters. Note that the available datacenters in the cloud federation environment
     * are not considered in this list.
     *
     * @return list of provider's datacenters
     */
    public List<Datacenter> getProviderDatacenters() {
        return providerDatacenters;
    }

    /**
     * Gets the list of federated datacenters which belong to other providers.
     *
     * @return the list of federated datacenters
     */
    protected List<Datacenter> getFederatedDatacenters() {
        return cloudCoordinatorList.stream()
            .flatMap(cloudCoordinator -> cloudCoordinator.getFederatedDatacenterList().stream())
            .collect(Collectors.toList());
    }

    /**
     * Fails the given list of Vms
     *
     * @param vmList the failed Vm list
     */
    protected void failVms(final List<Vm> vmList) {
        List<Vm> waitingVmList = new ArrayList<>(vmList);
        waitingVmList.forEach(vm -> {
            SimEvent simEvent = new CloudSimEvent(this, CloudSimTags.VM_CREATE_ACK, vm);
            processEvent(simEvent);
        });
    }

    @Override
    public <T extends Vm> List<T> getVmWaitingList() {
        return (List<T>) vmWaitingList.parallelStream()
            .filter(vm -> !getVmWaitingAckList().contains(vm))
            .collect(Collectors.toList());
    }
}
