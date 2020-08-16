package org.myPaper.broker;

import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.util.Conversion;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.acsAlgorithms.DatacenterSolutionEntry;
import org.myPaper.datacenter.DatacenterPro;
import org.myPaper.datacenter.vmAllocationPolicies.VmAllocationPolicyMigrationStaticThresholdLiu;

import java.util.*;
import java.util.stream.Collectors;

public class DatacenterBrokerLiu extends DatacenterBrokerMain {

    /**
     * Creates a DatacenterBroker giving a specific name.
     * Subclasses usually should provide this constructor and
     * and overloaded version that just requires the {@link CloudSim} parameter.
     *
     * @param simulation            the CloudSim instance that represents the simulation the Entity is related to
     * @param name                  the DatacenterBroker name
     * @param datacenterList        list of connected datacenters to this broker
     */
    public DatacenterBrokerLiu(CloudSim simulation,
                               String name,
                               List<Datacenter> datacenterList) {
        super(simulation, name, datacenterList);
    }

    @Override
    protected Vm defaultVmMapper(Cloudlet cloudlet) {
        if (cloudlet.isBoundToVm()) {
            return cloudlet.getVm();
        }

        return Vm.NULL;
    }

    @Override
    protected boolean requestDatacenterToCreateWaitingVms(final boolean isFallbackDatacenter) {
        if (getVmWaitingList().isEmpty()) {
            return true;
        }

        if (getProviderDatacenters().isEmpty()) {
            throw new IllegalStateException("You don't have any Datacenter created.");
        }

        return runVmAllocationPolicy(isFallbackDatacenter);
    }

    /**
     * Runs the Vm allocation policy to find suitable hosts for the given Vm list at their requested data centers.
     *
     * @param isFallbackDatacenter set true if the fallback data center is true, false otherwise
     * @return true if the last selected data center is not Null, false otherwise
     */
    protected boolean runVmAllocationPolicy(boolean isFallbackDatacenter) {
        if (getVmWaitingList().isEmpty()) {
            return true;
        }

        if (getProviderDatacenters().isEmpty()) {
            throw new IllegalStateException("You don't have any Datacenter created.");
        }

        Map<Vm, Host> solutionMap;

        LOGGER.info("{}: {} is trying to find suitable resources for allocating to the new Vm creation requests inside the datacenters of the " +
                "current cloud provider.",
            getSimulation().clockStr(),
            getName());

        for (Datacenter datacenter : getProviderDatacenters()) {
            List<Host> allowedHostList = getAllowedHostList(datacenter);

            if (allowedHostList.isEmpty()) {
                continue;
            }

            VmAllocationPolicyMigrationStaticThresholdLiu vmAllocationPolicy =
                (VmAllocationPolicyMigrationStaticThresholdLiu) datacenter.getVmAllocationPolicy();

            solutionMap = vmAllocationPolicy.findSolutionForVms(getVmWaitingList(), getAllowedHostList(datacenter)).orElse(Collections.emptyMap());

            if (!solutionMap.isEmpty()) {
                LOGGER.info("{}: {} has found some suitable resources for allocating to the new Vm creation requests inside the datacenters of the " +
                        "current cloud provider.",
                    getSimulation().clockStr(),
                    getName());

                sendVmCreationRequest(datacenter, solutionMap, isFallbackDatacenter);

                return true;
            }
        }

        LOGGER.warn("{}: {} could not find any suitable resources for allocating to the new Vm creation requests inside the datacenters " +
                "of the current cloud provider!",
            getSimulation().clockStr(),
            getName());

        //When the program reaches here, it means so solution was found in the provider's datacenters for the Vm waiting list
        List<Datacenter> federatedDcList = getFederatedDatacenters();

        if (!federatedDcList.isEmpty()) {

            LOGGER.info("{}: {} is trying to federate the new Vm creation requests.",
                getSimulation().clockStr(),
                getName());

            for (Datacenter federatedDc : federatedDcList) {
                VmAllocationPolicyMigrationStaticThresholdLiu vmAllocationPolicy =
                    (VmAllocationPolicyMigrationStaticThresholdLiu) federatedDc.getVmAllocationPolicy();

                solutionMap = vmAllocationPolicy.findSolutionForVms(getVmWaitingList(), getAllowedHostList(federatedDc)).orElse(Collections.emptyMap());

                if (!solutionMap.isEmpty()) {
                    LOGGER.info("{}: {} has found some suitable resources for allocating to the new Vm creation requests inside " +
                            "the federated environment.",
                        getSimulation().clockStr(),
                        getName());

                    sendVmCreationRequest(federatedDc, solutionMap, isFallbackDatacenter);

                    return true;
                }
            }

            LOGGER.warn("{}: {} could not find any suitable resources for allocating to the new Vm creation requests " +
                    "inside the federated environment!",
                getSimulation().clockStr(),
                getName());
        }

        failVms(getVmWaitingList());

        return false;
    }

    private void sendVmCreationRequest(final Datacenter datacenter, final Map<Vm, Host> solutionMap, final boolean isFallbackDatacenter) {
        VmAllocationPolicyMigrationStaticThresholdLiu vmAllocationPolicy =
            (VmAllocationPolicyMigrationStaticThresholdLiu) datacenter.getVmAllocationPolicy();

        Map<Vm, Host> migrationMap = vmAllocationPolicy.getSolutionMigrationMap(solutionMap);
        for (Map.Entry<Vm, Host> vmHostEntry : vmAllocationPolicy.getSolutionWithoutMigrationMap(solutionMap).entrySet()) {
            Vm vm = vmHostEntry.getKey();
            Host host = vmHostEntry.getValue();
            double submissionDelay = 0;

            if (!migrationMap.isEmpty()) {
                //Considering the migration delay if the a not created VM has a same host with some VMs in the migrationMap
                for (Map.Entry<Vm, Host> migrationEntry : migrationMap.entrySet()) {
                    Vm vmInMigration = migrationEntry.getKey();
                    Host destinationHost = migrationEntry.getValue();

                    if (vmInMigration.getHost() == host) {
                        double migrationTime = vmInMigration.getRam().getCapacity() /
                            Conversion.bitesToBytes(destinationHost.getBw().getCapacity() * datacenter.getBandwidthPercentForMigration());
                        submissionDelay += (migrationTime + 1);
                    }
                }
            }

            if (host.isSuitableForVm(vm)) {
                host.createTemporaryVm(vm);
            }
            vm.setHost(vmHostEntry.getValue());
            vm.setSubmissionDelay(submissionDelay);
            this.vmCreationRequests += requestVmCreation(datacenter, isFallbackDatacenter, vmHostEntry.getKey());
        }

        if (!migrationMap.isEmpty()) {
            migrationMap.forEach((sourceVm, targetHost) -> {
                DatacenterPro datacenterPro = (DatacenterPro) sourceVm.getHost().getDatacenter();
                datacenterPro.requestVmMigration(sourceVm, targetHost);
            });
        }
    }

    @Override
    public List<DatacenterSolutionEntry> getMigrationSolutionMapList(Datacenter sourceDatacenter, List<Vm> vmList, boolean selfDatacenters) {
        //The list solutions at different datacenters
        List<DatacenterSolutionEntry> solutionMapList = new ArrayList<>();

        if (getCloudCoordinatorList().isEmpty() && !selfDatacenters) {
            LOGGER.warn("{}: {}: The cloud federation is not activated at the moment!",
                getSimulation().clockStr(),
                getName());

            return solutionMapList;
        }else if (isExternalDatacenter(sourceDatacenter)){
            LOGGER.warn("{}: The source {} is not part of {} domain!",
                getSimulation().clockStr(),
                sourceDatacenter,
                getName());

            return solutionMapList;
        }else {
            List<Datacenter> availableDatacenterList = getDatacenterList().stream()
                .filter(datacenter -> datacenter != sourceDatacenter)
                .filter(datacenter -> selfDatacenters || isExternalDatacenter(datacenter))
                .collect(Collectors.toList());

            if (!availableDatacenterList.isEmpty()) {
                LOGGER.info("{}: {} is trying to produce some migrations maps from the federated environment...",
                    getSimulation().clockStr(),
                    getName());

                solutionMapList = availableDatacenterList.parallelStream()
                    .map(datacenter -> new DatacenterSolutionEntry(datacenter, getVmWaitingList(), getAllowedHostList(datacenter)))
                    .filter(datacenterSolutionEntry -> !datacenterSolutionEntry.getSolution().isEmpty())
                    .collect(Collectors.toList());

                if (!solutionMapList.isEmpty()) {

                    LOGGER.info("{}: {} produced some migration maps for the applicant {} in the federated environment successfully.",
                        getSimulation().clockStr(),
                        getName(),
                        sourceDatacenter);
                }else {
                    LOGGER.warn("{}: {} could not produce any migration map for the applicant {} in the federated environment!",
                        getSimulation().clockStr(),
                        getName(),
                        sourceDatacenter);
                }
            }else {
                LOGGER.warn("{}: {} could not find any available datacenter in the federated environment!",
                    getSimulation().clockStr(),
                    getName());
            }

            return solutionMapList;
        }
    }
}
