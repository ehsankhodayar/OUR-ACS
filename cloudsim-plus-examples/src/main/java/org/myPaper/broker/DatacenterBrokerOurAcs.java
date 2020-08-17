package org.myPaper.broker;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.acsAlgorithms.DatacenterSolutionEntry;
import org.myPaper.acsAlgorithms.OurAcsAlgorithm.KneePointSelectionPolicy;
import org.myPaper.acsAlgorithms.OurAcsAlgorithm.MinimumPowerSelectionPolicy;

import java.util.*;
import java.util.stream.Collectors;

public class DatacenterBrokerOurAcs extends DatacenterBrokerMain {
    /**
     * Creates a DatacenterBroker giving a specific name.
     * Subclasses usually should provide this constructor and
     * and overloaded version that just requires the {@link CloudSim} parameter.
     *
     * @param simulation     the CloudSim instance that represents the simulation the Entity is related to
     * @param name           the DatacenterBroker name
     * @param datacenterList list of connected datacenters to this broker
     */
    public DatacenterBrokerOurAcs(CloudSim simulation, String name, List<Datacenter> datacenterList) {
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

        LOGGER.info("{}: {} is trying to find suitable resources for allocating to the new Vm creation requests inside the datacenters of the " +
                "current cloud provider.",
            getSimulation().clockStr(),
            getName());

        List<DatacenterSolutionEntry> datacenterSolutionEntryList = getProviderDatacenters().stream()
            .filter(datacenter -> !getAllowedHostList(datacenter).isEmpty())
            .map(datacenter -> new DatacenterSolutionEntry(datacenter, getVmWaitingList(), getAllowedHostList(datacenter)))
            .filter(datacenterSolutionEntry -> !datacenterSolutionEntry.getSolution().isEmpty())
            .collect(Collectors.toList());

        if (datacenterSolutionEntryList.isEmpty()) {
            LOGGER.warn("{}: {} could not find any suitable resources for allocating to the new Vm creation requests inside the datacenters " +
                "of the current cloud provider!",
                getSimulation().clockStr(),
                getName());

            List<Datacenter> federatedDcList = getFederatedDatacenters();

            if (federatedDcList.isEmpty()) {
                failVms(getVmWaitingList());
                return false;
            }else {
                LOGGER.info("{}: {} is trying to federate the new Vm creation requests.",
                    getSimulation().clockStr(),
                    getName());

                datacenterSolutionEntryList = federatedDcList.parallelStream()
                    .filter(federatedDc -> !getAllowedHostList(federatedDc).isEmpty())
                    .map(federatedDc -> new DatacenterSolutionEntry(federatedDc, getVmWaitingList(), getAllowedHostList(federatedDc)))
                    .filter(datacenterSolutionEntry -> !datacenterSolutionEntry.getSolution().isEmpty())
                    .collect(Collectors.toList());

                if (datacenterSolutionEntryList.isEmpty()) {
                    LOGGER.warn("{}: {} could not find any suitable resources for allocating to the new Vm creation requests " +
                            "inside the federated environment!",
                        getSimulation().clockStr(),
                        getName());

                    failVms(getVmWaitingList());
                    return false;
                }else {
                    LOGGER.info("{}: {} has found some suitable resources for allocating to the new Vm creation requests inside " +
                            "the federated environment.",
                        getSimulation().clockStr(),
                        getName());

//                    Map<Vm, Host> kneeSolution = selectKneePoint(datacenterSolutionEntryList);
                    Map<Vm, Host> solution = selectMinimumEnergyConsumption(datacenterSolutionEntryList);
                    performSolution(solution, isFallbackDatacenter);
                    return true;
                }
            }
        } else {
            LOGGER.info("{}: {} has found some suitable resources for allocating to the new Vm creation requests inside the datacenters of the " +
                "current cloud provider.",
                getSimulation().clockStr(),
                getName());

//            Map<Vm, Host> kneeSolution = selectKneePoint(datacenterSolutionEntryList);
            Map<Vm, Host> solution = selectMinimumEnergyConsumption(datacenterSolutionEntryList);
            performSolution(solution, isFallbackDatacenter);
            return true;
        }
    }

    @Override
    public List<DatacenterSolutionEntry> getMigrationSolutionMapList(Datacenter sourceDatacenter, List<Vm> vmList, boolean selfDatacenters) {
        //The list solutions at different datacenters
        List<DatacenterSolutionEntry> solutionEntryList = new ArrayList<>();

        if (getCloudCoordinatorList().isEmpty() && !selfDatacenters) {
            LOGGER.warn("{}: {}: The cloud federation is not activated at the moment!",
                getSimulation().clockStr(),
                getName());

            return solutionEntryList;
        }else if (isExternalDatacenter(sourceDatacenter)){
            LOGGER.warn("{}: The source {} is not part of {} domain!",
                getSimulation().clockStr(),
                sourceDatacenter,
                getName());

            return solutionEntryList;
        }else {
            List<Datacenter> availableDatacenterList = getDatacenterList().stream()
                .filter(datacenter -> datacenter != sourceDatacenter)
                .filter(datacenter -> selfDatacenters || isExternalDatacenter(datacenter))
                .collect(Collectors.toList());

            if (!availableDatacenterList.isEmpty()) {
                LOGGER.info("{}: {} is trying to produce some migrations maps from the federated environment...",
                    getSimulation().clockStr(),
                    getName());

                solutionEntryList = availableDatacenterList.parallelStream()
                    .map(datacenter -> new DatacenterSolutionEntry(datacenter, vmList, getAllowedHostList(datacenter)))
                    .filter(datacenterSolutionEntry -> !datacenterSolutionEntry.getSolution().isEmpty())
                    .collect(Collectors.toList());

                if (!solutionEntryList.isEmpty()) {

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

            return solutionEntryList;
        }
    }

    /**
     * Performs the given solution and allocate resources for the requested Vms.
     *
     * @param solution the solution
     */
    private void performSolution(final Map<Vm, Host> solution, final boolean isFallbackDatacenter) {
        for (Map.Entry<Vm, Host> vmHostEntry : solution.entrySet()) {
            Vm vm = vmHostEntry.getKey();
            Host host = vmHostEntry.getValue();

            host.createTemporaryVm(vm);
            vm.setHost(vmHostEntry.getValue());
            this.vmCreationRequests += requestVmCreation(host.getDatacenter(), isFallbackDatacenter, vmHostEntry.getKey());
        }
    }

    private Map<Vm, Host> selectKneePoint(final List<DatacenterSolutionEntry> datacenterSolutionEntryList) {
        KneePointSelectionPolicy kneePointSelectionPolicy = new KneePointSelectionPolicy(getVmWaitingList());

        return kneePointSelectionPolicy.getKneePoint(datacenterSolutionEntryList, true);
    }

    private Map<Vm, Host> selectMinimumEnergyConsumption(final List<DatacenterSolutionEntry> datacenterSolutionEntryList) {
        MinimumPowerSelectionPolicy powerSelectionPolicy = new MinimumPowerSelectionPolicy(getVmWaitingList());

        return powerSelectionPolicy.getSolutionWithMinimumPowerConsumption(datacenterSolutionEntryList);
    }
}
