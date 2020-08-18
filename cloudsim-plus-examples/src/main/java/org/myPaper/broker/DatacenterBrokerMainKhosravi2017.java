package org.myPaper.broker;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.acsAlgorithms.DatacenterSolutionEntry;
import org.myPaper.additionalClasses.SortMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatacenterBrokerMainKhosravi2017 extends DatacenterBrokerMain {
    private final Map<Datacenter, List<PowerModelEntry>> datacenterPowerModelListMap;

    /**
     * Creates a DatacenterBroker giving a specific name.
     * Subclasses usually should provide this constructor and
     * and overloaded version that just requires the {@link CloudSim} parameter.
     *
     * @param simulation the CloudSim instance that represents the simulation the Entity is related to
     * @param name       the DatacenterBroker name
     */
    public DatacenterBrokerMainKhosravi2017(CloudSim simulation, String name) {
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
    public DatacenterBrokerMainKhosravi2017(CloudSim simulation, String name, List<Datacenter> datacenterList) {
        super(simulation, name, datacenterList);
        datacenterPowerModelListMap = new HashMap<>();
    }

    @Override
    protected Datacenter defaultDatacenterMapper(Datacenter lastDatacenter, Vm vm) {
        if (getProviderDatacenters().isEmpty()) {
            throw new IllegalStateException("You don't have any Datacenter created.");
        }

        //Look for suitable resources inside the datacenters of current cloud provider
        LOGGER.info("{}: {} is trying to find suitable resources for allocating to the new Vm creation requests inside the datacenters of the " +
                "current cloud provider.",
            getSimulation().clockStr(),
            getName());

        Datacenter datacenter = runKhosraviAlgorithm(vm, getProviderDatacenters());

        if (datacenter != Datacenter.NULL) {
            LOGGER.info("{}: {} has found suitable resources for one of the new Vm creation requests inside the datacenters of the " +
                    "current cloud provider.",
                getSimulation().clockStr(),
                getName());

            return datacenter;
        }else {
            LOGGER.warn("{}: {} could not find suitable resources for all the new Vm creation requests inside the datacenters " +
                    "of the current cloud provider!",
                getSimulation().clockStr(),
                getName());

            //Look for suitable resources inside the federated datacenters
            List<Datacenter> federatedDcList = getFederatedDatacenters();


            if (!federatedDcList.isEmpty()) {
                LOGGER.info("{}: {} is trying to federate some of the new Vm creation requests.",
                    getSimulation().clockStr(),
                    getName());

                Datacenter federatedDc = runKhosraviAlgorithm(vm, federatedDcList);

                if ((federatedDc != Datacenter.NULL)) {
                    LOGGER.info("{}: {} has found a suitable resource for a new Vm creation request inside " +
                            "the federated environment.",
                        getSimulation().clockStr(),
                        getName());

                    return federatedDc;
                }else {
                    LOGGER.warn("{}: {} could not find any suitable resource for one of the new Vm creation request " +
                            "inside the federated environment!",
                        getSimulation().clockStr(),
                        getName());
                }
            }
        }

        return Datacenter.NULL;
    }

    @Override
    protected boolean requestDatacenterToCreateWaitingVms(final boolean isFallbackDatacenter) {
        if (getVmWaitingList().isEmpty()) {
            return true;
        }

        Map<Vm, Datacenter> solution = new HashMap<>();
        List<Vm> failedVmList = new ArrayList<>();

        for (Vm vm : getVmWaitingList()) {
            this.lastSelectedDc = defaultDatacenterMapper(lastSelectedDc, vm);

            if (lastSelectedDc != Datacenter.NULL) {
                this.vmCreationRequests += requestVmCreation(lastSelectedDc, isFallbackDatacenter, vm);
            }else {
                failedVmList.add(vm);
            }
        }

        if (!failedVmList.isEmpty()) {
            failVms(failedVmList);
        }

        return failedVmList.isEmpty();
    }

    private Datacenter runKhosraviAlgorithm(final Vm vm, List<Datacenter> allowedDatacenterList) {
        final double vmHoldingTime = 3600; //1 hour
        Map<Datacenter, Double> aggregatedDatacenterListMap = new HashMap<>();

        for (Datacenter datacenter : allowedDatacenterList) {
            final double avgVmUtil = getVmAverageCpuUtilization(vm, datacenter);
            final double vmPowerConsumption = vmHoldingTime * getAveragePowerConsumption(datacenter, avgVmUtil);
            final double vmOverheadPowerConsumption = vmPowerConsumption * (getDatacenterPro(datacenter).getDatacenterDynamicPUE(vmPowerConsumption) - 1);
            final double vmTotalPowerConsumption = vmPowerConsumption + vmOverheadPowerConsumption;

            //calculating energy and carbon cost
            final double energyCost = getDatacenterPro(datacenter).getTotalEnergyCost(vmTotalPowerConsumption / 3600);
            final double carbonTax = getDatacenterPro(datacenter).getTotalCarbonTax(vmTotalPowerConsumption / 3600);
            final double totalCost = energyCost + carbonTax;
            aggregatedDatacenterListMap.put(datacenter, totalCost);
        }

        List<Datacenter> sortedDatacenterList = new ArrayList<>(SortMap.sortByValue(aggregatedDatacenterListMap, true).keySet());

        for (Datacenter datacenter : sortedDatacenterList) {
            Map<Host, Double> aggregatedHostListMap = new HashMap<>();
            for (Host host : getAllowedHostList(datacenter)) {
                if (host.isSuitableForVm(vm)) {
                    final double currentPowerConsumption = host.getPowerModel().getPower();
                    final double futureCpuUtilization =
                        ((double) vm.getNumberOfPes() / (double) host.getNumberOfPes()) + host.getCpuPercentUtilization();
                    final double futurePowerConsumption =
                        host.getPowerModel().getPower(futureCpuUtilization <=1 ? futureCpuUtilization : 1);
                    final double addedPowerConsumption = futurePowerConsumption - currentPowerConsumption;
                    aggregatedHostListMap.put(host, addedPowerConsumption);
                }
            }

            List<Host> sortedHostList = new ArrayList<>(SortMap.sortByValue(aggregatedHostListMap, true).keySet());

            for (Host host : sortedHostList) {
                if (host.isSuitableForVm(vm)) {
                    host.createTemporaryVm(vm);
                    vm.setHost(host);
                    return datacenter;
                }
            }
        }

        return Datacenter.NULL;
    }

    private void savePowerModels() {
        getDatacenterList().forEach(datacenter -> {
            List<PowerModelEntry> powerModelEntryList = new ArrayList<>();
            List<String> powerModelNameList = new ArrayList<>();

            for (Host host : datacenter.getHostList()) {
                String powerModelName = host.getPowerModel().getClass().getSimpleName();
                if (!powerModelNameList.contains(powerModelName)) {
                    powerModelNameList.add(powerModelName);
                    final double minimumPowerConsumption = host.getPowerModel().getPower(0);
                    final double maximumPowerConsumption = host.getPowerModel().getMaxPower();
                    powerModelEntryList.add(new PowerModelEntry(minimumPowerConsumption, maximumPowerConsumption));
                }
            }

            datacenterPowerModelListMap.put(datacenter, powerModelEntryList);
        });
    }

    /**
     * Gets the average VM's CPU utilization base on its required Pes and the hosts' average Pes number at the given
     * data center.
     *
     * @param vm the target VM
     * @param datacenter the target datacenter
     * @return the average VM utilization at the given datacenter
     */
    private double getVmAverageCpuUtilization(final Vm vm, final Datacenter datacenter) {
        double avgVmUtil = vm.getNumberOfPes() / datacenter.getHostList().stream()
            .mapToDouble(Host::getNumberOfPes)
            .average()
            .orElseThrow(() -> new IllegalStateException("The calculation of average cpu utilization of the given VM is not possible!"));

        return avgVmUtil <= 1 ? avgVmUtil : 1;
    }

    /**
     * Gets the average host's power consumption base on the give CPU utilization and different types
     * of hosts' power models that are used in the given datacenter.
     *
     * @param datacenter the target datacenter
     * @param utilization the CPU utilization
     * @return average host's power consumption base on the give CPU utilization
     */
    private double getAveragePowerConsumption(final Datacenter datacenter, final double utilization) {
        if (datacenterPowerModelListMap.isEmpty()) {
            savePowerModels();
        }

        double powerConsumption = 0;

        for (PowerModelEntry powerModelEntry : datacenterPowerModelListMap.get(datacenter)) {
            powerConsumption += calculatePowerConsumption(powerModelEntry.getMINIMUM_POWER_CONSUMPTION(),
                powerModelEntry.getMAXIMUM_POWER_CONSUMPTION(), utilization);
        }
        return powerConsumption / datacenterPowerModelListMap.size();
    }

    /**
     * Calculates the power consumption according to the linear relation between the CPU utilization and server power
     * consumption.
     * @param minimum minimum power consumption
     * @param maximum maximum power consumption
     * @param utilization target utilization in range [0-1]
     * @return
     */
    private double calculatePowerConsumption(final double minimum, final double maximum, final double utilization) {
        return maximum + (maximum - minimum) * utilization;
    }

    @Override
    public List<DatacenterSolutionEntry> getMigrationSolutionMapList(Datacenter sourceDatacenter, List<Vm> vmList, boolean selfDatacenters) {
        return null;
    }
}

class PowerModelEntry {
    private final double MINIMUM_POWER_CONSUMPTION;
    private final double MAXIMUM_POWER_CONSUMPTION;

    public PowerModelEntry(final double minimum, final double maximum) {
        MINIMUM_POWER_CONSUMPTION = minimum;
        MAXIMUM_POWER_CONSUMPTION = maximum;
    }

    public double getMINIMUM_POWER_CONSUMPTION() {
        return MINIMUM_POWER_CONSUMPTION;
    }

    public double getMAXIMUM_POWER_CONSUMPTION() {
        return MAXIMUM_POWER_CONSUMPTION;
    }
}
