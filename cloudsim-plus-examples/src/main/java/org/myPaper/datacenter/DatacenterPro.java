package org.myPaper.datacenter;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicy;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.Simulation;
import org.cloudbus.cloudsim.datacenters.DatacenterPowerSupply;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudsimplus.listeners.EventInfo;
import org.cloudsimplus.listeners.HostEventInfo;
import org.myPaper.coordinator.CloudCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class DatacenterPro extends DatacenterSimple {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatacenterPro.class.getSimpleName());

    /**
     * @see #setCloudCoordinator(CloudCoordinator)
     * @see #getCloudCoordinator()
     */
    private CloudCoordinator cloudCoordinator;

    /**
     * @see #loadWeatherDataset(String)
     * @see #getOutsideTemperature()
     */
    private OutsideTemperature outsideTemperature;

    /**
     * @see #setEnergyPriceModel(double)
     * @see #getTotalEnergyCost(double)
     */
    private EnergyPrice energyPriceModel;

    /**
     * @see #setCarbonTaxAndFootprintRateModel(double, double)
     * @see #getTotalCarbonTax(double)
     */
    private CarbonRateAndTax carbonRateAndTaxModel;

    /**
     * @see #enableHostOverUtilizedHistoryRecorder(boolean)
     */
    private boolean hostOverUtilizedStateHistory = false;

    /**
     * @see #getHostTotalOverUtilizationTime(Host)
     */
    private final Map<Host, List<HostOverUtilizationHistoryEntry>> hostOverUtilizationHistoryMap;

    /**
     * @see #migrationQueueCheckUp(HostEventInfo)
     */
    private final Map<Host, List<Vm>> hostVmMigrationQueueMap;

    private final List<Host> hostEventListenerSuspensionQueue;

    /**
     * @see #getMaximumNumberOfLiveVmMigrations()
     * @see #increaseVmNumberOfMigrationsHistory(Vm)
     */
    private final Map<Vm, Integer> vmNumberOfVmMigrationsMap;

    /**
     * @see #updateHostCpuUtilizationHistory(HostEventInfo)
     */
    private final Map<Host, HostCpuUtilizationHistorySimpleEntry> hostCpuUtilizationHistorySimpleEntryMap;

    /**
     * @see #enableSaveHostAverageCpuUtilization(boolean)
     */
    private boolean saveHostAverageCpuUtilization;

    public DatacenterPro(Simulation simulation, List<? extends Host> hostList) {
        this(simulation, hostList, new VmAllocationPolicySimple());
    }

    public DatacenterPro(Simulation simulation, List<? extends Host> hostList, VmAllocationPolicy vmAllocationPolicy) {
        super(simulation, hostList, vmAllocationPolicy);

        cloudCoordinator = null;
        outsideTemperature = null;
        energyPriceModel = null;
        carbonRateAndTaxModel = null;
        hostOverUtilizationHistoryMap = new HashMap<>();
        hostVmMigrationQueueMap = new HashMap<>();
        hostEventListenerSuspensionQueue = new ArrayList<>();
        vmNumberOfVmMigrationsMap = new HashMap<>();
        hostCpuUtilizationHistorySimpleEntryMap = new HashMap<>();
        saveHostAverageCpuUtilization = false;

        getSimulation().addOnClockTickListener(this::simulationClockTickListener);
        getHostList().parallelStream().forEach(host -> host.addOnUpdateProcessingListener(this::hostOnUpdateProcessingListener));
    }

    /**
     * Sets the Cloud coordinator for sharing IaaS layer in the federated environment.
     *
     * @param coordinator the coordinator of provider for sharing IaaS layer in the federated environment
     */
    public void setCloudCoordinator(final CloudCoordinator coordinator) {
        cloudCoordinator = Objects.requireNonNull(coordinator);
    }

    /**
     * Gets the Cloud coordinator.
     *
     * @return the cloud coordinator of this cloud provider
     */
    public CloudCoordinator getCloudCoordinator() {
        return cloudCoordinator;
    }

    /**
     * Activates the host on update processing listener to save the average CPU utilization of datacenter's
     * hosts. Note that this option uses just a little amount of memory.
     *
     * @param activate set if it is needed, false otherwise
     */
    public void enableSaveHostAverageCpuUtilization(final boolean activate) {
        saveHostAverageCpuUtilization = activate;

        if (activate) {
            getHostList().forEach(host -> hostCpuUtilizationHistorySimpleEntryMap.put(host, new HostCpuUtilizationHistorySimpleEntry(host)));
        }else {
            hostCpuUtilizationHistorySimpleEntryMap.clear();
        }
    }

    /**
     * Enables the data center to record the history of hosts that have experienced 100% or higher amount of CPU utilization
     * during their life time.
     *
     * @param activate set true if it is needed, false otherwise
     */
    public void enableHostOverUtilizedHistoryRecorder(final boolean activate) {
        if (activate) {
            if (getPowerSupply() == DatacenterPowerSupply.NULL) {
                throw new IllegalStateException("The Datacenter power supply is not enabled!");
            }

            hostOverUtilizedStateHistory = true;
        } else {
            hostOverUtilizedStateHistory = false;
        }
    }

    private void hostOverUtilizationCheckUp(HostEventInfo hostEventInfo) {
        if (!hostOverUtilizedStateHistory || hostEventListenerSuspensionQueue.contains(hostEventInfo.getHost())) {
            return;
        }
        Host host = hostEventInfo.getHost();

        if (host.getCpuPercentUtilization() >= 1.0 || host.getPreviousUtilizationOfCpu() >= 1.0) {
            hostOverUtilizationHistoryMap.putIfAbsent(host, new ArrayList<>());
            hostOverUtilizationHistoryMap.get(host).add(new HostOverUtilizationHistoryEntry(host));
        }
    }

    /**
     * Gets the total time (in second) that the given host has experienced 100% or higher amount CPU utilization during its life time.
     *
     * @param host the target host
     * @return the total time that the given host has experienced 100% or higher amount of CPU utilization in second
     */
    private double getHostTotalOverUtilizationTime(Host host) {
        List<HostOverUtilizationHistoryEntry> hostOverUtilizationStateHistory = hostOverUtilizationHistoryMap.get(host);

        if (hostOverUtilizationStateHistory == null || hostOverUtilizationStateHistory.isEmpty()) {
            return 0;
        }

        double totalTime = 0;
        double previousTime = -1;
        boolean wasPreviousEntryFullyUtilized = false;

        for (HostOverUtilizationHistoryEntry hostOverUtilizationHistoryEntry : hostOverUtilizationStateHistory) {
            if (hostOverUtilizationHistoryEntry.wasFullyUtilized()) {
                if (previousTime == -1) {
                    previousTime = hostOverUtilizationHistoryEntry.getTIME();
                }

                if (wasPreviousEntryFullyUtilized) {
                    totalTime += hostOverUtilizationHistoryEntry.getTIME() - previousTime;
                }

                wasPreviousEntryFullyUtilized = true;
            } else {
                if (wasPreviousEntryFullyUtilized) {
                    totalTime += hostOverUtilizationHistoryEntry.getTIME() - previousTime;
                }

                wasPreviousEntryFullyUtilized = false;
            }

            previousTime = hostOverUtilizationHistoryEntry.getTIME();
        }

        return totalTime;
    }

    /**
     * Loads the datacenter outside temperature dataset.
     *
     * @param weatherDataset weather dataset (CSV file)
     * @see #getOutsideTemperature()
     */
    public void loadWeatherDataset(final String weatherDataset) throws IOException, ParseException {
        outsideTemperature = new OutsideTemperature(this);
        outsideTemperature.loadOutsideTemperature(weatherDataset);
    }

    /**
     * Gets the datacenter's current outside temperature in centigrade. Note that the weather dataset must be already set up
     * by {@link #loadWeatherDataset(String)} before using it.
     *
     * @return outside temperature in centigrade
     * @see #loadWeatherDataset(String)
     */
    public double getOutsideTemperature() {
        return outsideTemperature.getOutsideTemperature();
    }

    /**
     * Gets the datacenter's current outside temperature in centigrade at the given time. Note that the weather dataset must be already set up
     * by {@link #loadWeatherDataset(String)} before using it.
     *
     * @param time the target time
     * @return outside temperature in centigrade
     * @see #loadWeatherDataset(String)
     */
    public double getOutsideTemperature(double time) {
        return outsideTemperature.getOutsideTemperature(time);
    }

    /**
     * Sets the energy price at this datacenter.
     *
     * @param energyPriceModel the energy price in cents/KWh
     */
    public void setEnergyPriceModel(final double energyPriceModel) {
        if (energyPriceModel < 0) {
            throw new IllegalArgumentException("The given energy price is not allowed and must be equal or greater than zero");
        }

        this.energyPriceModel = new EnergyPrice(this);
        this.energyPriceModel.setEnergyPrice(energyPriceModel);
    }

    /**
     * Gets the total energy cost in cent base on the given amount of energy consumption in Watt-h.
     * The energy price at off-peak times (from 10:00 p.m. to 08:00 a.m.) would be
     * half of the on-peak times (from 08:00 a.m. to 10:00 p.m.).
     * Note that the energy price must be already set up by {@link #setEnergyPriceModel(double)} before using it.
     *
     * @param energyConsumption total amount of energy consumption in Watt-h
     * @return total energy cost in cent
     */
    public double getTotalEnergyCost(final double energyConsumption) {
        double energyCost = Double.NaN;
        try {
            energyCost = energyPriceModel.getEnergyPrice(energyConsumption);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (Double.isNaN(energyCost)) {
            throw new IllegalStateException("The energy cost is not calculable!");
        }

        return energyCost;
    }

    /**
     * Sets the carbon tax and footprint rate at the this datacenter.
     *
     * @param carbonTax           the carbon tax in cents/ton
     * @param carbonFootprintRate the carbon footprint rate in tons/MWh
     * @see #getTotalCarbonTax(double)
     */
    public void setCarbonTaxAndFootprintRateModel(final double carbonTax, final double carbonFootprintRate) {
        if (carbonTax < 0 || carbonFootprintRate < 0) {
            throw new IllegalArgumentException("The given carbon tax or carbon footprint rate is not allowed and must be equal or greater that zero.");
        }

        carbonRateAndTaxModel = new CarbonRateAndTax(this);
        carbonRateAndTaxModel.setCarbonTaxAndRate(carbonTax, carbonFootprintRate);
    }

    /**
     * Gets the total carbon tax in cent base on the given amount of energy consumption in Watt-h.
     * Note that the carbon tax and carbon footprint rate must be already set up by {@link #setCarbonTaxAndFootprintRateModel(double, double)}
     * before using it.
     *
     * @param energyConsumption the energy consumption in Watt-h
     * @return the total carbon tax in cent
     * @see #setCarbonTaxAndFootprintRateModel(double, double)
     */
    public double getTotalCarbonTax(double energyConsumption) {
        return carbonRateAndTaxModel.getCarbonTax(energyConsumption);
    }

    /**
     * Gets the total carbon footprint rate in ton base on the given amount of energy consumption in Watt-h.
     * Note that the carbon tax and carbon footprint rate must be already set up by {@link #setCarbonTaxAndFootprintRateModel(double, double)}
     * before using it.
     *
     * @param energyConsumption the energy consumption in Watt-h
     * @return the total carbon footprint rate in ton
     * @see #setCarbonTaxAndFootprintRateModel(double, double)
     */
    public double getTotalCarbonFootprint(double energyConsumption) {
        return carbonRateAndTaxModel.getCarbonFootprintRate(energyConsumption);
    }

    /**
     * Gets current datacenter's hosts which are in sleep mode.
     *
     * @return datacenter's sleep mode hosts list
     * @see #getActiveHostsList()
     */
    public List<Host> getSleepHostList() {
        return getHostList().stream().parallel()
            .filter(host -> !host.isActive())
            .collect(Collectors.toList());
    }

    /**
     * Gets current datacenter's hosts which are not in sleep mode.
     *
     * @return datacenter's active hosts list
     * @see #getSleepHostList()
     */
    public List<Host> getActiveHostsList() {
        return getHostList().stream().parallel()
            .filter(Host::isActive)
            .collect(Collectors.toList());
    }

    /**
     * Gets the current datacenter's overall CPU utilization in scale [0-1] (the average cpu utilization of active hosts).
     *
     * @return data center overall CPU utilization in scale [0-1]
     */
    public double getDatacenterOverallCpuUtilization() {
        return getActiveHostsList().stream().parallel()
            .mapToDouble(Host::getCpuPercentUtilization)
            .average()
            .orElseThrow(() -> new IllegalStateException("The overall Cpu utilization of " + this + " is not calculable!"));
    }

    /**
     * Gets the datacenter local time base on the simulation time. The simulation timezone is considered as GMT time (00:00 a.m.).
     *
     * @return datacenter local time
     */
    public double getLocalTime() {
        return getLocalTime(getSimulation().clock());
    }

    /**
     * Gets the datacenter local time base on the given time. The simulation timezone is considered as GMT time (00:00 a.m.).
     *
     * @return datacenter local time
     */
    public double getLocalTime(double time) {
        double timezoneInSeconds = getTimeZone() * 3600;
        return time + timezoneInSeconds;
    }

    /**
     * Gets the datacenter's daily local time (in scale 0 to 86400 or 24 hours) base on its timezone and GMT. The simulation timezone is considered as GMT time (00:00 a.m.).
     *
     * @return datecenter local time
     * @throws ParseException
     */
    public double getDailyLocalTime() {
        return getDailyLocalTime(getSimulation().clock());
    }

    /**
     * Gets the datacenter's daily local time (in scale 0 to 86400 or 24 hours) base on the given time. The time is considered as GMT time (00:00 a.m.).
     *
     * @param time the gmt time in seconds
     * @return
     */
    public double getDailyLocalTime(double time) {

        DateFormat dateFormat = new SimpleDateFormat("HH:mm");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        try {
            return dateFormat.parse(dateFormat
                .format((time + (this.getTimeZone() * 60 * 60)) * 1000L)).getTime() / 1000.0;
        } catch (ParseException e) {
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * Gets the list of Vms that have been created on this datacenter. It consists both running and finished Vms.
     *
     * @return
     */
    public List<Vm> getCreatedVmList() {
        return getCloudCoordinator().getCloudBroker().getVmCreatedList().stream()
            .filter(vm -> vm.getHost().getDatacenter() == this)
            .collect(Collectors.toList());
    }

    public List<Vm> getVmExecutionList() {
        return getCreatedVmList().stream()
            .filter(Vm::isWorking)
            .collect(Collectors.toList());
    }

    /**
     * Gets the Performance Degradation due to Migrations (PDM).
     *
     * @return the PDM
     */
    public double getPDM() {
        if (vmNumberOfVmMigrationsMap.isEmpty()) {
            return 0;
        }

        final int numberOfCreatedVms = getCreatedVmList().size();

        return vmNumberOfVmMigrationsMap.keySet().stream()
            .mapToDouble(integer -> (integer.getTotalMipsCapacity() * integer.getHost().getVmScheduler().getVmMigrationCpuOverhead() /
                integer.getTotalMipsCapacity()))
            .sum() / numberOfCreatedVms;
    }

    /**
     * Gets the SLA violation time per active hosts (SLATAH). Note that the {@link #enableHostOverUtilizedHistoryRecorder(boolean)}
     * must be enabled before using this method. The datacenter power supply needs to be enable in order to be able to keep the
     * previous utilization of each hosts.
     *
     * @return the SLATAH
     */
    public double getSLATAH() {
        int numberOfHosts = (int) getHostList().stream()
            .filter(host -> host.getTotalUpTime() > 0)
            .count();

        if (numberOfHosts == 0) {
            return 0;
        }

        return getHostList().stream()
            .filter(host -> host.getTotalUpTime() > 0)
            .mapToDouble(host -> getHostTotalOverUtilizationTime(host) / host.getTotalUpTime())
            .sum() / numberOfHosts;
    }


    /**
     * Gets the current datacenter's dynamic PUE,or future PUE if addedPowerConsumption parameter is set greater than 0,
     * in range >= 1 (considering both IT load and outside temperature).
     *
     * @param addedPowerConsumption the extra amount of power consumption that might be added to the datacenter in the future
     * @return the datacenter's dynamic PUE in range >= 1
     */
    public double getDatacenterDynamicPUE(final double addedPowerConsumption) {
        return getPowerSupplyOverheadPowerAware().
            getDynamicPUE(getPowerSupplyOverheadPowerAware().getITPowerConsumption(),
                addedPowerConsumption);
    }

    /**
     * Gets the power supply overhead power aware.
     *
     * @return the power supply
     */
    public DatacenterPowerSupplyOverheadPowerAware getPowerSupplyOverheadPowerAware() {
        return (DatacenterPowerSupplyOverheadPowerAware) getPowerSupply();
    }

    @Override
    public void requestVmMigration(final Vm sourceVm, final Host targetHost) {
        final String currentTime = getSimulation().clockStr();
        final Host sourceHost = sourceVm.getHost();

        final double delay = timeToMigrateVm(sourceVm, targetHost);
        final String msg1 =
            sourceHost == Host.NULL ?
                String.format("%s to %s", sourceVm, targetHost) :
                String.format("%s from %s to %s", sourceVm, sourceHost, targetHost);

        final String msg2 = String.format(
            "It's expected to finish in %.2f seconds, considering the %.0f%% of bandwidth allowed for migration and the VM RAM size.",
            delay, getBandwidthPercentForMigration() * 100);
        LOGGER.info("{}: {}: Migration of {} is started. {}", currentTime, getName(), msg1, msg2);

        /*
        When the the resource provisioner considers the Vm as not created, considers the capacity of each resources as
        the needed amount of resources and not its current requsted amount of resources.
         */
        sourceVm.setCreated(false);

        if (targetHost.addMigratingInVm(sourceVm)) {
            if (!sourceHost.getVmsMigratingOut().contains(sourceVm)) {
                sourceHost.addVmMigratingOut(sourceVm);
            }

            increaseVmNumberOfMigrationsHistory(sourceVm);

            send(targetHost.getDatacenter(), delay, CloudSimTags.VM_MIGRATE, new TreeMap.SimpleEntry<>(sourceVm, targetHost));
        } else {
            hostVmMigrationQueueMap.putIfAbsent(targetHost, new ArrayList<>());
            if (!hostVmMigrationQueueMap.get(targetHost).contains(sourceVm)) {
                sourceHost.addVmMigratingOut(sourceVm);

                LOGGER.warn("{}: {}: Migration of {} is not possible at the moment due to the lack of resources at {}.",
                    currentTime,
                    getName(),
                    sourceVm,
                    targetHost);

                LOGGER.info("{}: {}: The {} is added to the migration queue of {} and will be done as soon as possible.",
                    currentTime,
                    getName(),
                    sourceVm,
                    targetHost);
                hostVmMigrationQueueMap.get(targetHost).add(sourceVm);
            }
        }

        //Turns the Vm to its previous state
        sourceVm.setCreated(true);
    }

    private void migrationQueueCheckUp(final HostEventInfo hostEventInfo) {
        if (hostVmMigrationQueueMap.isEmpty() || hostEventListenerSuspensionQueue.contains(hostEventInfo.getHost())) {
            return;
        }

        Host host = hostEventInfo.getHost();

        if (!hostVmMigrationQueueMap.containsKey(host) || hostVmMigrationQueueMap.get(host).isEmpty()) {
            return;
        }

        //Suspend the listener in order to avoid repetitive calls
        hostEventListenerSuspensionQueue.add(host);

        List<Vm> migrationList = new ArrayList<>(hostVmMigrationQueueMap.get(host));
        List<Vm> removeQueueList = new ArrayList<>();

        for (Vm sourceVm : migrationList) {
            if (host.isSuitableForVm(sourceVm)) {
                LOGGER.info("{}: {}: {} has become suitable for {} and closed the VM from its migration queue.",
                    getSimulation().clockStr(),
                    getName(),
                    host,
                    sourceVm);

                requestVmMigration(sourceVm, host);
                removeQueueList.add(sourceVm);
            }
        }

        removeQueueList.forEach(vm -> hostVmMigrationQueueMap.get(host).remove(vm));

        hostEventListenerSuspensionQueue.remove(host);
    }

    /**
     * Gets the total uptime of the datacenter's hosts
     *
     * @return the total uptime of the datacenter's hosts
     */
    public double getHostsTotalUptime() {
        return getHostList().parallelStream()
            .mapToDouble(Host::getTotalUpTime)
            .sum();
    }

    /**
     * Gets the maximum number of live Vm migrations.
     *
     * @return the maximum number of live Vm migrations
     */
    public int getMaximumNumberOfLiveVmMigrations() {
        return vmNumberOfVmMigrationsMap.values().stream()
            .mapToInt(i -> i)
            .sum();
    }

    /**
     * Increases the number of migrations that has happened up to now by the given Vm.
     *
     * @param vm the Vm
     */
    public void increaseVmNumberOfMigrationsHistory(final Vm vm) {
        vmNumberOfVmMigrationsMap.putIfAbsent(vm, 0);

        int totalNumberOfMigrations = vmNumberOfVmMigrationsMap.get(vm) + 1;

        vmNumberOfVmMigrationsMap.replace(vm, totalNumberOfMigrations);
    }

    /**
     * Makes the Idle hosts shutdown when (No event happens in the datacenter for checking them).
     */
    private void resourceController() {
        if (getSimulation().clock() - getLastProcessTime() > getSchedulingInterval()) {
            if (getVmExecutionList().isEmpty() && getSleepHostList().size() != getHostList().size()) {
                getHostList().parallelStream()
                    .filter(host -> host.isIdleEnough(host.getIdleShutdownDeadline()))
                    .forEach(host -> host.setActive(false));
            }
        }
    }

    /**
     * Computes the datacenter total power consumption more accurate than the default Datacenter Simple.
     * Note that this method is only compatible with the {@link DatacenterPowerSupplyOverheadPowerAware}.
     */
    private void computePower() {
        getPowerSupplyOverheadPowerAware().computePowerUtilizationForTimeSpan(getLastProcessTime());
    }

    /**
     * Updates the host CPU utilization history. Note that for turning on this option the {@link #saveHostAverageCpuUtilization}
     * must be activated by calling {@link #enableSaveHostAverageCpuUtilization(boolean)} method.
     *
     * @param hostEventInfo the host event info
     */
    private void updateHostCpuUtilizationHistory(final HostEventInfo hostEventInfo) {
        if (!saveHostAverageCpuUtilization) {
            return;
        }

        final Host host = hostEventInfo.getHost();

        hostCpuUtilizationHistorySimpleEntryMap.get(host).addNewCpuUtilization(host.getCpuPercentUtilization());

    }

    /**
     * Gets the average CPU utilization of datacenter's hosts up to now in range 0-1. Note that to use this option
     * the {@link #saveHostAverageCpuUtilization} must be activated by calling {@link #enableSaveHostAverageCpuUtilization(boolean)}
     * method.
     *
     * @return the average CPU utilization of datacenter's hosts up to now in range 0-1
     */
    public double getHostsAverageCpuUtilization() {
        if (!saveHostAverageCpuUtilization) {
            return 0;
        }

        return hostCpuUtilizationHistorySimpleEntryMap.values().parallelStream()
            .filter(hostCpuUtilizationHistorySimpleEntry -> hostCpuUtilizationHistorySimpleEntry.getHost().getTotalUpTime() > 0)
            .mapToDouble(HostCpuUtilizationHistorySimpleEntry::getAverageCpuUtilization)
            .average()
            .orElse(0);
    }

    /**
     * Gets the average CPU utilization of the given host up to now in range 0-1. Note that to use this option
     * the {@link #saveHostAverageCpuUtilization} must be activated by calling {@link #enableSaveHostAverageCpuUtilization(boolean)}
     * method.
     *
     * @param host the host
     * @return the host's averae CPU utilization in range 0-1
     */
    public double getHostAverageCpuUtilization(final Host host) {
        if (!saveHostAverageCpuUtilization) {
            return 0;
        }

        return hostCpuUtilizationHistorySimpleEntryMap.get(host).getAverageCpuUtilization();
    }

    private void hostOnUpdateProcessingListener(final HostEventInfo hostEventInfo) {
        migrationQueueCheckUp(hostEventInfo);
        hostOverUtilizationCheckUp(hostEventInfo);
        updateHostCpuUtilizationHistory(hostEventInfo);
    }

    private void simulationClockTickListener(final EventInfo eventInfo) {
        resourceController();
        computePower();
    }
}
