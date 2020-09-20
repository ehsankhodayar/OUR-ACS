package org.myPaper.datacenter;

import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterPowerSupply;
import org.cloudbus.cloudsim.hosts.Host;

/**
 * Computes current amount of power being consumed by the {@link Host}s of a {@link Datacenter}. It also considers both
 * data center solar farm and off-side energy.
 */
public class DatacenterPowerSupplyOverheadPowerAware extends DatacenterPowerSupply {
    public static final DatacenterPowerSupplyOverheadPowerAware NULL = new DatacenterPowerSupplyOverheadPowerAware(Datacenter.NULL) {
        @Override
        public double computePowerUtilizationForTimeSpan(double lastDatacenterProcessTime) {
            return -1;
        }

        @Override
        public double getPower() {
            return -1;
        }
    };

    private final Datacenter datacenter;

    /** @see #getPower() */
    private double power;

    private double energyCost;

    private double sumPUE;

    private int numberOfPueSamples;

    private final double SLEEP_MODE_POWER_CONSUMPTION;

    private final double MINIMUM_IT_POWER_CONSUMPTION;

    private final double MAXIMUM_IT_POWER_CONSUMPTION;

    private int lastPowerComputationTime;

    public DatacenterPowerSupplyOverheadPowerAware(final Datacenter datacenter) {
        this.datacenter = datacenter;

        energyCost = 0;
        SLEEP_MODE_POWER_CONSUMPTION = 10;
        MINIMUM_IT_POWER_CONSUMPTION = datacenter.getHostList().size() * SLEEP_MODE_POWER_CONSUMPTION;
        MAXIMUM_IT_POWER_CONSUMPTION = datacenter.getHostList().stream().mapToDouble(host -> host.getPowerModel().getMaxPower()).sum();

        lastPowerComputationTime = 0;
    }

    /**
     * Computes an <b>estimation</b> of total power consumed (in Watts-sec) by all Hosts of the Datacenter
     * since the last time the processing of Cloudlets in this Host was updated.
     * It also updates the {@link #getPower() Datacenter's total consumed power up to now}.
     *
     * @return the <b>estimated</b> total power consumed (in Watts-sec) by all Hosts in the elapsed time span
     */
    protected double computePowerUtilizationForTimeSpan(final double lastDatacenterProcessTime) {
        final double clock = datacenter.getSimulation().clock();

        if ((int) clock - lastPowerComputationTime <= 5) { //time span
            return 0;
        }

        double datacenterTimeSpanPowerUse = 0;
        double IT_PowerConsumption = getITPowerConsumption();
        double currentOverhead  = getOverheadPowerConsumption(power, 0);
        double newOverhead = getOverheadPowerConsumption(power, IT_PowerConsumption);
        double extraOverhead = newOverhead - currentOverhead;

        if (Double.isNaN(IT_PowerConsumption) || Double.isNaN(extraOverhead)) {
            throw new IllegalStateException("The IT power consumption or overhead power consumption can not be NaN!");
        }

        energyCost += getDatacenterPro().getTotalEnergyCost((IT_PowerConsumption + extraOverhead) / 3600);

        sumPUE += getDynamicPUE(power, IT_PowerConsumption);
        numberOfPueSamples++;

        datacenterTimeSpanPowerUse += IT_PowerConsumption + extraOverhead;

        power += datacenterTimeSpanPowerUse;
        lastPowerComputationTime = (int) clock;

        return datacenterTimeSpanPowerUse;
    }

    /**
     * Gets the total power consumed by the Datacenter up to now in Watt-Second (Ws).
     *
     * @return the total power consumption in Watt-Second (Ws)
     * @see #getPowerInKWatts()
     */
    @Override
    public double getPower() {
        return power;
    }

    public double getITPowerConsumption() {
        final double clock = datacenter.getSimulation().clock();
        double datacenterITPowerConsumption = 0;
        for (final Host host : datacenter.getHostList()) {
            if (host.isActive()) {
                final double prevCpuUsage = host.getPreviousUtilizationOfCpu();
                final double cpuUsage = host.getCpuPercentUtilization();
                final double timeFrameHostEnergy =
                    host.getPowerModel().getEnergyLinearInterpolation(prevCpuUsage, cpuUsage, clock - lastPowerComputationTime);
                datacenterITPowerConsumption += timeFrameHostEnergy;
            }else {
                datacenterITPowerConsumption += (SLEEP_MODE_POWER_CONSUMPTION * (clock - lastPowerComputationTime));
            }
        }

        return datacenterITPowerConsumption;
    }

    public double getOverheadPowerConsumption(double itPowerConsumption, double addedPowerConsumption) {
        if (itPowerConsumption + addedPowerConsumption <= 0) {
            return 0;
        }

        return (itPowerConsumption + addedPowerConsumption) * (getDynamicPUE(itPowerConsumption, addedPowerConsumption) - 1);
    }

    /**
     * Gets the datacenter's IT load (i.e. current data center power consumption / maximum data center power consumption) or power utilization.
     *
     * @param addedPowerConsumption the extra amount of power consumption that might be added to the datacenter in the future.
     * @return IT load or power utilization in range [0-1]
     * @see #getOverheadPowerConsumption(double, double)
     * @see #getDynamicPUE(double, double)
     */
    private double getITLoad(final double ITPowerConsumption, final double addedPowerConsumption) {
        return (ITPowerConsumption + addedPowerConsumption) / MAXIMUM_IT_POWER_CONSUMPTION;
    }

    /**
     * Gets the current datacenter's dynamic PUE,or future PUE if addedPowerConsumption parameter is set greater than 0,
     * in range >= 1 (considering both IT load and outside temperature).
     *
     * @param ITPowerConsumption the power consumption by IT resources
     * @param addedPowerConsumption the extra amount of power consumption that might be added to the datacenter in the future
     * @return the datacenter's dynamic PUE in range >= 1
     * @see #getOverheadPowerConsumption(double, double)
     * @see #getITLoad(double, double)
     */
    public double getDynamicPUE(final double ITPowerConsumption, final double addedPowerConsumption) {
        if (ITPowerConsumption + addedPowerConsumption <= 0) {
            return 0;
        }

        double ITLoad = getITLoad(ITPowerConsumption, addedPowerConsumption);
        DatacenterPro datacenterPro = (DatacenterPro) datacenter;
        double outsideTemperature = datacenterPro.getOutsideTemperature();

        return 1 + ((0.2 + 0.01 * ITLoad + 0.01 * ITLoad * outsideTemperature) / ITLoad);
    }

    public double getAveragePueDuringSimulation() {
        if (numberOfPueSamples == 0) {
            return 0;
        }

        return sumPUE / numberOfPueSamples;
    }

    private DatacenterPro getDatacenterPro() {
        return (DatacenterPro) datacenter;
    }

    /**
     * Gets the total energy cost in Cents from the beginning of the simulation.
     * @return the total energy cost in Cents
     */
    public double getEnergyCost() {
        return energyCost;
    }
}
