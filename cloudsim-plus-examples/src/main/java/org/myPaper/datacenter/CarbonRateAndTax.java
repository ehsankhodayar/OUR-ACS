package org.myPaper.datacenter;

import org.cloudbus.cloudsim.datacenters.Datacenter;

public class CarbonRateAndTax{
    private final Datacenter DATACENTER;

    private double carbonTax;
    private double carbonFootprintRate;

    public CarbonRateAndTax(Datacenter datacenter) {
        DATACENTER = datacenter;
    }

    /**
     * Sets data center carbon tax and footprintRate.
     *
     * @param tax carbon tax (cents/ton)
     * @param rate carbon footprint rate (tons/MWh)
     */
    public void setCarbonTaxAndRate(double tax, double rate) {
        carbonTax = tax;
        carbonFootprintRate = rate;
    }

    /**
     * Gets the carbon tax in cent for the given amount of energy consumption.
     *
     * @param energyConsumption energy consumption in Watt-h
     * @return carbon cost in cent
     */
    public double getCarbonTax(double energyConsumption) {
        double powerConsumptionInMWh = energyConsumption / 1000 / 1000;
        return powerConsumptionInMWh * carbonTax * carbonFootprintRate;
    }

    /**
     * Gets the carbon footprint rate in ton for the given amount of energy consumption.
     *
     * @param energyConsumption energy consumption in Watt-h
     * @return carbon footprint rate in ton
     */
    public double getCarbonFootprintRate(double energyConsumption) {
        double powerConsumptionInMWh = energyConsumption / 1000 / 1000;
        return powerConsumptionInMWh * carbonFootprintRate;
    }

    public Datacenter getDatacenter() {
        return DATACENTER;
    }
}
