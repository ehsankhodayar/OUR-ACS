package org.myPaper.datacenter;

import java.text.ParseException;

public class EnergyPrice {
    private final DatacenterPro DATACENTER;

    /**
     * @see #setEnergyPrice(double)
     * @see #getEnergyPrice(double)
     */
    private double energyPrice;

    private static final double OFF_PEAK_START_TIME = 22 * 60 * 60;
    private static final double OFF_PEAK_FINISH_TIME = 8 * 60 * 60;

    public EnergyPrice(DatacenterPro datacenter) {
        DATACENTER = datacenter;
    }

    /**
     * Sets the data center energy price in cents/KWh.
     *
     * @param price energy price in cents/KWh
     */
    public void setEnergyPrice(double price) {
        energyPrice = price;
    }

    /**
     * Gets the off-side grid energy price in cent at this data center base on the given energy consumption value in Watt-h.
     * The price at off-peak time (10:00 p.m. to 08:00 a.m.) will be half of the on-peak time (08:00 a.m. to 10:00 p.m.).
     * On-peak and off-peak time will be detected from data center's local timezone. Note that the simulation timezone is considered as GMT+0.
     *
     * @param energyConsumption the amount of energy consumption in Watts-h
     * @return off-side grid energy price in cent
     * @throws ParseException
     * @see #setEnergyPrice(double)
     */
    public double getEnergyPrice(final double energyConsumption) throws ParseException {
        double localTime = getDatacenter().getDailyLocalTime();
        double energyConsumptionInKWh = (energyConsumption / 1000);

        if (localTime > OFF_PEAK_START_TIME || localTime < OFF_PEAK_FINISH_TIME) {
            //On-peak times (10:00 p.m. to 08:00 a.m.).

            return energyConsumptionInKWh * energyPrice;
        }else {
            //Off-peak times (08:00 a.m. to 10:00 p.m.).
            //At off-peak times the energy price is half of the on-peak times.

            return energyConsumptionInKWh * (energyPrice / 2);
        }
    }

    public DatacenterPro getDatacenter() {
        return DATACENTER;
    }
}
