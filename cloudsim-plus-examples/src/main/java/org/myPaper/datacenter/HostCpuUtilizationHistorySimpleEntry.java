package org.myPaper.datacenter;

import org.cloudbus.cloudsim.hosts.Host;

/**
 * The class is implemented to record the average CPU utilization of a host according to it CPU utilization
 * history during execution time. Note that in order to reduce the memory usage
 * in this class the sum of provided samples (CPU utilization) is only recorded and the average utilization will
 * be generated based on the dividing sum of sample to number of provided samples. So no list of CPU utilization
 * histories is considered.
 */
public class HostCpuUtilizationHistorySimpleEntry {
    /**
     * @see #getHost()
     */
    private final Host HOST;

    /**
     * @see #addNewCpuUtilization(double)
     * @see #getAverageCpuUtilization()
     */
    private double sumOfSamples;

    /**
     * @see #addNewCpuUtilization(double)
     * @see #getAverageCpuUtilization()
     */
    private int numberOfSamplings;

    public HostCpuUtilizationHistorySimpleEntry(final Host host) {
        HOST = host;
        sumOfSamples = 0;
        numberOfSamplings = 0;
    }

    /**
     * Gets the host.
     *
     * @return the host
     */
    public Host getHost() {
        return HOST;
    }

    /**
     * Adds a new cpu utilization entry  in range 0-1 to the host CPU utilization history.
     *
     * @param utilization the host CPU utilization in range 0-1
     * @see #getAverageCpuUtilization()
     */
    public void addNewCpuUtilization(final double utilization) {
        if (utilization > 1 || utilization < 0) {
            throw new IllegalStateException("The CPU utilization must be between 0 and one!");
        }

        sumOfSamples += utilization;
        numberOfSamplings++;
    }

    /**
     * Gets the host's average CPU utilization according to the added CPU utilization histories.
     *
     * @return the average CPU utilization in range 0-1
     * @see #addNewCpuUtilization(double)
     */
    public double getAverageCpuUtilization() {
        if (numberOfSamplings == 0) {
            return 0;
        }

        return sumOfSamples / numberOfSamplings;
    }
}
