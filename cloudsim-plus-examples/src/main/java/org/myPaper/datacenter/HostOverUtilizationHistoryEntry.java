package org.myPaper.datacenter;

import org.cloudbus.cloudsim.hosts.Host;

import java.sql.Time;

public class HostOverUtilizationHistoryEntry {

    /**
     * @see #getTIME()
     */
    private final double TIME;

    /**
     * @see #getHOST()
     */
    private final Host HOST;

    /**
     * @see #getUTILIZATION()
     */
    private final double UTILIZATION;

    /**
     * Instantiates a host current fully-utilized or previously fully-utilized history entry.
     *
     * @param host the target host
     */
    public HostOverUtilizationHistoryEntry(final Host host) {
        TIME = host.getSimulation().clock();
        HOST = host;
        UTILIZATION = host.getCpuPercentUtilization();
    }

    /**
     * Gets the time that the host was over-utilized or closed from the over-utilized status.
     *
     * @return event time
     */
    public double getTIME() {
        return TIME;
    }

    /**
     * Gets the host that was over-utilized or closed from the over-utilized status.
     *
     * @return the target host
     */
    public Host getHOST() {
        return HOST;
    }

    /**
     * Gets the host's CPU percent utilization when this history entry was created.
     *
     * @return host's CPU percent utilization
     */
    public double getUTILIZATION() {
        return UTILIZATION;
    }

    /**
     * Checks the host was fully-utilized or not.
     *
     * @return true if the host was fully-utilized, false otherwise.
     */
    public boolean wasFullyUtilized() {
        return UTILIZATION >= 1.0;
    }
}
