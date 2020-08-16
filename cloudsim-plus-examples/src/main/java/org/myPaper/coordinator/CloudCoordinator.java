package org.myPaper.coordinator;

import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.broker.DatacenterBrokerMain;
import org.myPaper.datacenter.DatacenterPro;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CloudCoordinator {
    Logger LOGGER = LoggerFactory.getLogger(CloudCoordinator.class.getSimpleName());

    private final String PROVIDER_NAME;

    /**
     * @see #getCloudBroker()
     */
    private final DatacenterBroker CLOUD_BROKER;

    private final double UNDERUTILIZATION_THRESHOLD;

    private final boolean SHARE_SLEEP_HOSTS;

    /**
     * List of Datacenters at this cloud provider that are allowed to join to the cloud federation.
     */
    private final List<Datacenter> federatedDatacenterList;

    /**
     * @see #getExternalBrokerList()
     */
    private final List<DatacenterBroker> allowedBrokerList;

    public CloudCoordinator(final String providerName,
                            final DatacenterBroker cloudBroker,
                            final List<Datacenter> datacenterList,
                            final List<DatacenterBroker> allowedBrokerList,
                            final double underutilizationThreshold,
                            final boolean shareSleepHosts) {
        PROVIDER_NAME = providerName;
        CLOUD_BROKER = cloudBroker;
        federatedDatacenterList = connectDatacentersToCloudCoordinator(datacenterList);
        this.allowedBrokerList = connectAllowedBrokersToCloudCoordinator(verifyAllowedBrokers(cloudBroker, allowedBrokerList));
        UNDERUTILIZATION_THRESHOLD = verifyUnderutilizationThreshold(underutilizationThreshold);
        SHARE_SLEEP_HOSTS = shareSleepHosts;
    }

    /**
     * Gets the list of federated datacenter.
     *
     * @return the list of federated datacenters
     */
    public List<Datacenter> getFederatedDatacenterList() {
        return federatedDatacenterList;
    }

    public List<Host> getListOfSharedInfrastructures(final DatacenterBroker sourceProviderBroker, Datacenter datacenter) {
        if (!brokerAccessVerification(sourceProviderBroker)) {
            throw new IllegalStateException("The Broker " + sourceProviderBroker.getName() + " is not allowed to access the " +
                "coordinator of" + CLOUD_BROKER.getName());
        }

        List<Host> sharedInfrastructures = new ArrayList<>();

        datacenter.getHostList().forEach(host -> {
            if ((!host.isActive() && SHARE_SLEEP_HOSTS) || host.getCpuPercentUtilization() <= UNDERUTILIZATION_THRESHOLD) {
                if (host.getFreePesNumber() != 0 && host.getRam().getAvailableResource() != 0) {
                    sharedInfrastructures.add(host);
                }
            }
        });

        if (sharedInfrastructures.isEmpty()) {
            LOGGER.warn("{}: {} could not share any resource at {} in the federated environment!",
                CLOUD_BROKER.getSimulation().clockStr(), PROVIDER_NAME, datacenter);
        }

        return sharedInfrastructures;
    }

    /**
     * Gets the list of connected external brokers which belongs to other cloud providers  in the federated environment.
     *
     * @return list of external brokers
     */
    public List<DatacenterBroker> getExternalBrokerList() {
        return allowedBrokerList;
    }

    private boolean brokerAccessVerification(final DatacenterBroker datacenterBroker) {
        if (allowedBrokerList.contains(datacenterBroker)) {
            return true;
        }
        {
            return false;
        }
    }

    private List<DatacenterBroker> verifyAllowedBrokers(final DatacenterBroker cloudBroker,
                                                        final List<DatacenterBroker> allowedBrokerList) {
        if (allowedBrokerList.contains(cloudBroker)) {
            throw new IllegalStateException("Self cloud broker could not be part of allowed brokers!");
        }

        return allowedBrokerList;
    }

    private double verifyUnderutilizationThreshold(double threshold) {
        if (threshold > 1 || threshold < 0) {
            throw new IllegalStateException("Underutilization threshold could not be less than zero and greater than one!");
        }

        return threshold;
    }

    private List<Datacenter> connectDatacentersToCloudCoordinator(final List<Datacenter> datacenterList) {
        datacenterList.forEach(datacenter -> {
            DatacenterPro datacenterPro = (DatacenterPro) datacenter;
            datacenterPro.setCloudCoordinator(this);
        });

        return datacenterList;
    }

    /**
     * Connects the given list of datacenter brokers to this cloud coordinator.
     *
     * @param datacenterBrokerList the list of cloud brokers
     * @return the list of connected cloud brokers
     */
    private List<DatacenterBroker> connectAllowedBrokersToCloudCoordinator(final List<DatacenterBroker> datacenterBrokerList) {
        datacenterBrokerList.forEach(datacenterBroker -> {
            DatacenterBrokerMain datacenterBrokerMain = (DatacenterBrokerMain) datacenterBroker;

            if (!((DatacenterBrokerMain) datacenterBroker).getCloudCoordinatorList().contains(this)) {
                datacenterBrokerMain.addNewCoordinator(this);

                LOGGER.info("{}: The cloud coordinator of {} connected to {}.",
                    CLOUD_BROKER.getSimulation().clockStr(),
                    PROVIDER_NAME,
                    CLOUD_BROKER.getName());
            } else {
                LOGGER.warn("{}: The cloud coordinator of {} is already connected to {}.",
                    CLOUD_BROKER.getSimulation().clockStr(),
                    PROVIDER_NAME,
                    CLOUD_BROKER.getName());
            }
        });

        return datacenterBrokerList;
    }

    /**
     * Gets the list of Vms that are created in the given datacenter from another source broker.
     *
     * @param datacenter the datacenter
     * @return the list of federated Vms
     */
    private List<Vm> getDatacenterFederatedVmList(final Datacenter datacenter) {
        return datacenter.getHostList().parallelStream()
            .flatMap(host -> host.getVmList().stream())
            .filter(this::isVmFederated)
            .collect(Collectors.toList());
    }

    /**
     * Checks if the source broker of the given Vm is for another cloud provider or not.
     *
     * @param vm the Vm
     * @return true if the given Vm was requested from the federated environment, false otherwise
     */
    public boolean isVmFederated(final Vm vm) {
        return vm.getBroker() != CLOUD_BROKER;
    }

    /**
     * Gets the cloud broker.
     *
     * @return the cloud broker
     */
    public DatacenterBroker getCloudBroker() {
        return CLOUD_BROKER;
    }
}
