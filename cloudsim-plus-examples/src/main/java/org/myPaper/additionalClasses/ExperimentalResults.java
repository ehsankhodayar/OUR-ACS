package org.myPaper.additionalClasses;

import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.broker.DatacenterBrokerMain;
import org.myPaper.datacenter.DatacenterPro;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ExperimentalResults {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExperimentalResults.class.getSimpleName());
    private final File SOURCE_DIR;
    private final List<DatacenterBroker> BROKERS;
    private final LocalTime SIMULATION_START_TIME;
    private final LocalTime SIMULATION_FINISH_TIME;

    public ExperimentalResults(final String outputDirectory,
                               final List<DatacenterBroker> brokerList,
                               final LocalTime startTime,
                               final LocalTime finishTime) {
        int numberOfSubmittedVmReqs = brokerList.stream()
            .mapToInt(datacenterBroker ->
                datacenterBroker.getVmCreatedList().size() + datacenterBroker.getVmWaitingList().size() + datacenterBroker.getVmFailedList().size())
            .sum();

        SOURCE_DIR = new File(outputDirectory + "\\" + LocalTime.now().toString().replace(":", "-") +
            "_" + numberOfSubmittedVmReqs + "_VmCreationReqs");

        if (!SOURCE_DIR.mkdir()) {
            throw new IllegalStateException("The system cannot create a new directory in the path specified");
        }

        BROKERS = brokerList;
        SIMULATION_START_TIME = startTime;
        SIMULATION_FINISH_TIME = finishTime;
    }

    public void generateResults() {
        LOGGER.info("The simulation process finished and the simulator is going to generate the experimental results");

        LOGGER.info("The experimental results will be saved at the following path: {}",
            SOURCE_DIR.getAbsolutePath());

        //Generate the Readme file
        generateReadme();

        //Generate the datacenters' experimental results and summaries
        generateDatacentersResults();

        //Generate the brokers' experimental results and summaries
        generateBrokersExperimentalResults();

        //Generate the simulation overall results
        generateSimulationOverallResults();

        //Generate the datacenters' outside temperature
        generateDatacentersOutsideTemperature();
    }

    private void generateReadme() {
        int numberOfAllDcs = BROKERS.stream()
            .mapToInt(datacenterBroker -> getProviderDatacenterList(datacenterBroker).size())
            .sum();

        int numberOfAllFederatedDcs = (int) BROKERS.stream()
            .flatMap(datacenterBroker -> getProviderFederatedAccessDatacenterList(datacenterBroker).stream())
            .distinct()
            .count();

        int numberOfSubmittedVmReqs = BROKERS.stream()
            .mapToInt(datacenterBroker ->
                datacenterBroker.getVmCreatedList().size() + datacenterBroker.getVmWaitingList().size() + datacenterBroker.getVmFailedList().size())
            .sum();

        int numberOfCreatedVms = BROKERS.stream()
            .mapToInt(datacenterBroker -> datacenterBroker.getVmCreatedList().size())
            .sum();

        int numberOfWaitingVms = BROKERS.stream()
            .mapToInt(datacenterBroker -> datacenterBroker.getVmWaitingList().size())
            .sum();

        int numberOfFailedVms = BROKERS.stream()
            .mapToInt(datacenterBroker -> datacenterBroker.getVmFailedList().size())
            .sum();

        int simulationRuntime = BROKERS.stream()
            .mapToInt(broker -> (int) broker.getSimulation().clock())
            .findFirst()
            .orElse(0);

        List<String> contentList = new ArrayList<>();

        contentList.add("Simulation Start Time: " + SIMULATION_START_TIME);
        contentList.add("Simulation Finish Time: " + SIMULATION_FINISH_TIME);
        contentList.add("Simulation Runtime (in second): " + simulationRuntime);
        contentList.add("Number of Cloud Providers: " + BROKERS.size());
        contentList.add("Number of All Datacenters: " + numberOfAllDcs);
        contentList.add("Number of All Federated Datacenters: " + numberOfAllFederatedDcs);
        contentList.add("Number of Submitted VM Creation Requests: " + numberOfSubmittedVmReqs);
        contentList.add("Number of Created Vms: " + numberOfCreatedVms);
        contentList.add("Number of Waiting Vms: " + numberOfWaitingVms);
        contentList.add("Number of Failed Vms: " + numberOfFailedVms);

        String fileName = SOURCE_DIR.getAbsolutePath() + "\\" + "readme";
        createNewFile(fileName, contentList);

        LOGGER.info("Readme file generated successfully at: {}" + fileName);
    }

    private void generateDatacentersResults() {
        final File datacenterDir = createNewDirectory("datacenters-experimental-results");
        List<String> contentList = new ArrayList<>();
        contentList.add("Provider,DC ID,DC Name,Broker ID,Broker Name,Number of Hosts,Average Hosts Uptime,Average CPU Utilization," +
            "Number of Created Vms,Number of Live Vm Migrations,PDM,SLATAH,SLAV,ESV,Average PUE,Total Energy Consumption (KWh),Total Energy Cost ($)," +
            "Total Carbon Emission (Kg),Total Carbon Tax ($),Total Cost ($)");

        int provider = 1;
        for (DatacenterBroker broker : BROKERS) {
            for (Datacenter datacenter : getProviderDatacenterList(broker)) {
                DatacenterPro datacenterPro = (DatacenterPro) datacenter;

                double averageHostsUptime = datacenterPro.getHostsTotalUptime()  /datacenter.getHostList().size();
                double totalEnergyConsumption = (datacenter.getPower() / 1000 / 3600);//IT energy consumption + Overhead energy consumption in KWh
                double totalCarbonEmission = datacenterPro.getTotalCarbonFootprint(totalEnergyConsumption * 1000) * 1000;//In Kg
                double totalEnergyCost = datacenterPro.getPowerSupplyOverheadPowerAware().getEnergyCost() / 100;//In Dollar
                double totalCarbonTax = datacenterPro.getTotalCarbonTax(totalEnergyConsumption * 1000) / 100;
                double totalCost = totalEnergyCost + totalCarbonTax;
                double pdm = datacenterPro.getPDM();
                double slatah = datacenterPro.getSLATAH();
                double slav = pdm * slatah;
                double esv = totalEnergyConsumption * slav;

                contentList.add(provider + "," + datacenter.getId() + "," + datacenter.getName() + "," + broker.getId() + "," + broker.getName() + "," +
                    datacenter.getHostList().size() + "," + averageHostsUptime + "," + datacenterPro.getHostsAverageCpuUtilization() + "," +
                    datacenterPro.getCreatedVmList().size() + "," + datacenterPro.getMaximumNumberOfLiveVmMigrations() + "," + pdm + "," +
                    slatah + "," + slav + "," + esv + "," + datacenterPro.getPowerSupplyOverheadPowerAware().getAveragePueDuringSimulation() + "," +
                    totalEnergyConsumption + "," + totalEnergyCost + "," + totalCarbonEmission + "," + totalCarbonTax + "," +
                    totalCost);
            }

            provider++;
        }

        String fileName = datacenterDir.getAbsolutePath() + "\\" + "datacenters.csv";
        createNewFile(fileName, contentList);

        LOGGER.info("Datacenters experimental results generated successfully at: {}", fileName);

        generateDatacentersSummary(datacenterDir.getAbsolutePath());
    }

    private void generateDatacentersSummary(final String datacenterDir) {
        List<String> contentList = new ArrayList<>();
        contentList.add("Number of Hosts,Average Hosts Uptime,Average CPU Utilization,Number of Created Vms," +
            "Number of Live Vm Migrations,PDM,SLATAH,SLAV,ESV,Average PUE,Total Energy Consumption (KWh),Total Energy Cost ($)," +
            "Total Carbon Emission (Kg),Total Carbon Tax ($),Total Cost ($)");

        int numberOfDcs = 0;
        int totalNumberOfHosts = 0;
        int totalAverageHostsUptime = 0;
        double totalAverageCpuUtilization = 0;
        int totalNumberOfCreatedVms = 0;
        int totalNumberOfLiveVmMigrations = 0;
        double averagePDM = 0;
        double averageSLATAH = 0;
        double averageSLAV = 0;
        double averageESV = 0;
        double averagePUE = 0;
        double totalEnergyConsumption = 0;
        double totalEnergyCost = 0;
        double totalCarbonEmission = 0;
        double totalCarbonTax = 0;
        double totalCost = 0;

        for (DatacenterBroker broker : BROKERS) {
            for (Datacenter datacenter : getProviderDatacenterList(broker)) {
                DatacenterPro datacenterPro = (DatacenterPro) datacenter;

                numberOfDcs++;
                totalNumberOfHosts += datacenter.getHostList().size();
                totalAverageHostsUptime += datacenterPro.getHostsTotalUptime();
                totalAverageCpuUtilization += datacenterPro.getHostsAverageCpuUtilization();
                totalNumberOfCreatedVms += datacenterPro.getCreatedVmList().size();
                totalNumberOfLiveVmMigrations += datacenterPro.getMaximumNumberOfLiveVmMigrations();

                double pdm = datacenterPro.getPDM();
                double slatah = datacenterPro.getSLATAH();
                double slav = pdm * slatah;
                double esv = (datacenter.getPower() / 1000 / 3600) * slav;

                averagePDM += pdm;
                averageSLATAH += slatah;
                averageSLAV += pdm * slatah;
                averageESV += esv;
                averagePUE += datacenterPro.getPowerSupplyOverheadPowerAware().getAveragePueDuringSimulation();
                totalEnergyConsumption += (datacenter.getPower() / 1000 / 3600);
                totalEnergyCost += datacenterPro.getPowerSupplyOverheadPowerAware().getEnergyCost() / 100;
                totalCarbonEmission += datacenterPro.getTotalCarbonFootprint(datacenter.getPower() / 3600) * 1000;
                totalCarbonTax += datacenterPro.getTotalCarbonTax(datacenter.getPower() / 3600) / 100;
                totalCost += datacenterPro.getPowerSupplyOverheadPowerAware().getEnergyCost() / 100 +
                    datacenterPro.getTotalCarbonTax(datacenter.getPower() / 3600) / 100;
            }
        }

        totalAverageHostsUptime = totalAverageHostsUptime / totalNumberOfHosts;
        totalAverageCpuUtilization = totalAverageCpuUtilization / (double) numberOfDcs;
        averagePDM = averagePDM / numberOfDcs;
        averageSLATAH = averageSLATAH / numberOfDcs;
        averageSLAV = averageSLAV / numberOfDcs;
        averageESV = averageESV / numberOfDcs;
        averagePUE = averagePUE / numberOfDcs;

        contentList.add(totalNumberOfHosts + "," + totalAverageHostsUptime + "," + totalAverageCpuUtilization + "," +
            totalNumberOfCreatedVms + "," + totalNumberOfLiveVmMigrations + "," + averagePDM + "," +
            averageSLATAH + "," + averageSLAV + "," + averageESV + "," + averagePUE + "," + totalEnergyConsumption + "," + totalEnergyCost + "," +
            totalCarbonEmission + "," + totalCarbonTax + "," + totalCost);

        String fileName = datacenterDir + "\\" + "summary.csv";
        createNewFile(fileName, contentList);

        LOGGER.info("Datacenters summary results generated successfully at: {}", fileName);
    }

    private void generateBrokersExperimentalResults() {
        final File brokerDir = createNewDirectory("brokers-experimental-results");

        List<String> contentList = new ArrayList<>();

        contentList.add("Provider,Broker ID,Broker Name,Number of Submitted Cloudlets, Number of Created Cloudlets," +
            "Number of Waiting Cloudlets,Number of Created VMs,Number of Waiting VMs,Number of VM Failures,Average Execution Time of VMs");

        int provider = 1;
        for (DatacenterBroker broker : BROKERS) {
            double averageVmsExecutionTime = broker.getVmCreatedList().stream()
                .mapToDouble(Vm::getTotalExecutionTime)
                .average()
                .orElse(0);

            contentList.add(provider + "," + broker.getId() + "," + broker.getName() + "," + broker.getCloudletSubmittedList().size() + "," +
                broker.getCloudletCreatedList().size() + "," + broker.getCloudletWaitingList().size() + "," + broker.getVmCreatedList().size() + "," +
                broker.getVmWaitingList().size() + "," + broker.getVmFailedList().size() + "," + averageVmsExecutionTime);

            provider++;
        }

        String fileName = brokerDir.getAbsolutePath() + "\\" + "brokers.csv";
        createNewFile(fileName, contentList);

        LOGGER.info("Brokers experimental results generated successfully at: {}", fileName);

        generateBrokersSummary(brokerDir.getAbsolutePath());
    }

    private void generateBrokersSummary(final String brokerDir) {
        List<String> contentList = new ArrayList<>();

        contentList.add("Provider,Broker ID,Broker Name,Number of Submitted Cloudlets, Number of Created Cloudlets," +
            "Number of Waiting Cloudlets,Number of Created VMs,Number of Waiting VMs,Number of VM Failures,Average Execution Time of VMs");

        int totalNumberOfSubmittedCloudlets = 0;
        int totalNumberOfCreatedCloudlets = 0;
        int totalNumberOfWaitingCloudlets = 0;
        int totalNumberOfCreatedVms = 0;
        int totalNumberOfWaitingVms = 0;
        int totalNumberOfFailedVms = 0;
        int averageVmsExecutionTime = 0;

        for (DatacenterBroker broker : BROKERS) {
            totalNumberOfSubmittedCloudlets += broker.getCloudletSubmittedList().size();
            totalNumberOfCreatedCloudlets += broker.getCloudletCreatedList().size();
            totalNumberOfWaitingCloudlets += broker.getCloudletWaitingList().size();
            totalNumberOfCreatedVms += broker.getVmCreatedList().size();
            totalNumberOfWaitingVms += broker.getVmWaitingList().size();
            totalNumberOfFailedVms += broker.getVmFailedList().size();
            averageVmsExecutionTime += broker.getVmCreatedList().stream()
                .mapToDouble(Vm::getTotalExecutionTime)
                .average()
                .orElse(0);
        }

        averageVmsExecutionTime = averageVmsExecutionTime / BROKERS.size();

        contentList.add("-" + "," + "-" + "," + "-" + "," + totalNumberOfSubmittedCloudlets + "," +
            totalNumberOfCreatedCloudlets + "," + totalNumberOfWaitingCloudlets + "," + totalNumberOfCreatedVms + "," +
            totalNumberOfWaitingVms + "," + totalNumberOfFailedVms + "," + averageVmsExecutionTime);

        String fileName = brokerDir + "\\" + "summary.csv";
        createNewFile(fileName, contentList);

        LOGGER.info("Brokers summary results generated successfully at: {}", fileName);
    }

    private void generateSimulationOverallResults() {
        final File file = createNewDirectory("simulation-overall-results");
        String fileName = file.getAbsolutePath() + "\\" + "results.csv";

        List<Cloudlet> cloudletList = BROKERS.stream()
            .map(DatacenterBroker::getCloudletCreatedList)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        List<String> contentList = new ArrayList<>();

        contentList.add("Cloudlet ID,Status,DC ID,Host ID,Host PEs CPU Cores,VM ID,VM PEs CPU Cores,Cloudlet Length (MI)," +
            "Cloudlet PEs CPU Cores,Start Time (Seconds),Finish Time (Seconds),Execution Time (Seconds)");

        for (Cloudlet cloudlet : cloudletList) {
            double executionTime;

            if (cloudlet.isFinished()) {
                executionTime = cloudlet.getFinishTime() - cloudlet.getExecStartTime();
            }else {
                executionTime = cloudlet.getSimulation().clock() - cloudlet.getExecStartTime();
            }

            contentList.add(cloudlet.getId() + "," + cloudlet.getStatus() + "," + cloudlet.getVm().getHost().getDatacenter().getId() + "," +
                cloudlet.getVm().getHost().getId() + "," + cloudlet.getVm().getHost().getNumberOfPes() + "," + cloudlet.getVm().getId() + "," +
                cloudlet.getVm().getNumberOfPes() + "," + cloudlet.getLength() + "," + cloudlet.getNumberOfPes() + "," +
                cloudlet.getExecStartTime() + "," + cloudlet.getFinishTime() + "," + executionTime);
        }

        createNewFile(fileName, contentList);

        LOGGER.info("The simulation overall results generated successfully at: {}", fileName);
    }

    private void generateDatacentersOutsideTemperature() {
        final File file = createNewDirectory("Datacenters-outside-temperature");

        for (DatacenterBroker broker : BROKERS) {
            for (Datacenter datacenter : getProviderDatacenterList(broker)) {
                DatacenterPro datacenterPro = (DatacenterPro) datacenter;

                String fileName = file.getAbsolutePath() + "\\" + datacenter.getId() + "_" + datacenter.getName() + ".csv";
                List<String> contentList = new ArrayList<>();

                contentList.add("From,To,Temperature (centigrade),Temperature (fahrenheit)");

                int simulationLength = (int) broker.getSimulation().clock();
                int from = 0;
                int to = 1;
                for (int i = 0; i < simulationLength; i++) {
                    double fahrenheit = (datacenterPro.getOutsideTemperature(i) * 180 / 100) + 32;
                    contentList.add(from + "," + to + "," + datacenterPro.getOutsideTemperature(i) + "," + fahrenheit);
                    from = to;
                    to++;
                    i = i + 3600;
                }

                createNewFile(fileName, contentList);
            }
        }

        LOGGER.info("Datacenters outside temperature generated successfully at: {}", file.getAbsolutePath());
    }

    private void createNewFile(final String filePathAndName, List<String> contentList) {
        try {
            FileWriter fw = new FileWriter(filePathAndName);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw);

            for (String content : contentList) {
                out.println(content);
            }

            out.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private File createNewDirectory(final String directoryName) {
        File file = new File(SOURCE_DIR.getAbsolutePath() + "\\" + directoryName);

        if (!file.mkdir()) {
            throw new IllegalStateException("The Simulator is not able to create the requested directory!");
        }

        return file;
    }

    private List<Datacenter> getProviderDatacenterList(final DatacenterBroker broker) {
        DatacenterBrokerMain cloudBroker = (DatacenterBrokerMain) broker;
        return cloudBroker.getProviderDatacenters();
    }

    private List<Datacenter> getProviderFederatedAccessDatacenterList(final DatacenterBroker broker) {
        DatacenterBrokerMain cloudBroker = (DatacenterBrokerMain) broker;

        return cloudBroker.getCloudCoordinatorList().stream()
            .flatMap(cloudCoordinator -> cloudCoordinator.getFederatedDatacenterList().stream())
            .collect(Collectors.toList());
    }
}
