package org.myPaper;

import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigration;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.myPaper.acsAlgorithms.Liu.Liu;
import org.myPaper.acsAlgorithms.Liu.Liu2017;
import org.myPaper.broker.DatacenterBrokerLiu;
import org.myPaper.datacenter.vmAllocationPolicies.VmAllocationPolicyMigrationStaticThresholdLiu;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MainClass_Liu2017 extends ParentClass {
    public static void main(String[] args) {
        new MainClass_Liu2017();
    }

    private MainClass_Liu2017() {
        simulation = new CloudSim();

        //Provider1 Dataceneters
        datacenter1 = createDatacenter("PaloAlto-California-USA", -7,
            DC1_OFF_SITE_ENERGY_PRICE, DC1_CARBON_FOOTPRINT_RATE, DC1_CARBON_TAX, DC1_WEATHER_DATASET);
        datacenter1.setVmAllocationPolicy(createNewVmAllocationPolicy());

        datacenter2 = createDatacenter("Richmond-Virginia-USA", -4,
            DC2_OFF_SITE_ENERGY_PRICE, DC2_CARBON_FOOTPRINT_RATE, DC2_CARBON_TAX, DC2_WEATHER_DATASET);
        datacenter2.setVmAllocationPolicy(createNewVmAllocationPolicy());

        datacenter3 = createDatacenter("Tokyo-Japan", +9,
            DC3_OFF_SITE_ENERGY_PRICE, DC3_CARBON_FOOTPRINT_RATE, DC3_CARBON_TAX, DC3_WEATHER_DATASET);
        datacenter3.setVmAllocationPolicy(createNewVmAllocationPolicy());

        datacenter4 = createDatacenter("Sydney-Australia", +10,
            DC4_OFF_SITE_ENERGY_PRICE, DC4_CARBON_FOOTPRINT_RATE, DC4_CARBON_TAX, DC4_WEATHER_DATASET);
        datacenter4.setVmAllocationPolicy(createNewVmAllocationPolicy());

        //Provider2 Dataceneters
        datacenter5 = createDatacenter("Vancouver-BritishColumbia-Canada", -7,
            DC5_OFF_SITE_ENERGY_PRICE, DC5_CARBON_FOOTPRINT_RATE, DC5_CARBON_TAX, DC5_WEATHER_DATASET);
        datacenter5.setVmAllocationPolicy(createNewVmAllocationPolicy());

        datacenter6 = createDatacenter("Toronto-Ontario-Canada", -4,
            DC6_OFF_SITE_ENERGY_PRICE, DC6_CARBON_FOOTPRINT_RATE, DC6_CARBON_TAX, DC6_WEATHER_DATASET);
        datacenter6.setVmAllocationPolicy(createNewVmAllocationPolicy());

        datacenter7 = createDatacenter("London-UK", +1,
            DC7_OFF_SITE_ENERGY_PRICE, DC7_CARBON_FOOTPRINT_RATE, DC7_CARBON_TAX, DC7_WEATHER_DATASET);
        datacenter7.setVmAllocationPolicy(createNewVmAllocationPolicy());

        //Provider3 Dataceneters
        datacenter8 = createDatacenter("Columbus-Ohio-USA", -4,
            DC8_OFF_SITE_ENERGY_PRICE, DC8_CARBON_FOOTPRINT_RATE, DC8_CARBON_TAX, DC8_WEATHER_DATASET);
        datacenter8.setVmAllocationPolicy(createNewVmAllocationPolicy());

        datacenter9 = createDatacenter("Portland-Oregon-USA", -7,
            DC9_OFF_SITE_ENERGY_PRICE, DC9_CARBON_FOOTPRINT_RATE, DC9_CARBON_TAX, DC9_WEATHER_DATASET);
        datacenter9.setVmAllocationPolicy(createNewVmAllocationPolicy());

        //Brokers of Providers
        broker1 = new DatacenterBrokerLiu(simulation, "Cloud-Broker-Provider1", Arrays.asList(datacenter1, datacenter2, datacenter3, datacenter4));
        broker1.setVmDestructionDelay(VM_DESTRUCTION_DELAY);
        broker1.setRetryFailedVms(false);
        broker2 = new DatacenterBrokerLiu(simulation, "Cloud-Broker-Provider2", Arrays.asList(datacenter5, datacenter6, datacenter7));
        broker2.setVmDestructionDelay(VM_DESTRUCTION_DELAY);
        broker2.setRetryFailedVms(false);
        broker3 = new DatacenterBrokerLiu(simulation, "Cloud-Broker-Provider3", Arrays.asList(datacenter8, datacenter9));
        broker3.setVmDestructionDelay(VM_DESTRUCTION_DELAY);
        broker3.setRetryFailedVms(false);

        //Createing Cloud Coordinators
        createCloudCoordinators();

        //Cloudlets and VMs
        cloudletList = createCloudlets();
        createVms(cloudletList);

        //Simulation
        simulation.addOnClockTickListener(this::simulationClocktickListener);
        simulation.terminateAt(SIMULATION_TIME);
        simulation.start();

        generateExperimentalResults();

        /*final List<Cloudlet> finishedCloudlets = new ArrayList<>();
        finishedCloudlets.addAll(broker1.getCloudletFinishedList());
        finishedCloudlets.addAll(broker2.getCloudletFinishedList());
        finishedCloudlets.addAll(broker3.getCloudletFinishedList());
        new CloudletsTableBuilder(finishedCloudlets).build();*/
    }

    /**
     * Creates a new VM allocation policy according to the OurAcs 2017 paper.
     *
     * @return a new VM allocation migration policy
     */
    private VmAllocationPolicyMigration createNewVmAllocationPolicy() {
        Liu liu2017 = new Liu2017(2, 5, 0.7, 0.1, 0.1, 2, OVERUTILIZATION_THRESHOLD, 10);;

        VmAllocationPolicyMigrationStaticThresholdLiu vmAllocationPolicyMigration =
            new VmAllocationPolicyMigrationStaticThresholdLiu(liu2017);
        vmAllocationPolicyMigration.setUnderUtilizationThreshold(UNDERUTILIZATION_THRESHOLD);

        return vmAllocationPolicyMigration;
    }
}
