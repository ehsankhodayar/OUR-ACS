package org.myPaper.acsAlgorithms.OurAcsAlgorithm;

import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.acsAlgorithms.DatacenterSolutionEntry;
import org.myPaper.datacenter.DatacenterPowerSupplyOverheadPowerAware;

import java.util.*;

public class MinimumPowerSelectionPolicy extends KneePointSelectionPolicy {
    public MinimumPowerSelectionPolicy(List<Vm> vmList) {
        super(vmList);
    }

    public Map<Vm, Host> getSolutionWithMinimumPowerConsumption(final List<DatacenterSolutionEntry> datacenterSolutionListMap) {
        //Record the energy consumption of each solution in the following map
        Map<Map<Vm, Host>, Double> solutionPowerConsumptionMap = new HashMap<>();

        for (DatacenterSolutionEntry datacenterSolutionEntry : datacenterSolutionListMap) {
            double solutionPowerConsumption =
                getSolutionTotalPowerConsumption(datacenterSolutionEntry.getSolution(), datacenterSolutionEntry.getDatacenter());

            solutionPowerConsumptionMap.put(datacenterSolutionEntry.getSolution(), solutionPowerConsumption);
        }

        return Collections.min(solutionPowerConsumptionMap.entrySet(), Map.Entry.comparingByValue()).getKey();
    }
}
