package org.myPaper.acsAlgorithms;

import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;
import org.myPaper.datacenter.vmAllocationPolicies.VmAllocationPolicyMigrationStaticThresholdAcsBased;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DatacenterSolutionEntry {

    /**
     * @see #getDatacenter()
     */
    private final Datacenter DATACENTER;

    /**
     * @see #getSolution()
     */
    private Map<Vm, Host> datacenterSolutionMap;

    public DatacenterSolutionEntry(final Datacenter datacenter,
                                   final List<Vm> vmList,
                                   final List<Host> allowedHostList) {
        this(datacenter, Collections.emptyMap());

        datacenterSolutionMap = constructSolutions(datacenter, vmList, allowedHostList);
    }

    public DatacenterSolutionEntry(final Datacenter datacenter,
                                   final Map<Vm, Host> solution) {
        if (datacenter == Datacenter.NULL) {
            throw new IllegalStateException("The datacenter of the solution list could not be empty");
        }

        DATACENTER = datacenter;
        datacenterSolutionMap = solution;
    }

    /**
     * Gets the Datacenter of the solution list
     *
     * @return the datacenter
     */
    public Datacenter getDatacenter() {
        return DATACENTER;
    }

    private Map<Vm, Host> constructSolutions(final Datacenter datacenter, final List<Vm> vmList, final List<Host> allowedHostList) {
        if (vmList.isEmpty() || allowedHostList.isEmpty()) {
            throw new IllegalStateException("The Vm list or allowed host list could not be empty");
        }

        VmAllocationPolicyMigrationStaticThresholdAcsBased vmAllocationPolicy =
            (VmAllocationPolicyMigrationStaticThresholdAcsBased) datacenter.getVmAllocationPolicy();

        Optional<Map<Vm, Host>> solution = vmAllocationPolicy.findSolutionForVms(vmList, allowedHostList);

        if (solution.isPresent() && !solution.get().isEmpty()) {
            return solution.get();
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Gets the solution list of datacenter
     *
     * @return the list of solutions
     */
    public Map<Vm, Host> getSolution() {
        return datacenterSolutionMap;
    }
}
