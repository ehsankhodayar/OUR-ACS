package org.myPaper.datacenter.vmAllocationPolicies;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface VmAllocationPolicyMigrationStaticThresholdAcsBased {

    /**
     * Finds suitable hosts for the given VM list according to the OurAcs ACS Algorithms.
     *
     * @param vmList          list of VMs that should be migrated out
     * @param allowedHostList list of allowed hosts for selection
     * @return a new migration map
     */
    Optional<Map<Vm, Host>> findSolutionForVms(final List<Vm> vmList, List<Host> allowedHostList);
}
