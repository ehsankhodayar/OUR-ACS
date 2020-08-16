package org.myPaper.datacenter.VmSelectionPolicy;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.Comparator;
import java.util.List;

public class VmSelectionPolicyMaximumCpuUtilization implements VmSelectionPolicy {
    @Override
    public Vm getVmToMigrate(Host host) {
        final List<Vm> migratableVms = host.getMigratableVms();

        if (migratableVms.isEmpty()) {
            return Vm.NULL;
        }

        return migratableVms.stream()
            .max(Comparator.comparing(Vm::getCpuPercentUtilization))
            .orElse(Vm.NULL);
    }

    /**
     * Gets a VM to migrate from the given Vm list.
     *
     * @param vmList the host Vm list
     * @return the vm to migrate or {@link Vm#NULL} if there is not Vm to migrate
     */
    public Vm getVmToMigrate(List<Vm> vmList) {
        return vmList.stream()
            .filter(vm -> !vm.isInMigration())
            .max(Comparator.comparing(Vm::getCpuPercentUtilization))
            .orElse(Vm.NULL);
    }
}
