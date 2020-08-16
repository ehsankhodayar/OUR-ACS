package org.myPaper.datacenter.vmAllocationPolicies;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicyAbstract;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.Optional;

public class VmAllocationPolicyFirstFitCustomized extends VmAllocationPolicyAbstract {

    @Override
    protected Optional<Host> defaultFindHostForVm(Vm vm) {
        if (vm.getHost() != Host.NULL) {

            vm.getHost().destroyTemporaryVm(vm);

            if (vm.getHost().isSuitableForVm(vm)) {
                return Optional.of(vm.getHost());
            }else {
                vm.setHost(Host.NULL);
                return Optional.of(Host.NULL);
            }
        }

        for (Host host : getHostList()){
            if (host.isSuitableForVm(vm)) {
                return Optional.of(host);
            }
        }

        return Optional.empty();
    }
}
