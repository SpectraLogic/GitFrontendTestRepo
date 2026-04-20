# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "bento/ubuntu-18.04"
  config.ssh.forward_agent = true
  config.ssh.insert_key = false
  config.vm.synced_folder ".", "/vagrant", type: "rsync"

  # hostmanager sets up the /etc/hosts file on each machine defined in the vagrantfile
  config.hostmanager.enabled = true
  config.hostmanager.manage_guest = true
  config.hostmanager.include_offline = true

  config.vm.provision :shell, :privileged => true, :path => "vagrantfile.sh"

  # We need one Ceph admin machine to manage the cluster
  config.vm.define "frontend-dev" do |admin|
    admin.vm.hostname = "frontend-dev"
  end

  config.vm.provider "vmware_fusion" do |v|
    # Default is true, present for clarity
    v.linked_clone = true
    v.vmx["memsize"] = "4196"
    v.vmx["numvcpus"] = "2"
  end

  config.vm.provider "virtualbox" do |v|
    v.linked_clone = true
    v.memory = 4196
    v.cpus = 2
  end

end
