
from aqueduct.task import Ansible, AnsibleTask


class QloginCluster:

    def __init__(self):
        Cluster.__init__(self)


    def log_in(self, hosts, user, expect_script):
        playbook             = Ansible()
        playbook.hosts       = hosts
        playbook.remote_user = user
        playbook.add(AnsibleTask("qlogin",
                                "script",
                                 expect_script))
        return playbook


    def log_out(self, playbook):
        playbook.add(AnsibleTask("logout", "shell", "exit"))
        playbook.add(AnsibleTask("logout", "shell", "exit"))
            
        return playbook
