
import rethinkdb as r

from io import StringIO

import json
import os
import hashlib
import re
import time

class Task:
    
    def __init__(self):
        self.subtasks = []
        self.filename = None
        self.prefix   = "task"
        self.extension = None
        self.generated_files = set()


    def add(self, obj):
        self.subtasks.append(obj)


    def to_string(self):
        pass


    def write(self):
        pass


    def make_unique_filename(self):
        com   = self.to_string().encode('utf-8')
        md5   = str(hashlib.md5(com).hexdigest())
        truncated_md5   = md5[0:16]
        self.filename = self.prefix + "_" + truncated_md5 + self.extension


    def start(self):
        pass


    def stop(self):
        pass
    

    def report(self):
        pass

    def read_sge_job_info(self, fname):
        line = open(fname + ".qsub_out").read()
        words = line.split(" ")
        return words[2]


    def store_job_info(self, info):
        conn = r.connect("localhost")
        if not self.exists_db(conn, "job_info"):
            self.create_db(conn, "job_info")
        
        if not self.exists_table(conn, "job_info", "job_info"):
            self.create_table(conn, "job_info", "job_info")

        result = r.db("job_info").table("job_info").insert(info).run(conn)
        print(result)
        conn.close()

        return result["generated_keys"][0]


    def replace_job_info(self, job_id, info):
        conn = r.connect("localhost")
        result = r.db("job_info").table("job_info").get(job_id).replace(info).run(conn)
        print(result)
        conn.close()

        #return result["generated_keys"][0]



    def exists_db(self, conn, db_name):
        db_list = r.db_list().run(conn)
        for d in db_list:
            if d == db_name:
                return True
        
        return False


    def exists_table(self, conn, db_name, table_name):
        table_list = r.db(db_name).table_list().run(conn)
        for t in table_list:
            if t == table_name:
                return True
        
        return False


    def create_db(self, conn, db_name):
        r.db_create(db_name).run(conn)


    def create_table(self, conn, db_name, table_name):
        r.db(db_name).table_create(table_name).run(conn)


    def search_job_info(self, job_id):
        conn = r.connect("localhost")
        record = r.db("job_info").table("job_info").get(job_id).run(conn)
        conn.close()

        return record



    def is_qsub_script(self, filename):
        if re.match(r"qsub_[0-9A-Za-z]+\.sh", filename):
            return True
        else:
            return False




from six import string_types
from subprocess import Popen


class Bash(Task):
    
    def __init__(self, coms=None):
        Task.__init__(self)
        self.filename = "bash_script.sh"
        self.debug = False
        self.prefix = "bash"
        self.extension = ".sh"
        if coms != None:
            self.subtasks = coms


    def to_string(self):
        return "\n".join(self.subtasks)


    def write(self):
        fout = open(self.filename, "w")
        fout.write(self.to_string() + "\n")
        fout.close()


    def start(self):
        for obj in self.subtasks:
            if isinstance(obj, string_types):
                if self.debug:
                    print(line)
                p = Popen(line, shell=True)
                p.wait()

        job_id = self.store_job_info(self.job_info())
        return job_id
        

    def job_info(self):
        info = {}
        info["job_type"]           = "Bash"
        info["submitted_tick"]     = str(time.time())
        info["submitted_asctime"]  = time.asctime( time.localtime(time.time()) )
        info["timezone"]           = time.tzname[0]
        info["filename"]           = self.filename
        info["generated_files"]    = list(self.generated_files)
        #info["server"]


        return info



class SGE(Task):
    """
    > cat qsub_script.sh
    qsub -cwd -V -S /bin/bash bash_script1.sh
    qsub -cwd -V -S /usr/bin/ansible-playbook ansible_playbook.yaml
    qsub -cwd -V -S /bin/bash bash_script3.sh
    """
    
    def __init__(self):
        Task.__init__(self)
        self.filename = "submittee.sh"
        self.prefix = "qsub"
        self.extension = ".sh"
        self.qsub_params = ""


    def to_string(self):
        lines = []
        for obj in self.subtasks:
            interpreter = "/bin/bash"
            if isinstance(obj, Bash):
                interpreter = "/bin/bash"
            elif isinstance(obj, SGE):
                interpreter = "/bin/bash"
            elif isinstance(obj, Ansible):
                interpreter = "/usr/bin/ansible-playbook"

            obj.make_unique_filename()
            obj.write()
            self.generated_files.add(obj.filename)

            lines.append(self.qsub(interpreter, obj.filename))

        return "\n".join(lines)


    def qsub(self, interpreter, fname):
        com = ["qsub", "-cwd", "-V"]
        if self.qsub_params != "":
            com.append(self.qsub_params)
        com.extend(["-S", interpreter, fname])
        
        return " ".join(com)


    def write(self):
        f = open(self.filename, "w")
        f.write(self.to_string() + "\n")
        f.close()


    def start(self):

        self.write()
        p = Popen("bash " + self.filename + " > " + self.filename + ".qsub_out", shell=True)
        p.wait()

        job_id = self.store_job_info(self.job_info())
        return job_id
        

    def job_info(self):
        info = {}
        info["job_type"]           = "SGE"
        info["qsub_params"]        = self.qsub_params
        info["submitted_tick"]     = str(time.time())
        info["submitted_asctime"]  = time.asctime( time.localtime(time.time()) )
        info["timezone"]           = time.tzname[0]
        info["filename"]           = self.filename
        info["generated_files"]    = list(self.generated_files)
        #info["server"]
        info["SGE_job_id"]         = self.read_sge_job_info(self.filename)

        return info




class AnsibleTask(Task):

    def __init__(self, name, module, *params):
        Task.__init__(self)
        self.__name   = name
        self.__module = module
        self.__params = " ".join(params)
        self.args = []


    def add(self, obj):
        pass


    def to_string(self):
        lines = []
        lines.append("    - name: " + self.__name)
        lines.append("      " + self.__module + ": " + self.__params)

        if len(self.args) > 0:
            lines.append("      args:")
            for a in self.args:
                lines.append("        " + a)
            
        return "\n".join(lines)



class Ansible(Task):
    
    def __init__(self):
        Task.__init__(self)
        self.hosts = []
        self.remote_user = None
        self.cwd = os.getcwd()
        self.filename = "ansible_playbook.yaml"
        self.prefix = "ansible"
        self.extension = ".yaml"


    def to_string(self):
        lines = []
        lines.append("---")
        lines.append("- hosts: " + ":".join(self.hosts))
        lines.append("  gather_facts: no")
        lines.append("  remote_user: " + self.remote_user)
        lines.append("  tasks:")

        for obj in self.subtasks:

            if isinstance(obj, AnsibleTask):
                lines.append(obj.to_string())

            elif isinstance(obj, Bash):
                obj.make_unique_filename()
                obj.write()
                self.generated_files.add(obj.filename)
                lines.extend(self.__bash_task(obj))

            elif isinstance(obj, SGE):
                obj.make_unique_filename()
                obj.write() 
                self.generated_files.add(obj.filename)
                lines.extend(self.__sge_task(obj))

            elif isinstance(obj, Ansible):
                obj.make_unique_filename()
                obj.write() 
                self.generated_files.add(obj.filename)
                lines.extend(self.__ansible(obj))

        
        return "\n".join(lines)


    def __bash_task(self, bash):
        lines = []
        t = AnsibleTask("make directories", "shell", 
                        "mkdir -p " + self.cwd)
        lines.append(t.to_string())
        
        t = AnsibleTask("upload " + bash.filename, "copy", 
                        "src=" + os.getcwd() + "/" + bash.filename,
                        "dest=" + self.cwd)
        lines.append(t.to_string())

        t = AnsibleTask(bash.filename,
                        "shell", 
                        "bash " + bash.filename)
        t.args = ["chdir: " + self.cwd]
        lines.append(t.to_string())

        return lines



    def __sge_task(self, sge):
        lines = []
        t = AnsibleTask("make directories", "shell", 
                        "mkdir -p " + self.cwd)
        lines.append(t.to_string())
        
        for fname in sge.generated_files:
            t = AnsibleTask("upload " + fname, "copy", 
                            "src=" + fname,
                            "dest=" + self.cwd)
            lines.append(t.to_string())

        t = AnsibleTask("upload " + sge.filename, "copy", 
                        "src=" + sge.filename,
                        "dest=" + self.cwd)
        lines.append(t.to_string())

        t = AnsibleTask(sge.filename,
                        "shell", 
                        "bash " + sge.filename + " > " + sge.filename + ".qsub_out")
        t.args = ["chdir: " + self.cwd]
        lines.append(t.to_string())

        t = AnsibleTask("fetch " + sge.filename + ".qsub_out", "fetch", 
                        "src=" + self.cwd + "/" + sge.filename + ".qsub_out",
                        "dest=" + os.getcwd(),
                        "flat=yes",
                        "validate_checksum=no")
        lines.append(t.to_string())
        
        return lines




    def __ansible(self, ansible):
        lines = []
        t = AnsibleTask("make directories", "shell", 
                        "mkdir -p " + self.cwd)
        lines.append(t.to_string())
        
        for fname in ansible.generated_files:
            t = AnsibleTask("upload " + fname, "copy", 
                            "src=" + fname,
                            "dest=" + self.cwd)
            lines.append(t.to_string())

        t = AnsibleTask("upload " + ansible.filename, "copy", 
                        "src=" + ansible.filename,
                        "dest=" + self.cwd)
        lines.append(t.to_string())

        t = AnsibleTask(ansible.filename,
                        "command", 
                        "ansible-playbook " + ansible.filename)
        t.args = ["chdir: " + self.cwd]
        lines.append(t.to_string())
        
        return lines



    def write(self):
        f = open(self.filename, "w")
        f.write(self.to_string() + "\n")
        f.close()


    def start(self):
        self.write()
        com = ["/usr/bin/python2.7",
               "/usr/bin/ansible-playbook",
               self.filename,
               "-T 120"]
        p = Popen(" ".join(com), shell=True)
        p.wait()

        job_id = self.store_job_info(self.job_info())
        return job_id
        

    def job_info(self):
        info = {}
        info["job_type"]           = "Ansible"
        info["submitted_tick"]     = str(time.time())
        info["submitted_asctime"]  = time.asctime( time.localtime(time.time()) )
        info["timezone"]           = time.tzname[0]
        info["filename"]           = self.filename
        info["generated_files"]    = list(self.generated_files)
        info["hosts"]              = self.hosts
        info["remote_user"]        = self.remote_user
        info["remote_cwd"]         = self.cwd
        info["local_cwd"]          = os.getcwd()

        return info




def start_job():
    """
    Get total size of a huge directory.
    """
    ansible             = Ansible()
    ansible.hosts       = ["gw.ddbj.nig.ac.jp"]
    ansible.remote_user = "o0gasawa"
    ansible.add(AnsibleTask("qlogin",
                            "script",
                            "qlogin.expect"))
    ansible.cwd = "/home/o0gasawa/data"

    bash = Bash(["du -h > outfile2.txt"])
    sge  = SGE()
    sge.add(bash)
    ansible.add(sge)

    ansible.add(AnsibleTask("logout", "shell", "exit"))
    ansible.add(AnsibleTask("logout", "shell", "exit"))

    #ansible.write()
    ansible.start()





def report_job(job_id):
    ansible             = Ansible()
    ansible.hosts       = ["gw.ddbj.nig.ac.jp"]
    ansible.remote_user = "o0gasawa"
    ansible.add(AnsibleTask("qlogin",
                            "script",
                            "qlogin.expect"))
    info = ansible.search_job_info(job_id)

    info["remote_cwd"] = "/home/o0gasawa/data"
    ansible.cwd = info["remote_cwd"]
    
    for f in info["generated_files"]:
        if ansible.is_qsub_script(f):
            sge_job_id = ansible.read_sge_job_info(f)
            bash = Bash([
                "qstat -j " + sge_job_id + " > " + f + "." + sge_job_id + ".qstat  2>&1",
                "qacct -j " + sge_job_id + " > " + f + "." + sge_job_id + ".qacct  2>&1",
            ])            
            ansible.add(bash)
            t = AnsibleTask("fetch " + f + ".qstat", "fetch", 
                            "src=" + info["remote_cwd"] + "/" + f + "." + sge_job_id + ".qstat",
                            "dest=" + os.getcwd(),
                            "flat=yes",
                            "validate_checksum=no")
            ansible.add(t)
            t = AnsibleTask("fetch " + f + ".qacct", "fetch", 
                            "src=" + info["remote_cwd"] + "/" + f + "." + sge_job_id + ".qacct",
                            "dest=" + os.getcwd(),
                            "flat=yes",
                            "validate_checksum=no")
            ansible.add(t)


    ansible.add(AnsibleTask("logout", "shell", "exit"))
    ansible.add(AnsibleTask("logout", "shell", "exit"))

    #ansible.write()
    ansible.start()

    info["current_state"] = {}
    info["current_state"]["tick"]     = str(time.time())
    info["current_state"]["asctime"]  = time.asctime( time.localtime(time.time()) )
    info["current_state"]["timezone"] = time.tzname[0]
    for f in info["generated_files"]:
        if ansible.is_qsub_script(f):
            sge_job_id = ansible.read_sge_job_info(f)
            qstat = open(f + "." + sge_job_id + ".qstat").read()
            qacct = open(f + "." + sge_job_id + ".qacct").read()
            info["current_state"][f] = {"sge_job_id": sge_job_id,
                                        "qstat": qstat,
                                        "qacct": qacct}

    ansible.replace_job_info(job_id, info)
    return job_id

def gather_info():
    pass


    


def main():
    #print(start_job())
    report_job("5c4c6d39-839e-4a14-b2e4-a733f4f1f30e")



if __name__=="__main__":
    main()

