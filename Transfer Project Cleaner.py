#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 16 12:20:48 2018

@author: pmaguire
"""

import dxpy
import os
import datetime
import subprocess
import argparse
import time
from dateutil.relativedelta import relativedelta

#Variables
current_date = datetime.datetime.fromtimestamp(time.time())
end_date = current_date - relativedelta(months=+6)
#end_date = "01/01/2017"
#end_date = time.mktime(datetime.datetime.strptime(end_date, "%m/%d/%Y").timetuple())
end_date = time.mktime(end_date.timetuple())
#Second To Millisecond Conversion
end_date = int(end_date*1000)

#Sets Saving Location
#save_location = "/srv/gsfs0/projects/seq_center/Users/pmaguire/DNAnexus_Archiving/"
#os.chdir(save_location)

#Logging
#atexit.register(endlog)
#log("Start Program")

#Grabs All Projects
all_projects = dxpy.api.org_find_projects(object_id="org-scgpm", input_params = {"created": {"before":  end_date}})
#all_projects_1 = dxpy.api.org_find_projects(object_id="org-scgpm")
#print(type(all_projects_1["results"]))
print(len(all_projects["results"]))
#print(len(all_projects_1["results"]))

#App
app = dxpy.DXApp(name="scgpm_clean_raw_data")
raw_data = "/raw_data/"
old_raw = "/tar_files/"

for project in all_projects:
    print(dxpy.describe(project))
    break

"""
while finished != True: 
    #Grabs All Projects
    all_projects = dxpy.api.org_find_projects(object_id="org-scgpm", input_params = {"created": {"before":  end_date}})
    
    for number, project in enumerate(all_projects["results"]):
        project_id = project["id"]
        project_info = dxpy.api.project_describe(project_id)
        project_name = project_info["name"]
        
        print(project_info)

        
        sys.exit
        
        #Checks To Make Sure The First 6 Characters Are Numbers (Date), That This Is The Original Project (Clones Have Characters After The Lane Number), And That The Land Number Is Not A Random Unrelated Substring
        if project_name[0:5].isdigit() == True and re.search("L\d_", project_name) == None and re.search("_L\d", project_name) != None:
            print(project_name)
            #Adds stanford.gssc To Project
            if project_info["level"] == "NONE":
                dxpy.api.project_invite(project_id, input_params = {"invitee": 'user-stanford.gssc', "level": "CONTRIBUTE", "suppressEmailNotification": True})

            project_created = project_info["created"]
            if verbose == True:
                print("Initiating Cleaning On " + project_name)
                print("\tCreation Date: " + datetime.datetime.fromtimestamp(project_created/1000).strftime('%Y-%m-%d %H:%M:%S'))
                print("Size: " + str(project_info["sponsoredDataUsage"]))
                print("Data Usage: " + str(project_info["dataUsage"]))
            
            
            #Deletes non-essential files
            if cleaning == True:    
                app.run(app_input={},project=project_id,folder=raw_data)
            
            if old_cleaning == True and project_name in old_projects:
                app.run(app_input={},project=project_id,folder=old_raw)
            if verbose == True and cleaning == True:
                print("\tJob Submitted Successfully")
                
            #Downloads Data
            if download == True:
                if verbose == True:
                    print("Initiating Download Of " + project_name)
                #Changes To Download Directory 
                os.chdir("/srv/gsfs0/projects/seq_center/Data_Archive/")
                #Make Project Folder
                if version == 3:
                    subprocess.run("mkdir -p", project_name)
                else:
                    subprocess.call(["mkdir", "-p", project_name])
                #Moves Into The New Directory
                os.chdir(project_name)
                #Creates Tmux Session
                if version == 3:
                    subprocess.run("tmux new -s " + project_name + " -d", shell=True) 
                else:
                    subprocess.call("tmux new -s " + project_name + " -d", shell=True) 
                #Selects Project In Dx-tools
                if version == 3:
                    subprocess.run("tmux send-keys -t " + project_name + " 'dx select " + project_name +"' C-m", shell=True)
                else:
                    subprocess.call("tmux send-keys -t " + project_name + " 'dx select " + project_name +"' C-m", shell=True)
                #Pipes Tmux Session To A Named Pipe
                #Note: Removes Previous Named Pipe To Avoid Conflict
                #subprocess.run("rm -f mapping_pipe && mkfifo mapping_pipe && tmux pipe-pane -t " + project_name + " -o 'cat > mapping_pipe'", shell=True)
                #Feeds The Pipe To The Stdout And Stderr
                #poc = subprocess.Popen(['cat', 'mapping_pipe'], stdout=PIPE, stderr=PIPE)
                
                #Writes Folder Check To Temp File
                if version == 3:
                    subprocess.run("tmux send-keys -t " + project_name + " 'dx ls > " + project_name +"_temp.txt' C-m", shell=True)
                else:
                    subprocess.call("tmux send-keys -t " + project_name + " 'dx ls > " + project_name +"_temp.txt' C-m", shell=True)
                
                #Waits Till DNAnexus Gives A Response
                while not os.path.exists(project_name + "_temp.txt"):
                    if verbose == True:
                        print("Waiting")
                    time.sleep(1)
                
                #Waiting Period To Allow System To Write To File
                #Note: If Not Present, Will Process File Instantly Upon Creation And Read A Blank Text Document
                time.sleep(2)
                
                #Reads In Temp File
                if os.path.isfile(project_name + "_temp.txt"):
                    with open(project_name + "_temp.txt", "r") as temp:
                        folders = temp.read().splitlines()
                
                #Extreme Safety Check
                if folders == []:
                    time.sleep(10)
                    #Reads In Temp File
                    if os.path.isfile(project_name + "_temp.txt"):
                        with open(project_name + "_temp.txt", "r") as temp:
                            folders = temp.read().splitlines()
                
                #Checks Whether Mapping Was Done Or Not
                #subprocess.run("tmux send-keys -t " + project_name + " 'dx ls' C-m", shell=True, stdout=subprocess.PIPE)
                #subprocess.Popen(["tmux", "send-key", "-t", project_name, "dx ls", "C-m"])

                if "stage1_bwa/" in folders:
                    if version == 3:
                        subprocess.run("tmux send-keys -t " + project_name + " 'dx download -raf raw_data/ stage0_bcl2fastq/ stage2_qc/ stage3_qc_report/' C-m", shell=True)
                    else:
                        subprocess.call("tmux send-keys -t " + project_name + " 'dx download -raf raw_data/ stage0_bcl2fastq/ stage2_qc/ stage3_qc_report/' C-m", shell=True)
                else:
                    if version == 3:
                        subprocess.run("tmux send-keys -t " + project_name + " 'dx download -raf raw_data/ stage0_bcl2fastq/ stage1_qc/ stage2_qc_report/' C-m", shell=True)
                    else:
                        subprocess.call("tmux send-keys -t " + project_name + " 'dx download -ra raw_data/ stage0_bcl2fastq/ stage1_qc/ stage2_qc_report/' C-m", shell=True)
                        
                #Deletes Temp File
                if version == 3:
                    subprocess.run("tmux send-keys -t " + project_name + " 'rm -f " + project_name + "_temp.txt' C-m", shell=True)
                else:
                    subprocess.call("tmux send-keys -t " + project_name + " 'rm -f " + project_name + "_temp.txt' C-m", shell=True)
            
                #Completion Message
                if version == 3:
                    subprocess.run("tmux send-keys -t " + project_name + " 'echo 'Finished Downloading All Files'' C-m", shell=True)
                else:
                    subprocess.call("tmux send-keys -t " + project_name + " 'echo 'Finished Downloading All Files'' C-m", shell=True)
                finished = True
        if finished == True:
            break
        else:
            #Ignores Projects That Weren't Automatically Created Or Are Copies
            pass
    else:
        print("Skipped " + project_name)
    
    if all_projects["next"] == None:
        finished = True
    else:
        end_date = project_created
        if verbose == True:
                print("_______________________TRANSITION_________________________")
"""