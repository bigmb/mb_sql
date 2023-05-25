#!/usr/bin/env python3
# -*- coding: utf-8 -*-

##Run this file to install the package and upload it to pipy. The commit needs to be done before.

import os
import subprocess
file = '/home/malav/mb_sql'

#subprocess.run(["cd",file]), check=True, stdout=subprocess.PIPE).stdout
os.system('cd ' + file)

os.system('./make_version.sh')

print("version file updated")
print('*'*100)

subprocess.run(["git", "add", "."], check=True, stdout=subprocess.PIPE).stdout
subprocess.run(["git", "commit", "-am", "Bug fix commit"], check=True, stdout=subprocess.PIPE).stdout

print('git commit done')

subprocess.run(["git", "pull"], check=True, stdout=subprocess.PIPE).stdout
print('git pull done')
print('*'*100)

subprocess.run(["git", "push"], check=True, stdout=subprocess.PIPE).stdout
print('*'*100)
print('removing dist and build folders')

if os.path.exists(file+'/dist'):
    os.system('sudo rm -rf '+file+'/dist')
    os.system('sudo rm -rf '+file+'/build')
#subprocess.run(["ls"]),check=True, stdout=subprocess.PIPE).stdout
os.system("ls")

os.system('python3.8 -m setup bdist_wheel')

print('*'*100)
print('wheel built')
os.system('python3.8 -m pip install '+file + '/dist/' +os.listdir(file +'/dist')[-1])

print('package installed')
print('*'*100)
os.system('python3.8 -m twine upload dist/*')