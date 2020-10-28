import os
import json

file_location = open('/home/cloudera/Documents/job_skills_reco_gen-2/job_requirements.json', 'r')

skills = json.loads(file_location.read()).keys()
file_location = open('/home/cloudera/Documents/job_skills_reco_gen-2/job_requirements.json', 'r')
complete_map = json.loads(file_location.read())

keymap_candidates = dict()

listofmapping = []


def filter(role, jd, skills, fname):
    # global listofmapping
    # print role, jd, skills
    listofmapping = [str(v) + ":" + str(i) for i, v in enumerate(jd) if v in skills]
    if len(listofmapping) >= 2:
        listofmapping.append(fname)
        listofmapping.append(role)
        print "MATCHING", listofmapping, len(listofmapping)
    elif len(listofmapping) < 2:
        listofmapping
        # print "SKILLS MATCHED", len(listofmapping)-2


for keys in skills:
    # print keys, complete_map[keys]
    for root, dirs, files in os.walk('/home/cloudera/Documents/job_skills_reco_gen-2/'):
        for name in files:
            if 'job' not in name:
                with open(os.path.join(root, name)) as f:
                    # key = json.loads(f.read()).keys()[1]
                    val = json.loads(f.read())
                    filter(keys, complete_map[keys], set(val['appln_skills']), name)
                    # print set(val['appln_skills']), name
