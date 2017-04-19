#!/bin/csh

#PBS -N rec-${user}-${description}
#PBS -A clas12
#PBS -S /bin/csh
#PBS -l nodes=1:ppn=${farm.cpu}
#PBS -l file=${farm.disk}kb
#PBS -l walltime=${farm.time}

"${farm.script}"
