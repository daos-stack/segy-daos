import os

env = Environment(ENV=os.environ)

AddOption('--mpi',
          dest='mpi',
          type='int',
          nargs=1,
          action='store',
          metavar='MPI',
          help='specify using mpi')


env.Append(MPI = GetOption('mpi'))

if GetOption('mpi'):
	env = Environment(CC = 'mpicc', CPPDEFINES={'MPI_BUILD' : '${1}'},  ENV=os.environ)

env.SConscript(dirs=['src'], exports='env', duplicate=0)
env.SConscript(dirs=['tests'], exports='env', duplicate=0)
env.SConscript(dirs=['mains'], exports='env',variant_dir='./build/mains_build', duplicate=0)



