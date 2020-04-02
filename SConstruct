#Import('env')
#VariantDir('build/main_build', '.')
env = Environment()
#env['CC'] = env['CXX']
env.AppendUnique(CPPDEFINES='SUXDR')
env.SConscript(dirs=['src/cwp'], exports='env',variant_dir='build/cwp_build')
env.SConscript(dirs=['src/par'], exports='env',variant_dir='build/par_build')
env.SConscript(dirs=['src/su'], exports='env',variant_dir='build/su_build')
env.AppendUnique(CPPPATH=[Dir('build/cwp_build/include')])
env.AppendUnique(CPPPATH=[Dir('build/par_build/include')])
env.AppendUnique(CPPPATH=[Dir('build/su_build/include')])
env.SConscript(dirs=['src/main'], exports='env',variant_dir='build/main_build')

