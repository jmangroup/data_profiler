#import required package
import os
from dotenv import dotenv_values

# Read the env variables
config = dotenv_values(".env")
if len(config)==0:
    config = os.environ

# Read the sample.profiles.yml file
f = open('ci/sample.profiles.yml', 'r', encoding='utf-8')
data = f.read()
f.close()

data = data.replace('$', '')
# Writing the actual profile.yml file
final = open('profiles.yml', 'w', encoding='utf-8')
final.write(data.format(**config))
final.close()