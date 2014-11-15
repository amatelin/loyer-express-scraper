import subprocess

shell = subprocess.Popen("pgrep -f 'scraper-app'", shell=True, stdout=subprocess.PIPE)
output = shell.stdout.read()[6:]

if len(output)>0:
    pass
else:
    subprocess.call("pkill phantomjs", shell=True)
    for i in range(2):
        subprocess.call("sudo python /opt/scripts/loyer-express-scraper/scraper-app.py 4 &", shell=True)