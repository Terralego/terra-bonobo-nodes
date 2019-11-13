
set -ex
apt-get update -yq
apt-get install -yqq python3-pip
apt-get install -yqq $(grep -vE "^\s*#" apt.txt  | tr "\n" " ")
python3 -m venv venv/
. venv/bin/activate
pip install wheel
pip install -r requirements.txt -r test-requirements.txt
# vim:set et sts=4 ts=4 tw=80:
