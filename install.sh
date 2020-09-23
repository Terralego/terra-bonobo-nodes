
set -ex
apt update -yq
apt install -yqq python3-pip
DEBIAN_FRONTEND=noninteractive apt install -yqq $(grep -vE "^\s*#" apt.txt  | tr "\n" " ")
python3 -m venv venv/
. venv/bin/activate
pip install wheel
pip install -r requirements.txt -r test-requirements.txt
# vim:set et sts=4 ts=4 tw=80:
