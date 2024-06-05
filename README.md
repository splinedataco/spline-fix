= Spline Fix Client

== Set Up Credentials

```bash
export SPLINE_USER=youraccount@forspline.com
 export SPLINE_PASSWORD="your spline password, leading space is important"
```

== Login and Get Credentials

Need some tools

==== jq

https://jqlang.github.io/jq/download/[jq]

For ubuntu/debian:

```bash
sudo apt-get install jq
```

==== Curl

```bash
sudo apt-get install curl
```


==== Socat

Adding TLS capabilities to a client.

=== Socat

```bash
sudo apt install socat
```


```bash
export TOKEN=`curl -X GET --no-progress-meter -u "${SPLINE_USER}:${SPLINE_PASSWORD}" https://splinedata.com/api/login | jq -r .access_token`
```

Examine your token.

```bash
echo $TOKEN
```

== Running The client

=== Help Getting started

Clone the repository someplace that will be referenced as $REPO.

Set up a venv, this is an example.

```bash
cd ~/work/
python3.12 -venv .py312_spline_fix
source ~/work/.py312_spline_fix
```

==== Install as editable

You can install as a wheel, however this repo is intended more as a development toolset.

```bash
cd $REPO
# required build tools
pip install -r build.requirements.txt
# optional development tools potentially useful to some
pip install -r development.requirements.txt
# install as editable
pip install -e .
```

=== Execute the client

Note: ipv4:port are hardcoded currently to 127.0.0.1:36000

==== TLS Tunnel

Open a TLS tunnel.

```bash
socat -v -d -d TCP-LISTEN:36000,bind=127.0.0.1,fork,reuseaddr OPENSSL:fix.splinedata.com:16923
```

==== Run fix client

```bash
python -m joshua.fix.feedhandler.datareader
```
