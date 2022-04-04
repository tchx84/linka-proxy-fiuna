# linka-proxy-fiuna

## Install

```
$ sudo dnf install mariadb-devel
$ virtualenv env
$ source env/bin/activate
$ pip install -r requirements.txt
```

## Setup

```
export LINKA_PROXY_FIUNA_HOST=""
export LINKA_PROXY_FIUNA_USER=""
export LINKA_PROXY_FIUNA_PASSWORD=""
export LINKA_PROXY_FIUNA_DATABASE=""
export LINKA_PROXY_FIUNA_TABLE=""
export LINKA_PROXY_FIUNA_LAST_PATH=".last"
export LINKA_PROXY_SERVER_ENDPOINT=""
export LINKA_PROXY_SERVER_API_kEY=""
export LINKA_PROXY_LOG_LEVEL=DEBUG
```

Or put these variables on a `.env` file.
