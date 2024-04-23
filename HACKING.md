# Hacking on csvbase

## Contributions

Gratefully accepted!

But please be patient in PRs - csvbase is a natural product and response times
may vary.

You can always [write to me for help](mailto:cal@calpaterson.com).

## Getting started, with Docker

Running `docker-compose up` should bring up a working instance of the system.

## Getting started, with virtual environments

You can use virtual environments such as python's
[virtualenv](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#installing-virtualenv)
or
[anaconda](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html):

with virtualenv:

```sh
virtualenv venv
source venv/bin/activate
pip install -e .[tests]
```

and anaconda:

```sh
conda create -n csvbase python=3.8
conda activate csvbase
pip install -e .[tests]
```

Depending on your base system, you may also need these prerequisites for `pip
install` to work, which are operating system packages, not python:

- [systemd utility library - development
  files](https://packages.debian.org/sid/libsystemd-dev)
- [header files for libpq5 (PostgreSQL
  library)](https://packages.debian.org/sid/libpq-dev)

On most debian/ubuntu systems, this command suffices:

```sh
sudo apt install libsystemd-dev libpq-dev
```
