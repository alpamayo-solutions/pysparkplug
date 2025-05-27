# **PySparkplug**: Sparkplug B for Python

[![CI/CD: n/a](https://github.com/matteosox/pysparkplug/actions/workflows/cicd.yaml/badge.svg)](https://github.com/matteosox/pysparkplug/actions/workflows/cicd.yaml)
[![Docs: n/a](https://readthedocs.org/projects/pysparkplug/badge/?version=stable)](https://pysparkplug.mattefay.com)
[![Downloads: n/a](https://static.pepy.tech/personalized-badge/pysparkplug?period=total&units=none&left_color=grey&right_color=blue&left_text=Downloads)](https://pepy.tech/project/pysparkplug)
[![PyPI: n/a](https://img.shields.io/badge/dynamic/json?color=blueviolet&label=PyPI&query=%24.info.version&url=https%3A%2F%2Fpypi.org%2Fpypi%2Fpysparkplug%2Fjson)](https://pypi.org/project/pysparkplug/)
[![codecov: n/a](https://codecov.io/gh/matteosox/pysparkplug/branch/main/graph/badge.svg?token=8VKKDG9SMZ)](https://codecov.io/gh/matteosox/pysparkplug)

## About this Fork

This fork of the `pysparkplug` package, implementing the `Sparkplug-B` specfication, intends to remain interoperability with the specification, while making some aspects of the standard optional, when used internally. Deviations from the standard are:

- retention of birth certificates
- optionally disable Protobuf payload encodings (they are replaced by human-readable JSON payloads)

Furthermore, this fork provides:

- a `HostApplication` class based on the specification
- a `DataOpsNode` class (not described in the specification, but useful from our point of view)
- an automatic rebirthing mechanism for `EdgeNodes` and `Devices`

## Getting Started

### Installation

`pysparkplug` uses the Eclipse Paho™ MQTT Python Client, i.e. [`paho-mqtt`](https://github.com/eclipse/paho.mqtt.python), for low-level MQTT communication.

### Usage

Simple demos of the `EdgeNode`, `Device`, and `Client` classes publishing and subscribing all supported payloads and metric datatypes can be found in the `notebooks` directory. To run them dynamically, you'll need to install Docker and run `just notebooks` before opening up your local browser to http://localhost:8888. The password is `bokchoy`.

## Features

### Fully type annotated

`pysparkplug`'s various interfaces are fully type annotated.
