# Red Pitaya Streaming Interface for GNU Radio

This repository contains the custom GNU Radio integration developed for direct
ADC and DAC streaming with a Red Pitaya STEMlab using the official
Data Stream Control / Streaming API.

The implementation provides a shared Python backend (`rp_shared_dual`) that
manages the ADC and DAC streaming clients of one Red Pitaya and allows GNU Radio
Embedded Python Blocks to access the device without creating conflicting
connections.

The implementation was developed as part of a Master's thesis on a free-space
optical 4-PAM demonstrator.

## Requirements

The following components are required:

- GNU Radio
- Python and NumPy
- Red Pitaya Data Stream Control / Streaming Client
- The Python modules contained in this repository
- A Red Pitaya with the Streaming application available

The Red Pitaya Streaming Client should be downloaded directly from the Red
Pitaya that will be used. This ensures that the native Python library matches
the software version installed on the board.

Open the Red Pitaya web interface using

    http://<RED_PITAYA_IP>

and start the **Data Stream Control** application. The corresponding client can
be downloaded from the **Clients** section.
A working set for the STEMlab125-14 Pro Gen 2 is given within this repository.

> Important: The Data Stream Control application must be active on the
> Red Pitaya while using the streaming interface.

## Clone this repository

Clone the repository to a permanent location:

```bash
git clone <repository-url>
```
The directory containing rp_shared_dual.py must later be available in the
Python module search path.

## Windows Installation

The Windows implementation was tested using GNU Radio installed through
Radioconda.

First download the given rp_shared_dual.py and put it in a location like:

```bash
C:\RedPitaya\GNURadio
```

Second, open a Radioconda Prompt and verify the Python environment:

```bash
where python
where gnuradio-companion
python --version
python -c "import sys,platform; print(sys.executable); print(sys.version); print(platform.architecture())"
```

A tested configuration used Python 3.12.x.
Download the Windows Streaming Client from the Red Pitaya Data Stream
Control application and extract it, for example to

```bash
C:\RedPitaya\StreamingClient
```

The python_lib directory should contain the Python wrapper and a native
.pyd library matching the Python version in use.
For Python 3.12, the native library must therefore be compiled for Python 3.12.
Test the Red Pitaya streaming module from the client directory:

```bash
python -c "import streaming; print(streaming.__file__)"
python -c "import streaming; print(streaming.ConfigStreamClient); print(streaming.ADCStreamClient); print(streaming.DACStreamClient)"
```

Next, determine the Radioconda site-packages directory:

```bash
python -c "import site; print(site.getsitepackages()[1])"
```

Create a file named

```bash
redpitaya_local.pth
```

inside this directory. The file should contain the paths to the Red Pitaya
streaming library and this repository, for example:

```bash
C:\RedPitaya\StreamingClient\python_lib
C:\RedPitaya\GNUradio
```

Finally, verify the complete Python environment:

```bash
python -c "import numpy; import gnuradio; import streaming; import rp_shared_dual; print('ALL IMPORTS OK')"
```

If all imports succeed, start GNU Radio from the same environment:

```bash
gnuradio-companion
```

## Linux Installation

First download the given rp_shared_dual.py and put it in a location like:

```bash
\home\user\RedPitaya\GNURadio
```

The Linux implementation was tested under Ubuntu with GNU Radio 3.10.9.2 and
Python 3.12.3.
First verify which Python interpreter is used:

```bash
which python3
which gnuradio-companion
python3 --version

python3 -c "import sys,platform; print(sys.executable); print(sys.version); print(platform.architecture())"

python3 -c "import gnuradio; print(gnuradio.__file__)"
```

Download the Linux Streaming Client directly from the Red Pitaya Data
Stream Control application and extract it, for example to

```bash
\home\user\RedPitaya\StreamingClient
```

The native Red Pitaya Python library must match the Python version used by GNU
Radio.
For Python 3.12, the relevant files may for example be named
