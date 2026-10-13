#
#
# @file        4PAM Preamble Extractor GNU Radio Custom Python Block
# @description Detects a given sequence of symbol (preamble) within a
#              continous symbol stream. It also checks, if the 
#              distance between preambles is equal to the required
#              payload length in symbols. Optionally bad frames can be
#              filled with a fixed sequence.
# 
# @author      Vincent Ebert
# @created     September 2026
# @institution Berliner Hochschule für Technik (BHT)
# @context     Master's Thesis
# 
# @disclaimer  This source code was generated and optimized with the assistance of 
#              Artificial Intelligence (AI) - ChatGPT-5.6 Sol.
#              The author retains full responsibility 
#              for its functionality, validation, and academic integrity.
#
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

from collections import deque
import atexit
import threading
import time
from typing import Deque, Dict, Optional

import numpy as np
import streaming # see https://github.com/RedPitaya/RedPitaya/tree/master/apps-tools/streaming_manager/src/streaming_api_lib/src

_INSTANCES: Dict[str, "RPBoard"] = {} # global dictionary for connecte Red Pitaya boards
_INSTANCES_LOCK = threading.RLock() # threading lock to prevent race conditions

_RANGE_LABELS = {"LV": "+/- 1V", "HV": "+/- 20V"} # dictionary for input modes, STEMlab125-14 Pro Gen 2 supports LV and HV

# validate transmission setting 1 byte (8) or 2 byte (16)
def _norm_bits(value, name):
    value = int(value)
    if value not in (8, 16):
        raise ValueError(f"{name} must be 8 or 16")
    return value

# implement one board per IP (multiton-pattern)
def get_board(host="192.168.178.74", *, block_size=65536, verbose=False):
    """Return one shared backend per Red Pitaya IP address."""
    host = str(host)
    block_size = int(block_size) # size of transmitted packets, between 2 kB and 8 MB
    verbose = bool(verbose)
    if block_size <= 0:
        raise ValueError("block_size must be positive")

    with _INSTANCES_LOCK: # thread-proof access to connected boards
        board = _INSTANCES.get(host)
        if board is None or board.closed: # no board instance or has been closed
            board = RPBoard(host, block_size, verbose) # new instance for the board
            _INSTANCES[host] = board
        elif board.block_size != block_size: # check for existing instance and block size match
            raise RuntimeError(              # block size needs to be configured per ADC and DAC, with only one board they must match
                f"RP[{host}] block_size mismatch: existing={board.block_size}, requested={block_size}"
            )
        return board

# terminating all board connections
def shutdown_all():
    with _INSTANCES_LOCK:
        boards = list(_INSTANCES.values())
    for board in boards:
        try:
            board.shutdown(force=True)
        except Exception: # if not possible, try next one (best effort)
            pass
# execute above function when exiting via sys.exit -> by termintating the flow graph in GNU-Radio
atexit.register(shutdown_all)

# callback via base class within Red Pitayas "streaming" library
class _ConfigCallback(streaming.ConfigCallback):
    def __init__(self, owner):
        super().__init__()
        self.owner = owner

    def configConnected(self, client, host): # log-level output
        print(f"RP[{host}] configuration connected")

    def configError(self, client, host, code): # log-level output
        self.owner.set_error(f"RP[{host}] configuration error, code {code}")

    def configErrorTimeout(self, client, host): # log-level output
        self.owner.set_error(f"RP[{host}] configuration timeout")

# callback via base class within Red Pitayas "streaming" library, retrieves ADC readings
class _ADCCallback(streaming.ADCCallback):
    def __init__(self, owner):
        super().__init__()
        self.owner = owner

    def receivePack(self, client, pack): # retrieves ADC readings
        channel = pack.channel1 if self.owner.adc_channel == 1 else pack.channel2
        count = int(channel.samples)
        if count <= 0: # no samples > leave
            return

        dtype = np.int8 if self.owner.adc_bits == 8 else np.int16 # set base type according to transmission setting, see https://redpitaya.readthedocs.io/en/latest/appsFeatures/applications/streaming/reference/technical_details.html
        raw = np.asarray(channel.raw).astype(dtype, copy=True) # copy raw data to array, force a true copy to prevent loss to unknown buffers
        if raw.size > count: # limit to amount of samples to leave possible padding behind
            raw = raw[:count]

        with self.owner.rx_condition: # threading race condiging, produce-consumer-pattern
            # check if current data would overflow the receiver
            overflow = self.owner.rx_available + raw.size - self.owner.rx_fifo_samples
            while overflow > 0 and self.owner.rx_chunks: # overflow condition
                first = self.owner.rx_chunks[0] # oldest element in retrieved chunks (each ADC data packet is a chunk)
                remaining = first.size - self.owner.rx_head_offset # remaining samples within oldest chunk
                remove = min(overflow, remaining) # remove as much samples as necessary (overflow), but not more then within the oldest chunk (remaining)
                self.owner.rx_head_offset += remove # increase sample counter
                self.owner.rx_available -= remove # remove samples
                self.owner.rx_host_dropped += remove # increase statistic for discarded data
                overflow -= remove # lower overlow, if overflow is bigger then remove -> move to second oldest chunk (now the oldest)
                if self.owner.rx_head_offset >= first.size: # oldest chunk is fully depleted -> remove chunk from queue
                    self.owner.rx_chunks.popleft()
                    self.owner.rx_head_offset = 0

            if raw.size > self.owner.rx_fifo_samples: # new data is bigger then the whole Rx buffer
                remove = raw.size - self.owner.rx_fifo_samples # amount to be discarded
                raw = raw[-self.owner.rx_fifo_samples:] # just keep the newest data
                self.owner.rx_host_dropped += remove # increase stat counter for removed packets

            self.owner.rx_chunks.append(raw) # append new data
            self.owner.rx_available += raw.size # increase counter for available data
            self.owner.rx_fpga_lost += int(channel.fpgaLost) # check for FPGA data loss (integrated within streaming library)
            self.owner.rx_packets += 1 # increase packet counter
            self.owner.rx_condition.notify_all() # notify all threads that wait for new data

    def connected(self, client, host): # log-level output
        print(f"RP[{host}] ADC connected ({self.owner.adc_bits}-bit)")

    def disconnected(self, client, host): # log-level output
        if self.owner.running:
            self.owner.set_error(f"RP[{host}] ADC disconnected")

    def error(self, client, host, code): # log-level output
        self.owner.set_error(f"RP[{host}] ADC error, code {code}")

# callback via base class within Red Pitayas "streaming" library, send DAC values
# returns False for success and True for Error!
class _DACCallback(streaming.DACCallback):
    def __init__(self, owner):
        super().__init__()
        self.owner = owner

    def _fill(self, ch1, ch2, size, bits): # get DAC values for transmission
        try:
            size = int(size)
            data = self.owner.dequeue_tx_for_dac(size) # get data from tx queue
            active = ch1 if self.owner.dac_channel == 1 else ch2 # select channel
            inactive = ch2 if self.owner.dac_channel == 1 else ch1

            if bits == 8: # if transmission is 1 byte per sample
                data = np.asarray(data, dtype=np.int8)
                if active is not None:
                    active.cast("b")[:] = data # b = signed char (8-bit) for memoryview objects
                if inactive is not None:
                    inactive.cast("b")[:] = np.zeros(size, dtype=np.int8) # zeros
            else: # transmission is 2 byte per sample
                data = np.asarray(data, dtype=np.int16)
                if active is not None:
                    active.cast("h")[:] = data # h = signed short (16 bit) for memoryview objects
                if inactive is not None:
                    inactive.cast("h")[:] = np.zeros(size, dtype=np.int16) # zeros
            return False # False = successfull
        except Exception as exc:
            self.owner.set_error(f"RP[{self.owner.host}] DAC callback failed: {exc}")
            return True # True = Callback failed

    def streamData16Bit(self, client, ch1, ch2, size): # stream data as 16 bit, if not configured correctly return True, else continue
        return True if self.owner.dac_bits != 16 else self._fill(ch1, ch2, size, 16)

    def streamData8Bit(self, client, ch1, ch2, size): # stream data as 8 bit, if not configured correctly return True, else continue
        return True if self.owner.dac_bits != 8 else self._fill(ch1, ch2, size, 8)

    def sentPack(self, client, ch1_size, ch2_size): # logging for send packet
        self.owner.tx_packets += 1

    def connected(self, client, host): # log-level output
        print(f"RP[{host}] DAC connected ({self.owner.dac_bits}-bit)")

    def disconnected(self, client, host): # log-level output
        if self.owner.running:
            self.owner.set_error(f"RP[{host}] DAC disconnected")

    def error(self, client, host, code): # log-level output
        self.owner.set_error(f"RP[{host}] DAC error, code {code}")

    def stopped(self, client, host): # log-level output
        if self.owner.running and self.owner.dac_started:
            self.owner.set_error(f"RP[{host}] DAC stopped")

    def stoppedEmpty(self, client, host): # log-level output
        if self.owner.running and self.owner.dac_started:
            self.owner.set_error(f"RP[{host}] DAC stream became empty")

    def stoppedMemError(self, client, host): # log-level output
        self.owner.set_error(f"RP[{host}] DAC memory error")

    def stoppedMemModify(self, client, host): # log-level output
        self.owner.set_error(f"RP[{host}] DAC memory changed")

# represent a Red Pitaya STEMlab125-14 Pro Gen 2 as its own class
class RPBoard:
    def __init__(self, host, block_size, verbose):
        self.host = str(host)
        self.block_size = int(block_size) # size of transmitted packets, between 2 kB and 8 MB
        self.verbose = bool(verbose)

        self.state_lock = threading.RLock() # separate lock for config/life-cycle data
        self.error_lock = threading.RLock() # separate lock for error logging
        self.error = None
        self.closed = False
        self.running = False

        self.config = None
        self.config_callback = None
        self.stream_host = None

        # ADC role state
        self.adc = None
        self.adc_callback = None
        self.adc_users = 0
        self.adc_started = False
        self.adc_decimation = None
        self.adc_channel = None
        self.adc_bits = None
        self.adc_input_range = None
        self.adc_voltage_range = None
        self.adc_size = None
        self.rx_fifo_samples = None

        # DAC role state
        self.dac = None
        self.dac_callback = None
        self.dac_users = 0
        self.dac_started = False
        self.dac_starting = False
        self.dac_rate = None
        self.dac_channel = None
        self.dac_bits = None
        self.dac_size = None
        self.tx_fifo_samples = None
        self.tx_prefill_samples = None
        self.tx_underflow_mode = None

        # RX FIFO
        self.rx_condition = threading.Condition()
        self.rx_chunks: Deque[np.ndarray] = deque()
        self.rx_head_offset = 0
        self.rx_available = 0
        self.rx_packets = 0
        self.rx_fpga_lost = 0
        self.rx_host_dropped = 0

        # TX FIFO
        self.tx_condition = threading.Condition()
        self.tx_chunks: Deque[np.ndarray] = deque()
        self.tx_head_offset = 0
        self.tx_available = 0
        self.tx_last_sample = 0
        self.tx_packets = 0
        self.tx_runtime_underflow = 0
        self.tx_shutdown_padding = 0
        self.tx_fifo_waits = 0
        self.tx_wait_time = 0.0
        self.tx_min_available_after_start = None
        self.tx_max_available = 0

    def set_error(self, message): # set/store error message
        with self.error_lock:
            if self.error is None:
                self.error = str(message) # store error message
                print(self.error, flush=True) # print error
        with self.tx_condition:
            self.tx_condition.notify_all() # inform tx thread
        with self.rx_condition:
            self.rx_condition.notify_all() # inform rx thread

    def raise_if_error(self): # raise error message
        with self.error_lock:
            error = self.error # converts error message to real exception
        if error is not None:
            raise RuntimeError(error)

    def _ensure_config(self): # lazy initialisation, called from aquire_adc or aquire_dac
        with self.state_lock: # prevent race condition
            if self.config is not None: # already configured
                return
            if self.closed:
                raise RuntimeError(f"RP[{self.host}] backend is closed")

            self.config = streaming.ConfigStreamClient() # create object from streaming library
            self.config_callback = _ConfigCallback(self) # register callbacks
            self.config.addCallback(self.config_callback) # actually add callbacks
            self.config.setVerbose(self.verbose) # debugging setting

            if not self.config.connect([self.host]): # connect to host
                raise RuntimeError(f"RP[{self.host}] configuration connection failed")

            # Shared by ADC and DAC, therefore configured here once.
            self.config.sendConfig("block_size", str(self.block_size))

            hosts = self.config.getHosts() # get actual connected host, to verify
            if not hosts: # theoretically connected, but no host registered
                raise RuntimeError(f"RP[{self.host}] configuration returned no host")
            self.stream_host = self.host if self.host in hosts else hosts[0] # safe the requested host from init function call or the first entry
            self.running = True # set running flag
            print(f"RP[{self.host}] backend ready: block_size={self.block_size} B")

    @property # read only parameter of the board, therefore @property
    def adc_rate(self): # returns actual ADC sample rate
        return None if self.adc_decimation is None else 125_000_000.0 / self.adc_decimation

    def acquire_adc(
        self,
        *,
        adc_decimation,
        adc_channel,
        adc_bits,
        input_range,
        adc_size,
        rx_fifo_samples,
    ):
        """
        Configure and start ADC streaming.

        input_range:
            "LV" -> 1:1 input path, full-scale range +/-1 V
            "HV" -> 1:20 input path, full-scale range +/-20 V

        The physical STEMlab 125-14 ADC has 14-bit resolution.
        Therefore the documentation states 8192 counts for positive full scale.
        However, for BIT_16 streaming the signed raw values seem to be converted
        using 32768 counts for positive full scale. BIT_8 streaming transports
        an 8-bit representation and is converted using 128 counts for
        positive full scale.
        https://redpitaya.readthedocs.io/en/latest/appsFeatures/applications/streaming/reference/technical_details.html#data-formats-and-conversion
        """
        # Normalization of all inputs
        adc_decimation = int(adc_decimation)
        adc_channel = int(adc_channel)
        adc_bits = _norm_bits(adc_bits, "adc_bits")
        input_range = str(input_range).strip().upper()
        adc_size = int(adc_size)
        rx_fifo_samples = int(rx_fifo_samples)

        if not 1 <= adc_decimation <= 65536: # verify adc decimation range
            raise ValueError("invalid adc_decimation")
        if adc_decimation <= 16 and adc_decimation not in (1, 2, 4, 8, 16): # verify adc decimation below 16, see https://redpitaya.readthedocs.io/en/latest/appsFeatures/examples/acquisition/acqRF-samp-and-dec.html
            raise ValueError("invalid adc_decimation: adc_decimation <= 16 must be 1, 2, 4, 8 or 16.")
        if adc_channel not in (1, 2): # verify channel setting
            raise ValueError("adc_channel must be 1 or 2")
        if input_range not in ("LV", "HV"): # verify input range
            raise ValueError("input_range must be 'LV' or 'HV'")
        if adc_size <= 0 or rx_fifo_samples <= 0: # verify sizes
            raise ValueError("adc_size and rx_fifo_samples must be positive")
        for ch in (adc_channel,):
            print(f"IN{ch}: Input range set to {input_range} ({_RANGE_LABELS[input_range]}). Verify with onboard jumper.")

        voltage_range = 1.0 if input_range == "LV" else 20.0

        requested = ( # store requested parameter, to ensure only one config
            adc_decimation,
            adc_channel,
            adc_bits,
            input_range,
            adc_size,
            rx_fifo_samples,
        )

        with self.state_lock: # prevent race conditions
            self._ensure_config() # initialization if neccessary
            
            # multiple instances can call the adc-stream, as long as the config matches
            if self.adc_users: # ADC already running
                existing = ( # get existing config
                    self.adc_decimation,
                    self.adc_channel,
                    self.adc_bits,
                    self.adc_input_range,
                    self.adc_size,
                    self.rx_fifo_samples,
                )
                if requested != existing: # check if config mismatch
                    raise RuntimeError(
                        f"RP[{self.host}] ADC configuration mismatch: "
                        f"existing={existing}, requested={requested}"
                    )

                self.adc_users += 1 # config matches > +1 user
                return

            # set parameters for not already running config
            self.adc_decimation = adc_decimation
            self.adc_channel = adc_channel
            self.adc_bits = adc_bits
            self.adc_input_range = input_range
            self.adc_voltage_range = voltage_range
            self.adc_size = adc_size
            self.rx_fifo_samples = rx_fifo_samples

            # ADC-only Red Pitaya settings.
            self.config.sendConfig("adc_pass_mode", "NET") # network streaming
            self.config.sendConfig("adc_decimation", str(adc_decimation))
            self.config.sendConfig("resolution", "BIT_8" if adc_bits == 8 else "BIT_16")
            self.config.sendConfig("adc_size", str(adc_size))

            # Use the Red Pitaya input attenuation that corresponds to the
            # physical LV/HV input range.
            attenuator = ("A_1_1" if input_range == "LV" else "A_1_20")
            self.config.sendConfig(f"channel_attenuator_{adc_channel}", attenuator)

            # OS 3.00-57+ supports FPGA-based calibration in streaming.
            self.config.sendConfig("use_calib", "ON")
            
            # set used channel active, otherwise off
            self.config.sendConfig("channel_state_1", "ON" if adc_channel == 1 else "OFF")
            self.config.sendConfig("channel_state_2", "ON" if adc_channel == 2 else "OFF")

            # start actual ADC streaming
            self.adc = streaming.ADCStreamClient(self.config)
            self.adc_callback = _ADCCallback(self) # instantiates custom class _ADCCallback functions
            self.adc.setCallback(self.adc_callback)
            self.adc.setVerbose(self.verbose)
            self.adc_users = 1 # first user

            if not self.adc.startStreaming(): # startup fails
                self.adc_users = 0
                raise RuntimeError(f"RP[{self.host}] ADC start failed")

            self.adc_started = True # started successfully

            print( # log-level output
                f"RP[{self.host}] ADC stream started: "
                f"IN{adc_channel}, "
                f"{self.adc_rate:.3f} S/s, "
                f"{adc_bits}-bit transport, "
                f"{input_range} (+/-{voltage_range:g} V), "
                f"ADC memory={adc_size} B"
            )
    
    # prepare DAC streaming WITHOUT actually starting
    def acquire_dac(self, *, dac_rate, dac_channel, dac_bits, dac_size,
                    tx_fifo_samples, tx_prefill_samples, tx_underflow_mode):
        # Normalization of Inputs
        dac_rate = int(dac_rate)
        dac_channel = int(dac_channel)
        dac_bits = _norm_bits(dac_bits, "dac_bits")
        dac_size = int(dac_size)
        tx_fifo_samples = int(tx_fifo_samples)
        tx_prefill_samples = int(tx_prefill_samples)
        tx_underflow_mode = str(tx_underflow_mode).strip().lower()

        if dac_rate <= 0: # verify dac rate
            raise ValueError("dac_rate must be positive")
        if dac_channel not in (1, 2): # verify channel
            raise ValueError("dac_channel must be 1 or 2")
        if dac_size <= 0 or tx_fifo_samples <= 0: # verify size
            raise ValueError("dac_size and tx_fifo_samples must be positive")
        if not 0 <= tx_prefill_samples <= tx_fifo_samples: # verify size
            raise ValueError("tx_prefill_samples must fit in TX FIFO")
        if tx_underflow_mode not in ("hold", "zero"): # verify underflow setting
            raise ValueError("tx_underflow_mode must be 'hold' or 'zero'")
        if dac_bits == 8 and not hasattr(streaming, "DAC_8BIT"): # verify 8-bit streaming is supported
            raise RuntimeError("Installed streaming module does not expose DAC_8BIT")

        requested = ( # get requested config
            dac_rate, dac_channel, dac_bits, dac_size,
            tx_fifo_samples, tx_prefill_samples, tx_underflow_mode
        )

        with self.state_lock: # prevent race condition
            self._ensure_config() # initilization if neccessary
            if self.dac_users: # DAC already running
                existing = ( # get already existing config
                    self.dac_rate, self.dac_channel, self.dac_bits, self.dac_size,
                    self.tx_fifo_samples, self.tx_prefill_samples, self.tx_underflow_mode
                )
                if requested != existing: # config mismatch
                    raise RuntimeError(
                        f"RP[{self.host}] DAC configuration mismatch: "
                        f"existing={existing}, requested={requested}"
                    )
                self.dac_users += 1 # config matches > +1 user
                return

            (
                self.dac_rate, self.dac_channel, self.dac_bits, self.dac_size,
                self.tx_fifo_samples, self.tx_prefill_samples, self.tx_underflow_mode
            ) = requested # not already running, set request as config

            # DAC-only configuration
            self.config.sendConfig("dac_pass_mode", "DAC_NET") # network streaming
            self.config.sendConfig("dac_rate", str(dac_rate))
            self.config.sendConfig("dac_size", str(dac_size))

            self.dac = streaming.DACStreamClient(self.config)
            self.dac_callback = _DACCallback(self) # instantiates custom class _DACCallback functions
            self.dac.setCallback(self.dac_callback)
            self.dac.setVerbose(self.verbose)
            self.dac_users = 1

            print( # log-level output
                f"RP[{self.host}] DAC armed: OUT{dac_channel}, {dac_rate} S/s, "
                f"{dac_bits}-bit, DAC memory={dac_size} B, prefill={tx_prefill_samples}"
            )

    # start dac if enough data to be transmitted is ready
    def start_dac_if_ready(self):
        with self.state_lock: # prevent race condition
            if self.dac is None or self.dac_started or self.dac_starting or self.dac_users <= 0 or not self.running:
                return # no dac or already running/started/starting
            with self.tx_condition: # hold tx-condition lock to safe Tx-buffer state
                ready = self.tx_available >= self.tx_prefill_samples # enough data ready?
            if not ready:
                return # not ready

            fmt_name = "DAC_8BIT" if self.dac_bits == 8 else "DAC_16BIT" # get DAC resolution for transmission (1 Byte or 2 Byte)
            fmt = getattr(streaming, fmt_name, None) # get format from streaming library
            if fmt is None:
                self.set_error(f"RP[{self.host}] no streaming.{fmt_name}") # Error, no transmission size set
                return

            self.dac_starting = True # set startup true to prevent multiple instances also thread locking is active
            try:
                ok = self.dac.startStreamingFromMemorySink( # start streaming in continous mode
                    self.stream_host,
                    self.dac_channel == 1,
                    self.dac_channel == 2,
                    fmt,
                )
                if not ok: # stream startup failed
                    raise RuntimeError("startStreamingFromMemorySink failed")
                self.dac_started = True # startup complete
                print(f"RP[{self.host}] DAC stream started on OUT{self.dac_channel} ({self.dac_bits}-bit)")
            except Exception as exc:
                self.set_error(f"RP[{self.host}] DAC start failed: {exc}")
            finally: # prevent lockout
                self.dac_starting = False # starting complete, back to false
    
    # tranmitting data via DAC (producer)
    def enqueue_tx_blocking(self, samples, stop_event: Optional[threading.Event] = None):
        offset = 0 # keeps track of position within sample chunk
        while offset < samples.size: # samples left to transmit
            self.raise_if_error() # check for error
            if stop_event is not None and stop_event.is_set():
                break # stop event detected, e.g. Ctrl+C
            if not self.running:
                break # Red Pitaya not running

            with self.tx_condition: # set transmit condition to prevent multiple instances, also threading lock is active
                free = self.tx_fifo_samples - self.tx_available # check available space within Tx-buffer
                if free <= 0: # fifo is full
                    start = time.monotonic() # get starting time for diagnostics
                    self.tx_fifo_waits += 1 # increase waiting counter
                    self.tx_condition.wait(timeout=0.05) # wait 50 ms non-blocking (thread "sleeps")
                    self.tx_wait_time += time.monotonic() - start # safe wait time for statisctic
                    continue # back to top

                count = min(free, samples.size - offset) # max number of samples: either remaining spaze in Tx-buffer or Samples ready
                self.tx_chunks.append(samples[offset:offset + count].copy()) # append chunks to be transmitted
                self.tx_available += count # increase counter for available transmit chunks
                self.tx_max_available = max(self.tx_max_available, self.tx_available) # set available maximum
                offset += count # increase offset
                self.tx_condition.notify_all() # notify all threads

            self.start_dac_if_ready() # transmit data if needed size is reached
        return offset # return amount of samples that werde set into transmit queue
    
    # transmitting data via DAC (consumer) -  see _DACCallback._fill
    def dequeue_tx_for_dac(self, size):
        dtype = np.int8 if self.dac_bits == 8 else np.int16 # set data format accordinly to transmission 
        out = np.empty(int(size), dtype=dtype) # create empty array with final size
        written = 0 # initialize counter

        with self.tx_condition: # set tx condition to prevent multiple instances
            while written < out.size: # data left to transmit
                while self.tx_available == 0 and self.running and self.error is None: # wait for transmission
                    start = time.monotonic() # get timer for statistic
                    self.tx_fifo_waits += 1 # count wait period
                    self.tx_condition.wait(timeout=0.05) # non-blocking
                    self.tx_wait_time += time.monotonic() - start # safe wait time

                if not self.running or self.error is not None:
                    break # error or stopped
                if not self.tx_chunks:
                    continue # no chunks to transmit

                chunk = self.tx_chunks[0] # load oldest chunk in transmission queue
                remaining = chunk.size - self.tx_head_offset # get remaining data within chunk
                count = min(out.size - written, remaining) # get the count of remaining data
                out[written:written + count] = chunk[self.tx_head_offset:self.tx_head_offset + count] # copy data from transmission queue to local variable
                written += count # increase counter for written data
                self.tx_head_offset += count # increase head offset (valid within chunk)
                self.tx_available -= count # reduce available transmission data

                if self.dac_started: # get statistic for possible underflow indication
                    self.tx_min_available_after_start = (
                        self.tx_available if self.tx_min_available_after_start is None # safe minimum available data
                        else min(self.tx_min_available_after_start, self.tx_available)
                    )

                if self.tx_head_offset >= chunk.size: # current chunk is depleted
                    self.tx_chunks.popleft() # remove chunk (since it has been transmitted)
                    self.tx_head_offset = 0 # set offset back to 0
                self.tx_condition.notify_all() # notify all threads

            if written: # get last written sample for "hold" while underflow
                self.tx_last_sample = int(out[written - 1])

            if written < out.size: # check if buffer is empty (underflow)
                missing = out.size - written # get size of missing data
                out[written:] = self.tx_last_sample if self.tx_underflow_mode == "hold" else 0 # either transmit last sample or zero
                if self.running and self.error is None: # board still running
                    self.tx_runtime_underflow += missing # increase counter for underflow
                else: # board shuts down
                    self.tx_shutdown_padding += missing # no underflow, just shutdown
        return out # returns data to be transmitted
    
    # read available ADC data with blocking
    def read_rx_blocking(
        self,
        out,
        *,
        timeout,
        stop_event=None,
    ):
        """
        Read ADC samples and convert raw streaming values directly to volts.

        STEMlab 125-14:
            LV: +/-1 V full scale
            HV: +/-20 V full scale

        BIT_16 transport:
            physical 14-bit ADC padded to 16-bit -> 32768 counts = +full scale
        Documentation states 8192 counts, but in practice this is off by a factor of 4
        https://redpitaya.readthedocs.io/en/latest/appsFeatures/applications/streaming/reference/technical_details.html#data-formats-and-conversion

        BIT_8 transport:
            signed 8-bit stream -> 128 counts = +full scale
        """
        deadline = time.monotonic() + max( # get deadline before function must be left, since it blocks
            0.0,
            float(timeout),
        )

        with self.rx_condition: # ADC transmitting
            while (self.rx_available == 0 and self.running and self.error is None): # ADC available, board running and no error
                if (stop_event is not None and stop_event.is_set()):
                    return 0 # board has been stopped

                remaining = deadline - time.monotonic() # get remaining time

                if remaining <= 0: # if no time left, exit
                    return 0

                self.rx_condition.wait(timeout=min(remaining, 0.02)) # timeout for max. 20 ms

            self.raise_if_error() # check for errors
            
            # get positive full scale counts for counts to voltage conversion
            if self.adc_bits == 8:
                full_scale_counts = 128.0
            elif self.adc_bits == 16:
                # BIT_16 is the transport format. The STEMlab 125-14
                # itself has a 14-bit ADC. It appears to be that 
                # the 14-bit are shifted upwards to meet the 16-bit
                # transport format. Therefore Division by 2^15
                full_scale_counts = 32768.0
            else:
                raise RuntimeError("ADC bit depth is not configured") # wrong parameter

            if self.adc_voltage_range is None:
                raise RuntimeError("ADC input range is not configured") # wrong parameter

            volts_per_count = (float(self.adc_voltage_range) / full_scale_counts) # convert one count to voltage step

            written = 0 # set counter of collected data

            while (written < len(out) and self.rx_chunks): # data left to get
                chunk = self.rx_chunks[0] # get oldest waiting data from ADC
                remaining = (chunk.size - self.rx_head_offset) # get amount of data that is waiting within this chunk
                count = min(len(out) - written, remaining) # get counter of data that is left

                raw = chunk[self.rx_head_offset:self.rx_head_offset + count] # retrieve raw ADC counts

                out[written:written + count] = (raw.astype(np.float32) * volts_per_count) # convert to volts and store in out

                written += count # increase counter for collected data
                self.rx_head_offset += count # increade head offset within chunk
                self.rx_available -= count # decrease available data counter

                if self.rx_head_offset >= chunk.size: # chunk is fully depleted
                    self.rx_chunks.popleft() # delete chunk
                    self.rx_head_offset = 0 # set offset to zero

            self.rx_condition.notify_all() # update all threads
            return written # return amount of collected data

    def release_adc(self): # release ADC and check if used by other instances
        with self.state_lock:
            self.adc_users = max(0, self.adc_users - 1)
        self._shutdown_if_unused()

    def release_dac(self):  # release ADC and check if used by other instances
        with self.state_lock:
            self.dac_users = max(0, self.dac_users - 1)
        self._shutdown_if_unused()

    def _shutdown_if_unused(self): # if no usage of ADC/DAC > shutdown cleanly
        with self.state_lock:
            unused = self.adc_users == 0 and self.dac_users == 0
        if unused:
            self.shutdown(force=False)

    def shutdown(self, force=False): # clean shutdown
        with self.state_lock: # prevent race condition
            if self.closed: # already closed
                return
            if not force and (self.adc_users > 0 or self.dac_users > 0): # still in use
                return

            self.running = False # no more users, shutting down
            with self.tx_condition: # update all threads
                self.tx_condition.notify_all()
            with self.rx_condition:
                self.rx_condition.notify_all()

            for client in (self.adc, self.dac): # try to stop every client
                if client is not None:
                    try:
                        client.notifyStop()
                    except Exception:
                        pass
            for client in (self.adc, self.dac): # wait for clients to stop
                if client is not None:
                    try:
                        client.wait()
                    except Exception:
                        pass

            if self.adc is not None: # remove ADC callback
                try:
                    self.adc.removeCallback()
                except Exception:
                    pass
            if self.dac is not None: # remove DAC callback
                try:
                    self.dac.removeCallback()
                except Exception:
                    pass
            if self.config is not None: # remove config callback
                try:
                    self.config.removeCallbacks()
                except Exception:
                    pass

            self.closed = True # all callback removed > closed
            roles = []
            if self.adc is not None: # board used as ADC?
                roles.append("ADC")
            if self.dac is not None: # board used as DAC?
                roles.append("DAC")
            
            # print stats with role if board has been used as ADC or DAC or both
            stats = [f"RP[{self.host}] statistics ({'+'.join(roles) or 'none'})"]
            if self.dac is not None:
                stats += [
                    f"TX packets={self.tx_packets}",
                    f"TX runtime underflow={self.tx_runtime_underflow}",
                    f"TX FIFO waits={self.tx_fifo_waits}",
                    f"TX wait time={self.tx_wait_time:.3f}s",
                    f"TX FIFO min={self.tx_min_available_after_start}",
                    f"TX FIFO max={self.tx_max_available}",
                    f"TX FIFO remaining={self.tx_available}",
                ]
            if self.adc is not None:
                stats += [
                    f"RX packets={self.rx_packets}",
                    f"RX FPGA lost={self.rx_fpga_lost}",
                    f"RX host dropped={self.rx_host_dropped}",
                    f"RX FIFO remaining={self.rx_available}",
                ]
            print(", ".join(stats))

        with _INSTANCES_LOCK: # delete board from instance cache
            if _INSTANCES.get(self.host) is self:
                del _INSTANCES[self.host]

