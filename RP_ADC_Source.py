#
#
# @file        RP ADC Source GNU Radio Custom Python Block
# @description Serves as a source, that streams directly from
#              a Red Pitaya Board, using the Streaming API.
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

import sys
import threading
import numpy as np
from gnuradio import gr

import rp_shared_dual # custom backend!


class blk(gr.sync_block):
    """Stream incoming data to a Red Pitaya Board, like the STEMlab125-14 Pro Gen 2
       using the Streaming API from Red Pitaya. For higher data rates, it is advised
       to disable the Red Pitayas Web Server.
       https://redpitaya.readthedocs.io/en/latest/appsFeatures/applications/streaming/reference/performance_optimization.html#quick-start-disable-web-interface

    Output:  continous data stream as float

    Parameters:
        Host: The IP Adress of the Red Pitaya Board as a string.
              example: "192.168.178.1" (with quotationmarks!)
        Adc_Decimation: Sets the decimation factor to achieve the
                  desired sample rate as an integer divisible of 125 MS/s.
                  decimation = (125 MS/s) / (desired sample rate)
        Adc_Channel: Choose the used ADC channel. Either 1 or 2 for the
                  STEMlab125-14 Pro Gen 2.
        Adc_Bits: Set the transport resolution of the Adc to "8" bit or 
                  "16" bit. This does not increase the 14-bit ADC
                  resolution, its just the transport size.
                  Less resolution = more troughput.
        Input_Range: Set the Red Pitayas input range according to the
                  applied signal range and onboard jumpers!
                  Set to "LV" for low voltage (-1 ... +1 V) or
                  "HV" for high voltage (-20 ... +20 V) mode.
                  (with quotationmarks!)
        Block_Size: Payload size in bytes per TCP/IP message.
                  Must be within 2 kB to 8 MB.
        Adc_Size: The amount of memory the ADC gets reserved onboard
                  of the Red Pitaya. The Memory is shared with the DAC
                  and GPIO! The maximum memory for everything is 32 MB.
                  Default is 769,5 kB.
        Rx_Fifo_Samples: Custom Fifo buffer of the PC-side streaming
                  interface. A value 2-times the Block_Size seems to work well.
        Read_Timeout: Duration in ms, after no incoming data is treated as a
                  disconnection/time out.
        Verbose:  Set to True to get live logs. Set to False to keep it quiet.
    """
    def __init__(
        self,
        host="192.168.178.201",
        adc_decimation=100, # set ADC decimation factor, can be 1,2,4,8,16 or 17-65536
        adc_channel=1, # set channel 1 or 2
        adc_bits=16, # transmission size, either 8 (1 byte) or 16 (2 byte)
        input_range="HV", # set input range, must match onboard jumpers!
        block_size=65536, # block size for packet transmission from 2 kB to 8 MB, default 64 kB (65536)
        adc_size=4194304, # memory reserved for DAC, default 769,5 kB, maximum 32 MB for DAC & ADC combined!, default 2 MB
        rx_fifo_samples=2500000, # custom buffer for incoming data
        read_timeout=0.05, # duration in ms, before no data is treated as a timeout
        verbose=False, # error logging from streaming library, default: False
    ):
        gr.sync_block.__init__(
            self,
            name="RP ADC Source", # block name
            in_sig=None, # no input
            out_sig=[np.float32], # output as float
        )

        self.adc_bits = int(adc_bits)
        if self.adc_bits not in (8, 16):
            raise ValueError("adc_bits must be 8 or 16")

        self.input_range = str(input_range).strip().upper()
        if self.input_range not in ("LV", "HV"):
            raise ValueError("input_range must be 'LV' or 'HV'")

        self.adc_decimation = int(adc_decimation)
        self.adc_channel = int(adc_channel)
        self.adc_size = int(adc_size)
        self.rx_fifo_samples = int(rx_fifo_samples)
        self.read_timeout = float(read_timeout)

        self.stop_event = threading.Event()
        self.acquired = False

        self.board = rp_shared_dual.get_board(
            host=host, # create board instance from custom backend
            block_size=int(block_size),
            verbose=verbose,
        )

    def start(self): # startup, get all parameters
        self.stop_event.clear()
        self.board.acquire_adc(
            adc_decimation=self.adc_decimation,
            adc_channel=self.adc_channel,
            adc_bits=self.adc_bits,
            input_range=self.input_range,
            adc_size=self.adc_size,
            rx_fifo_samples=self.rx_fifo_samples,
        )
        self.acquired = True
        return True

    def stop(self): # stop an ongoing connection
        self.stop_event.set()
        with self.board.rx_condition:
            self.board.rx_condition.notify_all()
        if self.acquired:
            self.board.release_adc()
            self.acquired = False
        return True

    def work(self, input_items, output_items):
        return self.board.read_rx_blocking(
            output_items[0],
            timeout=self.read_timeout,
            stop_event=self.stop_event,
        )

