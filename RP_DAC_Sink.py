#
#
# @file        RP DAC Sink GNU Radio Custom Python Block
# @description Serves as a sink, that streams directly to
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

# READ CAREFULLY !!!
# WARNING: Changing the following to variable can result in excessive laser radiation!
# Change those values only in a controlled matter!
MAX_POS_AMPLITUDE = 0.1 # established maximum positive amplitude at given laser operating point to limit emitted power to a certain level
MAX_NEG_AMPLITUDE = -0.15 # established maximum negative amplitude at given laser operating point to limit emitted power to a certain level
# END of warning
# READ CAREFULLY !!!

class blk(gr.sync_block):
    """Stream incoming data to a Red Pitaya Board, like the STEMlab125-14 Pro Gen 2
       using the Streaming API from Red Pitaya. For higher data rates, it is advised
       to disable the Red Pitayas Web Server.
       https://redpitaya.readthedocs.io/en/latest/appsFeatures/applications/streaming/reference/performance_optimization.html#quick-start-disable-web-interface

    Input:  continous data stream as float

    Parameters:
        Host: The IP Adress of the Red Pitaya Board as a string.
              example: "192.168.178.1" (with quotationmarks!)
        Dac_Rate: Desired sample rate of the DAC, must be an integer
                  divisible of 125 MS/s.
        Dac_Channel: Choose the used DAC channel. Either 1 or 2 for the
                  STEMlab125-14 Pro Gen 2.
        Dac_Bits: Set the transport resolution of the DAC to "8" bit or 
                  "16" bit. This does not increase the 14-bit DAC
                  resolution, its just the transport size.
                  Less resolution = more troughput.
        Block_Size: Payload size in bytes per TCP/IP message.
                  Must be within 2 kB to 8 MB.
        Dac_Size: The amount of memory the DAC gets reserved onboard
                  of the Red Pitaya. The Memory is shared with the ADC
                  and GPIO! The maximum memory for everything is 32 MB.
                  Default is 769,5 kB.
        Tx_Fifo_Samples: Custom Fifo buffer of the PC-side streaming
                  interface. A value 2-times the Block_Size seems to work well.
        Tx_Prefill_Samples: The amount of samples that are prefilled at
                  startup, to bridge the first ms of transmission.
                  Can be left at 0, a value of the Block_Size is advised.
        Tx_Underflow_Mode: Sets the reaction of the client, if not enough data
                  is passed. It can either "hold" the last sample, unitl new
                  ones arive or send out "zero". (with quotationmarks!)
        Verbose:  Set to True to get live logs. Set to False to keep it quiet.
    """
    def __init__( # initialize DAC with default values
        self,
        host="192.168.178.200",
        dac_rate=1250000, # set DAC sample rate, maximum 125 MS/s for STEMlab125-14 Pro Gen 2
        dac_channel=1, # set channel 1 or 2
        dac_bits=16, # transmission size, either 8 (1 byte) or 16 (2 byte)
        block_size=65536, # block size for packet transmission from 2 kB to 8 MB, default 64 kB (65536)
        dac_size=2097152, # memory reserved for DAC, default 769,5 kB, maximum 32 MB for DAC & ADC combined!, default 2 MB
        tx_fifo_samples=6000000, # custom buffer for outgoing data
        tx_prefill_samples=2500000, # samples to be transmitted at startup to prefill all buffers
        tx_underflow_mode="hold", # reaction to data underflow, either hold or zero
        verbose=False, # error logging from streaming library, default: False
    ):
        gr.sync_block.__init__( # Define GNU-Radio block, float input, no output
            self,
            name="RP DAC Sink", # block name
            in_sig=[np.float32], # input as float
            out_sig=None, # no output
        )

        self.dac_bits = int(dac_bits) # check transmission resolution 1 byte or 2 byte
        if self.dac_bits not in (8, 16):
            raise ValueError("dac_bits must be 8 or 16")

        # normalization for all variables
        self.dac_rate = int(dac_rate)
        self.dac_channel = int(dac_channel)
        self.dac_size = int(dac_size)
        self.tx_fifo_samples = int(tx_fifo_samples)
        self.tx_prefill_samples = int(tx_prefill_samples)
        self.tx_underflow_mode = str(tx_underflow_mode)

        self.stop_event = threading.Event()
        self.acquired = False

        self.board = rp_shared_dual.get_board(
            host=host, # create board instance via rp_shared_dual backend
            block_size=int(block_size),
            verbose=verbose,
        )

    def start(self): # startup, get all parameters
        self.stop_event.clear()
        self.board.acquire_dac(
            dac_rate=self.dac_rate,
            dac_channel=self.dac_channel,
            dac_bits=self.dac_bits,
            dac_size=self.dac_size,
            tx_fifo_samples=self.tx_fifo_samples,
            tx_prefill_samples=self.tx_prefill_samples,
            tx_underflow_mode=self.tx_underflow_mode,
        )
        self.acquired = True
        return True

    def stop(self): # stop an ongoing connection
        self.stop_event.set()
        with self.board.tx_condition:
            self.board.tx_condition.notify_all()
        if self.acquired:
            self.board.release_dac()
            self.acquired = False
        return True

    def work(self, input_items, output_items):
        x = np.asarray(input_items[0], dtype=np.float32)
        if x.size == 0:
            return 0
        
        # Overflowprotection: reduce the upper limit by the smallest float increment
        # important, if MAX_POS_AMPLITUDE = 1, to prevent conversion error
        # if +1.0 is multiplied with 2^15 for signed integer conversion -> an overflow occurs
        pos_scale = np.nextafter(np.float32(MAX_POS_AMPLITUDE), np.float32(0.0))
        neg_scale = np.abs(MAX_NEG_AMPLITUDE) # factor must be positive, since x is already negative

        # asymmetric scaling of x around the operating point
        # if x >= 0, multiply by pos_scale. if x < 0, multiply by neg_scale.
        x = np.where(x >= 0, x * pos_scale, x * neg_scale)

        if self.dac_bits == 8: # set factor for transmission > 1 byte
            samples = np.rint(x * 127.0).astype(np.int8)
        else: # set factor for transmission > 2 byte
            samples = np.rint(x * 32767).astype(np.int16)

        return self.board.enqueue_tx_blocking(
            samples,
            stop_event=self.stop_event,
        )

