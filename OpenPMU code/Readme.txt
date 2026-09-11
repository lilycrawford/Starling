WaveMU
-------------

This code will play back a FLAC file and pretend it is the OpenPMU ADC.


ADCtoMQTT
-------------

This code listens for a physical OpenPMU ADC...  or the output of WaveMU.  It can't tell the difference.


SVtoWave
-------------
This is actually the programme that records the FLAC files.  However, I started to modify the handler for the sampled value data "mqttSVsub" so that it works with JSON.


