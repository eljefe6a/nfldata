#!/usr/bin/python

import sys

currentmaxplay = None
currentplaytype = None

# Iterate through every line passed in to stdin
for input in sys.stdin.readlines():
	# Plays come in reverse chronological order to get the result first
	game, drive, play, playtype = input.strip().split("\t")
	
	if currentmaxplay is None:
		currentmaxplay = play
		currentplaytype = playtype
	
	# Printing more fields than normal to work around Hive's transform limitation
	print currentplaytype + "\t" + currentmaxplay + "\t" + game + "\t" + drive + "\t" + play
	
	# That's the last play in the drive, prepare for the next drive
	if int(play) == 1:
		currentmaxplay = None
		currentplaytype = None
