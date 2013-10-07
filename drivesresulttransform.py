#!/usr/bin/python

import sys

currentmaxplay = None
currentplaytype = None

# Iterate through every line passed in to stdin
for input in sys.stdin.readlines():
	# Plays come in reverse chronological order to get the result first
	game, drive, play, playtype, quarter, playid = input.strip().split("\t")
	
	if currentmaxplay is None:
		# We want to ignore KICKOFF as the result of a drive
		if playtype == "KICKOFF" and int(play) > 1:
			# Wait for the next result because that will be the real drive result
			currentmaxplay = None
			currentplaytype = None
			print playtype + "\t" + play + "\t" + playid
			continue
		else:
			# Use the result
			currentmaxplay = play
			currentplaytype = playtype
	
	# Printing more fields than normal to work around Hive's transform limitation
	print currentplaytype + "\t" + currentmaxplay + "\t" + playid
	
	# That's the last play in the drive, prepare for the next drive
	if int(play) == 1:
		currentmaxplay = None
		currentplaytype = None
