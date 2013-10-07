#!/usr/bin/python

import sys

currentgame = None
currentoffense = None
currentquarter = 1

currentdrive = 1
currentplay = 0

# Iterate through every line passed in to stdin
for input in sys.stdin.readlines():
	# Plays come in chronological order
	game, quarter, offense, gameminutes, gameseconds, playid = input.strip().split("\t")
	
	if currentgame is None:
		currentgame = game
		
	if currentoffense is None:
		currentoffense = offense
	
	# Game changed
	if game != currentgame:
		currentgame = game
		currentdrive = 1
		currentplay = 0
	
	if quarter != currentquarter:
		# Half changed
		if currentquarter == 3 or currentquarter >= 5:
			currentdrive += 1
			currentplay = 0
		# Change of possession at beginning of quarter
		elif currentoffense != offense:
			currentdrive += 1
			currentoffense = offense
			currentplay = 0
	# Change of possession
	elif currentoffense != offense:
		currentdrive += 1
		currentoffense = offense
		currentplay = 0
		
	currentplay += 1
	currentquarter = quarter
	
	# Printing more fields than normal to work around Hive's transform limitation
	print str(currentdrive) + "\t" + str(currentplay) + "\t" + playid
