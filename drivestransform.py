#!/usr/bin/python

import sys

currentgame = None
currentquarter = 1

currentdrive = 1
currentplay = 0

# Iterate through every line passed in to stdin
for input in sys.stdin.readlines():
	# Plays come in chronological order
	game, quarter, offense, gameminutes, gameseconds = input.strip().split("\t")
	
	if currentgame is None:
		currentgame = game
	
	# Game changed
	if game != currentgame:
		currentgame = game
		currentdrive = 1
		currentplay = 0

	# Half changed
	if quarter != currentquarter:
		if currentquarter == 3 or currentquarter >= 5:
			currentdrive = 1
			currentplay = 0
			
	currentplay += 1
	currentquarter = quarter
	
	# Printing more fields than normal to work around Hive's transform limitation
	print str(currentdrive) + "\t" + str(currentplay) + "\t" + game + "\t" + quarter + "\t" + gameminutes + "\t" + gameseconds
