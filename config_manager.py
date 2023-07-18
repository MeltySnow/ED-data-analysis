def ConfigGen() -> None:
	with open("ed_data_analysis.conf", 'w', encoding="utf-8") as Writer:
		Writer.write("""\
#Lines beginning in a \'#\' will be ignored. Empty strings will be ignored.
#dashboard:
#output: out.html
#exclude: False"""
	       )

#Dependency for LoadConfig
def StripWhitespace(ip: str) -> str:
	whitespace: str = " \t\n"
	op: str = ""
	for n in range(0, len(ip)):
		if not (ip[n] in whitespace):
			op += ip[n]
	return op

#Dependency for LoadConfig
#Returns the index of the first colon in a string
def FirstColonIndex(ip: str) -> int:
	for n in range(0, len(ip)):
		if ip[n] == ':':
			return n
	return 0

def LoadConfig(config: dict) -> None:
	#I THINK Python dictionaries get passed by reference so I should be able to modify it within this function
	#Open config file in read mode
	Reader: _io.TextIOWrapper = open(config["config"], "r", encoding="utf=8")

	#Initialise constants for loop:
	line: str = Reader.readline()

	while line:
		line = StripWhitespace(line)
		#So that hashtags can be used to denote comments in the config file
		if line[0] != '#':
			colonIndex: int = FirstColonIndex(line)
			key: str = line[0 : colonIndex]
			val: str = line[colonIndex + 1 :]

			if (not config[key]) and val:#First evaluation checks if the key has already been set (as command line arguments should override the config file). Second checks that the value in the config file exists and isn't a null string
				if key == "exclude":
					#Convert argument from string to boolean
					config[key] = val.lower() != "false"
				else:
					config[key] = val

		line = Reader.readline()

	Reader.close()
