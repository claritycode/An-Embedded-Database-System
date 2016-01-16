import os
import ast
import sys
from SliceDB import SliceDB
from SliceField import SliceField

class SliceEnv :

	def __init__ ( self ) :
		self.tables = dict ()
		if os.path.isfile ( "schema.env" ) :
			schemaFile = open ( "schema.env" )
			for line in schemaFile :
				dataList = line.rstrip ( '\n' ).split ( '|' )
				name = str ( dataList[0] )
				
				schemaRepresentation = ast.literal_eval ( dataList[1] )
				schema = list ()
				
				for colName, colType in schemaRepresentation :
					if colType == "int" :
						schema.append ( SliceField ( colName, SliceField.INT ) ) 
					elif colType == "float" :
						schema.append ( SliceField ( colName, SliceField.DOUBLE ) ) 
					elif colType == "str" :
						schema.append ( SliceField ( colName, SliceField.STRING ) )
	
				try :
					index = int ( dataList[2] )
				except :
					index = ''
				self.tables [ name ] = ( name, schema, index )
			schemaFile.close ()

	
	def close ( self ) :

		try :
			schemaFile = open ( "schema.env", 'w' )
		except IOError :
			raise IOError ( "An IO error occured while trying to open schema.env file for the environment" )

		for key in self.tables :
			schema = self.tables[key] [1] 
			schemaString = list ()
			for value in schema :
				colName, colType = value.field
				if colType == SliceField.INT :
					schemaString.append ( ( colName, "int" ) ) 
				elif colType == SliceField.DOUBLE :
					schemaString.append ( ( colName, "float" ) ) 
				elif colType == SliceField.STRING :
					schemaString.append ( ( colName, "str" ) )
			value = ( self.tables[key][0] , schemaString, self.tables[key][2] ) 
			writeValue = '|'.join ( map ( str, value ) )
			schemaFile.write ( "%s\n" %writeValue )
		schemaFile.close ()

	def create ( self, name, schema, index = None ) :
		if name in self.tables :
			raise AttributeError ( "%s is already defined in the current environment" % name )
		newDB = SliceDB ( name, schema, index )
		self.tables [ name ] = [ name, schema, index ]
		return newDB

	def open ( self, dbName ) :
		if dbName in self.tables :
			name = self.tables[dbName][0]
			schema = self.tables[dbName][1]
			index = self.tables[dbName][2]
			fileName = "%s.slc" % name
			db = SliceDB.initFromFile ( name, schema, index, fileName )
		else :
			raise KeyError ( "There is no Slice Database named %s" % dbName )
		return db