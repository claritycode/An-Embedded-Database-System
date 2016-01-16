from SliceField import SliceField
from SliceRecord import SliceRecord
from SliceQuery import SliceQuery

class SliceDB:

	# Can tableSchema be a sngle SLiceField object instead of a list
	# Can duck typing be used
	def __init__ ( self, tableName, tableSchema, tableIndex = None ) :
		if isinstance ( tableName, ( str, int ) ) :
			self.name = str ( tableName)	
		else :								#raise if not convertable to string
			raise TypeError ( "Table name must be either a string value" )
	
		if isinstance ( tableSchema, list ) :
			if all ( isinstance ( tmp, SliceField ) for tmp in tableSchema ) :
				self.schema = tableSchema
			else :
				raise TypeError ( "All the elements of a list denoting the schema must be instances of SliceField" )
		
		elif isinstance ( tableSchema, SliceField ) :
			self.schema = [ tableSchema ]
		else :
			raise TypeError ( "A schema must be denoted by either an instance of SliceField or a list of instances of SliceField" )

		if not tableIndex :
			self.index = None
			self.counter = 0
		else :
			self.index = None
			for i, tmp in enumerate ( self.schema ) :
				colName, colType = tmp.field
				if colName == tableIndex :
					self.index = i
					break
			if self.index == None:
				raise NameError ( "%s does not represent any column for the table named %s" % ( tableIndex, tableName ) )
		
		self.data = dict ()
		open ( "%s.slc" % tableName, 'a' ).close ()
		# self.commit ()
	
	def createRecord ( self ) :
		return SliceRecord ( self.schema )

	def set ( self, newRecord ) :
		if not isinstance ( newRecord, SliceRecord ) :
			raise TypeError ( "set method only accepts objects of type SliceRecord" )
		if self.index == None:
			self.counter += 1
			self.data [ self.counter ] = newRecord.record
		elif newRecord.record [self.index] == '' :
			raise KeyError ( "%s being the index column for %s cannot have undefined value" % ( self.index, self.name ) )
		else :
			indexName = newRecord.record [self.index]
			self.data [indexName] = newRecord.record

	def get ( self, indexValue ) :
		if self.index == None:
			raise NotImplementedError ( "%s does not have an index column. \
				The Slice system can only get records from tables with predefined index column" % self.name )
		else :
			if indexValue in self.data :
				return SliceRecord ( self.schema, self.data [indexValue] )
			else :
				raise KeyError ( "%s does not have any records with %s equal to %s" % ( self.name, self.index, indexValue ) )

	def commit ( self ) :
		try :
			f = open ( "%s.slc" % self.name , 'w' )
		except IOError :
			raise IOError ( "An IO error occured while trying to open %s.slc database file for table named %s" % self.name )

		for key in self.data :
			value = self.data[key]
			writeValue = '|'.join ( map ( str, value ) )
			f.write ( "%s\n" %writeValue )

		f.close ()

	def load ( self, fileName ) :
		if self.index == None :
			self.counter = 0
		try :
			sourceFile = open ( fileName )
		except IOError :
			raise IOError ( "An IO error occured while trying to open %s bulk load file for table named %s" % ( fileName, self.name ) )

		for line in sourceFile :
			lineList = line.rstrip ( '\n' ).split ( '|' )
			if not len ( lineList ) == len ( self.schema ) :
				raise AttributeError ( "%s is not according to the schema for %s" % ( line, self.name ) )

			newRecord = [''] * len ( self.schema )
			for i, value in enumerate ( lineList ) :
				valueType = self.schema[i].field[1]
				try :
					newRecord[i] = valueType ( value )
				except :
					if value == '' :
						newRecord[i] = ''
					else :
						raise AttributeError ( "%s is of inappropriate type for the record %s" % ( value, line ) )
			if self.index == None :
				self.counter += 1
				self.data [ self.counter ] = newRecord
			
			elif newRecord[self.index] == '' :
				raise AttributeError ( "for record %s, %s being index column cannot have blank value" % ( newRecord, self.schema[self.index].field[0] ) )
			
			elif newRecord[self.index] in self.data :
				raise AttributeError ( "The file consist of duplicate values for index column %s" % self.schema[self.index].field[0] )
			
			else :
				self.data[newRecord[self.index]] = newRecord
		if not fileName == "%s.slc" % self.name :
			self.commit ()
		else :
			sourceFile.close ()

	@staticmethod
	def initFromFile ( tableName, tableSchema, indexColumn, fileName ) :
		name = tableName
		schema = tableSchema
		index = indexColumn
		db = SliceDB ( name, schema, index )
		db.load ( fileName )
		return db

	def join ( self, rhs, joinColumn = None ) :
		
		match = list ()
		rhsJoinSchemaIndex = list ()
		
		if joinColumn == None :		
			for i, rhsSchema in enumerate ( rhs.schema ) :
				for j, lhsSchema in enumerate ( self.schema ) :
					iMatch = False
					if rhsSchema == lhsSchema :
						match.append( ( j, i ) )
						iMatch = True
						break
				if iMatch == False :
					rhsJoinSchemaIndex.append (i)

			if len ( match ) == 0 :
				raise ValueError ( "%s and %s do not have any common column for join operation" % ( self.name, rhs.name ) )
		else :
			lhsCommonColumnIndex = None
			rhsCommonColumnIndex = None
			for i, lhsSchema in enumerate ( self.schema ) :
				if lhsSchema.field[0] == joinColumn :
					lhsCommonColumnIndex = i
					break


			for j, rhsSchema in enumerate ( rhs.schema ) :
				if rhsSchema.field[0] == joinColumn :
					rhsCommonColumnIndex = j
				else :
					rhsJoinSchemaIndex.append (j)
					

			if lhsCommonColumnIndex == None or rhsCommonColumnIndex == None :
				raise ValueError ( "%s is not a common column for the tables %s and %s" % ( joinColumn, self.name, rhs.name ) )
			match.append ( ( lhsCommonColumnIndex, rhsCommonColumnIndex ) )

		joinSchema = list ( self.schema )

		for i in rhsJoinSchemaIndex :
			joinSchema.append ( rhs.schema [i] )

		joinResult = list ()

		for lhsKey in self.data :
			for rhsKey in rhs.data :
				if all ( self.data[lhsKey][i] == rhs.data[rhsKey][j] for i, j in match ) :
					joinData = list ( self.data[lhsKey] )

					for k in rhsJoinSchemaIndex :
						joinData.append ( rhs.data[rhsKey][k])
					print ( joinData )
					joinResult.append ( SliceRecord ( joinSchema, joinData ) )

		return joinResult				


	def delete ( self, indexValue ) :
		if self.index == None:
			raise NotImplementedError ( "%s does not have an index column. \
				The Slice system can only delete records from tables with predefined index column" % self.name )
		else :
			for key in self.data :
				if isinstance ( key, (int,float)) and isinstance (indexValue, str) :
					try :
						indexValue = type (key) ( indexValue )
						break
					except :
						raise TypeError ( "You have specified a value of inappropriate type for index column" )
			if indexValue in self.data :
				del self.data[indexValue]
				return True
			else :
				return False
		
	def query ( self, newQuery ) :
		if not newQuery.targetDB == self.name :
			raise AttributeError ( "%s cannot run queries on %s database" % ( self.name, newQuery.targetDB ) )
		
		resultColumnsIndex = list ()

		for rCol in newQuery.resultColumn :
			lenResultColumnsIndex = len ( resultColumnsIndex )
			for i, column in enumerate ( self.schema ) :
				colName, colType = column.field
				if colName == rCol :
					resultColumnsIndex.append ( i )
					break
			if lenResultColumnsIndex == len ( resultColumnsIndex ) :
				raise AttributeError ( "%s is not a column of %s" % ( rCol, self.name ) )


		operatingColumnIndex = None

		for i, column in enumerate ( self.schema ) :
			colName, colType = column.field
			if colName == newQuery.condition.operatingColumn :
				if not isinstance ( newQuery.condition.literalValue, colType ) :
					if colType == SliceField.INT or colType == SliceField.DOUBLE :
						try :
							newQuery.condition.literalValue = colType ( newQuery.condition.literalValue )
							operatingColumnIndex = i
							break
						except :
							raise AttributeError ( "%s does not support values of type %s" % ( colName, type ( newQuery.condition.literalValue ) ) )
					else :
						raise AttributeError ( "%s does not support values of type %s" % ( colName, type ( newQuery.condition.literalValue ) ) )
				else :
					operatingColumnIndex = i
					break

		if operatingColumnIndex == None :
			raise AttributeError ( "%s does not have a column named %s" % ( self.name, newQuery.condition.operatingColumn ) )

		resultSchema = list ()

		for index in resultColumnsIndex :
			resultSchema.append ( self.schema[index] )

		rhsValue = newQuery.condition.literalValue
		operator = newQuery.condition.operator
		queryResult = list()
		
		for key in self.data :
			lhsValue = self.data[key][operatingColumnIndex]
			if operator ( lhsValue, rhsValue ) :
				resultData = list ()
				for index in resultColumnsIndex :
					resultData.append ( self.data[key][index] )
				queryResult.append ( SliceRecord ( resultSchema, resultData ) )

		return queryResult