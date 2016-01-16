from SliceField import SliceField

class SliceRecord:

	def __init__ ( self, sch, data = None ) :
		self.recordSchema = sch 	# It is a list of SliceField Objects
		#If the user have not specified the record, create a default initialized record satisfying the schema
		if not data :
			# self.record is a list that stores a single record of a Slice Database Table
			self.record = [''] * len ( self.recordSchema )		# Every record statisfies the schema 
		else :
			self.record = data

	def set ( self, colName, colValue ) :

		for i, tmp in enumerate(self.recordSchema) :
			schemaColName, schemaColType = tmp.field
			if colName == schemaColName :
				if isinstance ( colValue, schemaColType ) :		
					self.record [i] = colValue
					return
				elif schemaColType == SliceField.INT or schemaColType == SliceField.DOUBLE :
					try :
						self.record[i] = schemaColType ( colValue )
						return
					except :
						raise TypeError ( "%s column can only store elements of type %s" % ( schemaColName, schemaColType ) )
				else :
					raise TypeError ( "%s column can only store elements of type %s" % ( schemaColName, schemaColType ) )

		raise AttributeError ( "%s is not a valid column name" % colName )

	def get ( self, colName ) :

		for i, tmp in enumerate ( self.recordSchema ) :
			schemaColName, schemaColType = tmp.field
			if schemaColName == colName :
				return self.record [i]

		raise AttributeError ( "%s is not a valid column name" % colName )