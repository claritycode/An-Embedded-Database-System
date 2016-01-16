class SliceField :

	INT = int
	STRING = str
	DOUBLE = float

	def __init__ ( self, colName, colType ) :
		if isinstance ( colName, ( str, int ) ) :
			if colType == SliceField.INT or colType == SliceField.STRING or colType == SliceField.DOUBLE :
				self.field = ( colName, colType )					
			else :
				raise TypeError ( "%s column has an invalid type" %colName )
		else :
			raise TypeError ( "%s is not a valid column name" % colName )
		
	def __eq__ ( self, rhs ) :
		if ( isinstance ( rhs, self.__class__ ) ) :
			return self.__dict__ == rhs.__dict__
		else :
			return False