from SliceCondition import SliceCondition

class SliceQuery :

	def __init__ ( self, colName, dbName, cond ) :
		if isinstance ( colName, list ) :
			if not all ( isinstance ( tmp, str ) for tmp in colName ) :
				raise TypeError ( "Column Name is not a string or a list of string values")
			else :
				self.resultColumn = colName

		elif not isinstance ( colName, str ) :
			raise TypeError ( "Column Name is not a string or a list of string values" )

		else :
			self.resultColumn = [ colName ]

		if not isinstance ( dbName, str ) :
			raise TypeError ( "Database Name is not a string value" )
		else :
			self.targetDB = dbName

		if not isinstance ( cond, SliceCondition ) :
			raise TypeError ( "Condition to a query is not an object of type SliceCondition" )
		else :
			self.condition = cond