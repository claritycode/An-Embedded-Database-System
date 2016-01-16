import operator

class SliceCondition :

	EQ = operator.eq
	LT = operator.lt
	GT = operator.gt

	def __init__ ( self, colName, op, value ) :
		self.operatingColumn = colName
		self.literalValue = value
		if op == SliceCondition.EQ or op == SliceCondition.LT or op == SliceCondition.GT :
			self.operator = op
		else:
			raise TypeError ( "You have specified an inappropriate operation for SLice Query condition" )