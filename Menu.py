from SliceField import SliceField
from SliceEnv import SliceEnv
from SliceCondition import SliceCondition
from SliceQuery import SliceQuery

class Menu :

	def __init__ ( self ) :

		self.methodList = ( self.createDatabase, self.addRecord, self.deleteRecord, 
			self.bulkLoad, self.join, self.runQuery, self.report1, self.report2 )
		self.env = SliceEnv ()

	def getDatabaseServices ( self ) :

		choice = self.getMenu ()
		while choice != 9 :

			self.methodList [choice - 1] ()
			i = input ( "Press any key to continue" )
			choice = self.getMenu ()

	def createDatabase ( self ) :

		while True :
			try :
				tableName = input ( "Enter Table Name: " )
				while True :
					fieldCount = input ( "Enter the number of columns: " )
					try :
						fieldCount = int ( fieldCount )
						if fieldCount < 1 :
							raise ValueError
						else :
							break
					except ValueError as e :
						print ( "You have entered a wrong value for number of columns" )
						print ( "Please enter a correct value" )

				schema = list ()

				while fieldCount != 0 :
						while True :
							try :
								columnName = input ( "Enter Column Name: " )
								while True :
									columnType = input ( "Enter the type for %s column: " % columnName )
									if columnType == "STRING" :
										schema.append ( SliceField ( columnName, SliceField.STRING ) )
										break
									elif columnType == "INT" :
										schema.append ( SliceField ( columnName, SliceField.INT ) )
										break
									elif columnType == "FLOAT" :
										schema.append ( SliceField ( columnName, SliceField.DOUBLE ) )
										break
									else :
										print ( "You have Entered an incorrect type for %s column" % columnName )
										print ( "Please try again" )
								break
							except Exception as e:
								print ( "The following error occurred: %s" %e )
								print ( "Please try again" )


						fieldCount = fieldCount - 1

				indexColumn = input ( "Enter an index column ( or press enter key to skip it) : " )
				db = self.env.create ( tableName, schema, indexColumn )
				self.env.close ()
				break

			except Exception as e:
				print ( "The following error occurred: %s" %e )
				print ( "Please try again" )
		print ( "%s database has been created" % tableName )

	def addRecord ( self ) :

		while True :
			try :
				dbName = input ( "Enter the name of the database to whom you would like to add record: " )
				db = self.env.open ( dbName )
				break
			except Exception as e :
				print ( "The following error occurred: %s" %e )
				print ( "Please try again" )

		newRecord = db.createRecord ()

		#Catch exception
		while True :
			try :
				for column in newRecord.recordSchema :
					columnName = column.field[0]
					while True :
						try :
							newData = input ( "Enter a value for %s: " % columnName )
							newRecord.set ( columnName, newData )
							break
						except Exception as e :
							print ( "The following error occurred: %s" %e )
							print ( "Please try again" )

				db.set ( newRecord )
				break
			except Exception as e :
				print ( "The following error occurred: %s" %e )
				print ( "Please try again" )

		db.commit ()
		print ( "A new record have been added to the database named %s" % dbName )

	def deleteRecord ( self ) :

		while True :
			try :
				dbName = input ( "Enter the name of the database from whom you would like to delete record: " )
				db = self.env.open ( dbName )
				break
			except Exception as e :
				print ( "The following error occurred: %s" %e )
				print ( "Please try again" )

		if db.index == None :
			print ( "%s do not have an index column. Hence, you are not permitted to delete a record from %s" % (dbName,dbName) )
		else :
			indexColumnName = db.schema[ db.index ][0] 
			key = input ( "Enter a value for %s to delete the corresponding record: " % indexColumnName )
			try :
				result = db.delete ( key )
			except Exception as e :
				print ( "The following error occurred: %s" %e )
				return

			if result == True :
				db.commit ()
				print ( "The record with %s having a value of %s has been deleted from %s" % ( indexColumnName, key, dbName ) )
			else :
				print ( "No record with %s having a value of %s was found in %s" % ( indexColumnName, key, dbName ) )

	def bulkLoad ( self ) :

		while True :
			try :
				dbName = input ( "Enter the name of the database into whom you would like to load the data: " )
				db = self.env.open ( dbName )
				break
			except Exception as e :
				print ( "The following error occurred: %s" %e )
				print ( "Please try again" )

		while True :
			try :
				fileName = input ( "Enter the name of the bulk load file: " )
				db.load ( fileName )
				break
			except Exception as e :
				print ( "The following error occurred relating to your bulk load file: %s" %e )
				print ( "Please try again" )

		print ( "The data has been loaded from %s into %s database" % ( fileName, dbName ) )

	def join ( self ) :

		while True :
			try :
				lhsName = input ( "Enter first database name: " )
				lhs = self.env.open ( lhsName )
				break
			except Exception as e :
				print ( "The following error occurred: %s" %e )
				print ( "Please try again" )

		while True :
			try :
				rhsName = input ( "Enter second database name: " )
				rhs = self.env.open ( rhsName )
				break
			except Exception as e :
				print ( "The following error occurred: %s" %e )
				print ( "Please try again" )

		userInput = input ( "Enter the join column (press enter to perform default join): " )
		try :
			if userInput == '' :  
				joinResult = lhs.join ( rhs )
			else :
				joinResult = lhs.join ( rhs, userInput )
		except Exception as e:
			print ( "The following error occurred: %s" %e )
			return
		print ( "Join result is as follows" )
		self.printRecords ( joinResult )

	def runQuery ( self ) :
		while True :
			try :
				dbName = input ( "Enter the name of the database on which you would like to perform a query: " )
				db = self.env.open ( dbName )
				break
			except Exception as e :
				print ( "The following error occurred: %s" %e )
				print ( "Please try again" )

		resultColumn = list ()
		while True :
			value = input ( "Enter the name of a column to display" )

			if value != '' :
				break
			print ( "The result of the query must have atleast one column" )

		while value != '' :
			resultColumn.append ( value )
			value = input ( "Enter the name of a column to display (or press enter key if you have entered all the needed columns): " )

		operatingColumn = input ( "Enter the name the column to be used for conditional checking records: " )
		operator = input ( "Enter the operation you would like to perform: " )
		while True :
			if operator == "EQ" :
				operator = SliceCondition.EQ
				break
			elif operator == "LT" :
				operator = SliceCondition.LT
				break
			elif operator == "GT" :
				operator = SliceCondition.GT
				break
			else :
				print ( "You have entered a wrong operation" )
				print ( "Please enter one among EQ, LT and GT operation" )
				operator = input ( "Enter the operation you would like to perform: " )

		literalValue = input ( "Enter the value you would like compare the %s values against: " % operatingColumn )
		condition = SliceCondition ( operatingColumn, operator, literalValue )
		query = SliceQuery ( resultColumn, dbName, condition )
		try :
			queryResult = db.query ( query )
		except Exception as e:
			print ( "Your query failed due to the following error: %s" %e )	
			return

		self.printRecords ( queryResult )		

	def printRecords ( self, re ) :

		schema = re[0].recordSchema
		widthList = list()

		for value in schema :
			colName, colType = value.field
			if colType == SliceField.INT or colType == SliceField.DOUBLE :
				widthList.append (7)
				print ( "{0:<7}".format ( colName ), end = '' )
			else :
				widthList.append (35)
				print ( "{0:<35}".format ( colName ), end = '' )
		print ()
		print ( "{0:-<{width}}".format ( '-', width = sum (widthList) ) )

		for value in re :
			currentRecord = value.record
			for i, element in enumerate ( currentRecord ) :
				print ( "{0:<{width}}".format ( element, width = widthList[i] ), end = '' )
			print ()

	def report1 ( self ) :
		pass

	def report2 ( self ) :
		pass

	def getMenu ( self ) :

		print ()
		print ( "Slice database testing menu" )
		print ( "1. Create Database" )
		print ( "2. Add Record" )
		print ( "3. Delete Record" )
		print ( "4. Bulk Load" )
		print ( "5. Natural Join" )
		print ( "6. Run Query" )
		print ( "7. Report 1" )
		print ( "8. Report 2" )
		print ( "9. Exit" )
		print ()
		while True :
			choice = input ( "Enter your choice: ")
			try :
				choice = int ( choice )
				if choice < 1 or choice > 9 :
					raise ValueError 
				break
			except ValueError as e :
				print ( "You have entered a wrong choice" )
				print ( "Please try again" )
		print ()

		return choice

if __name__ == "__main__" :
	obj = Menu ()
	obj.getDatabaseServices ()