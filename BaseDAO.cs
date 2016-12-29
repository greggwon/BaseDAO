using Microsoft.Practices.EnterpriseLibrary.Data;
using NetLog.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Web;

using MySql.Data;
using MySql.Data.MySqlClient;
using System.Configuration;
using System.Reflection;
using Microsoft.Practices.EnterpriseLibrary.Data.Configuration;
using Microsoft.Practices.EnterpriseLibrary.Data.Sql;
using System.Text.RegularExpressions;
using System.Collections.Concurrent;

namespace org.wonderly.model.DAO {
	/// <summary>
	/// The functions in this base class, provide convenience methods for access to database access and operation
	/// on the associated data.  There are methods with Transaction<typeparamref name="T"/>, Operation<typeparamref name="T"/> and Action<typeparamref name="T"/>
	/// signature.
	/// </summary>
	public class BaseDAO {
		private static Logger log = Logger.GetLogger(typeof(BaseDAO).FullName);
		public delegate T DelDataSetOperation<T>( DataRow row );
		public delegate T DelDataSetOperationIndexed<T>( DataRow row, int idx, int cnt );
		public delegate T DelDatabaseTransaction<T>( Database db, DbTransaction trans );
		public delegate T DelDatabaseTask<T>( Database db );
		public delegate T DelScalarAction<T>( T val );

		public delegate void DelDataSetAction( DataRow row );
		public delegate void DelDataSetActionIndexed( DataRow row, int idx, int cnt );
		public delegate void DelDatabaseActionTransaction( Database db, DbTransaction trans );
		public delegate void DelDatabaseActionTask( Database db );
		public delegate void DelDatabaseOptionTask( Database db, DbCommand dbCommand );

		/// <summary>
		/// Get int value for column in data row
		/// </summary>
		/// <param name="row"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public static int intValue( DataRow row, string columnName ) {
			return (int)Convert.ChangeType( row[ columnName ], typeof( int ) );
		}

		/// <summary>
		/// Get long value for column in data row
		/// </summary>
		/// <param name="row"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public static long longValue( DataRow row, string columnName ) {
			return (long)Convert.ChangeType( row[ columnName ], typeof( long ) );
		}

		/// <summary>
		/// Get float value for column in data row
		/// </summary>
		/// <param name="row"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public static float floatValue( DataRow row, string columnName ) {
			return (float)Convert.ChangeType( row[ columnName ], typeof( float ) );
		}

		/// <summary>
		/// Get double value for column in data row
		/// </summary>
		/// <param name="row"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public static double doubleValue( DataRow row, string columnName ) {
			return (double)Convert.ChangeType( row[ columnName ], typeof( double ) );
		}

		/// <summary>
		/// Get bool value for column in data row
		/// </summary>
		/// <param name="row"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public static bool boolValue( DataRow row, string columnName ) {
			return (bool)Convert.ChangeType( row[ columnName ], typeof( bool ) );
		}

		/// <summary>
		/// Get DateTime value for column in data row
		/// </summary>
		/// <param name="row"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public static DateTime DateTimeValue( DataRow row, string columnName ) {
			return (DateTime)Convert.ChangeType( row[ columnName ], typeof( DateTime ) );
		}

		/// <summary>
		/// Get string value for column in data row
		/// </summary>
		/// <param name="row"></param>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public static string stringValue( DataRow row, string columnName ) {
			return row[ columnName ].ToString();
		}

		/// <summary>
		/// Get int value for the passed value
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static int intValue( object value ) {
			return (int)Convert.ChangeType( value, typeof( int ) );
		}

		/// <summary>
		/// Get long value for the passed value
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static long longValue( object value ) {
			return (long)Convert.ChangeType( value, typeof( long ) );
		}

		/// <summary>
		/// Get float value for the passed value
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static float floatValue( object value ) {
			return (float)Convert.ChangeType( value, typeof( float ) );
		}

		/// <summary>
		/// Get double value for the passed value
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static double doubleValue( object value ) {
			return (double)Convert.ChangeType( value, typeof( double ) );
		}

		/// <summary>
		/// Get bool value for the passed value
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static bool boolValue( object value ) {
			return (bool)Convert.ChangeType( value, typeof( bool ) );
		}

		/// <summary>
		/// A shortened function to construct a KeyValuePair instance.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="val"></param>
		/// <returns></returns>
		public static KeyValuePair<string,object> KV( string key, object val, bool validate = true ) {
			//if (validate && val != null && val.ToString().Length > 2)
			//{
				//    val = regexString(val.ToString());
			//}
			return new KeyValuePair<string,object>( key, val );
		}

		public static string regexString( string inputString ) {
			string outputString = inputString;
			Regex tagRegex = new Regex( @"<[^>]+>" );
			while( tagRegex.IsMatch( outputString ) ) {
				int index1 = outputString.IndexOf( '<' );
				int index2 = outputString.IndexOf( '>' );
				string filterString = outputString.Substring( index1, index2 - index1 + 1 );
				outputString = outputString.Replace( filterString, "" );
			}
			return outputString;
		}

		/// <summary>
		/// Creates a foreign key index on the passed parameters
		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="fromTable">The table the reference is from</param>
		/// <param name="toTable">The table the references is to</param>
		/// <param name="fromKeyFieldName">The column holding the referenced value in the from table</param>
		/// <param name="toKeyFieldName">The column holding the refererred to key in the to table</param>
		public static void AddForeignKeyFromToOnKey( Database db, DbTransaction trans, string fromTable, string toTable, string fromKeyFieldName, string toKeyFieldName ) {
			Update(db, trans, @"
				ALTER TABLE `"+fromTable+@"`
				ADD FOREIGN KEY FK_"+fromKeyFieldName+"_"+fromTable+"_"+toTable + " ("+fromKeyFieldName+") references `"+toTable+"` ("+toKeyFieldName+@") 
			");
		}

		/// <summary>
		/// The types of index keys to use in CreateNewIndexIfNotExists
		/// </summary>
		public enum IndexType { INDEX_UNIQUE, INDEX_KEY };

		/// <summary>
		/// Create an index with the passed parameters if it does not already exist.  If it
		/// does exist, do nothing.
		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="table"></param>
		/// <param name="type"></param>
		/// <param name="keys"></param>
		/// <returns>true if the index was created, false if it already existed</returns>
		public static bool CreateNewIndexIfNotExists( Database db, DbTransaction trans, string table, IndexType type, params string[] keys ) {

			string typestr = type == IndexType.INDEX_UNIQUE ? "unique" : "";
			string prefstr = type == IndexType.INDEX_UNIQUE ? "UNQ" : "IDX";
			string fieldstr = "";
			string fieldlist = "";
			foreach( string fld in keys ) {
				if( fieldstr.Length > 0 )
					fieldstr += "_";
				if( fieldlist.Length > 0 )
					fieldlist += ",";
				fieldstr += fld.Split(' ')[ 0 ].Replace("`", "");
				fieldlist += fld;
			}

			string indexname = prefstr + "_" + table + "_" + fieldstr;

			int haveIndex = DataSetActionWith(db, trans,
				@"SHOW INDEX FROM "+table+" WHERE KEY_NAME = @indexName",
				KV("indexName", indexname)).Tables[ 0 ].Rows.Count;

			int maxIndexLen = 64;
			if( indexname.Length > maxIndexLen ) {
				string origIndex = indexname;
				string rest = indexname.Substring( maxIndexLen-6 );
				indexname = indexname.Substring( 0, maxIndexLen-6 ) + "_" +
						( Math.Abs( ( indexname + rest ).GetHashCode() ) % 100000 ).ToString();
				log.warning("index name, \"{0}\" was too long ({1} of {2} chars), using \"{3}\" instead",
					origIndex, origIndex.Length, maxIndexLen, indexname );
			}

			string sql = @"CREATE " + typestr + " INDEX "+
					indexname+" ON "+table+"("+fieldlist+");";

			if( haveIndex == 0 ) {
				log.info("Creating new index: {0}", sql);
				Update(db, trans, sql);
				return true;
			}

			log.info("Already have index for {0}", indexname);
			return false;
		}
		/// <summary>
		/// Delete an index with the passed parameters if it already exist.  If it
		/// does exist, do nothing.
		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="table"></param>
		/// <param name="type"></param>
		/// <param name="keys"></param>
		/// <returns>true if the index was deleted, false if it already was gone</returns>
		public static bool DeleteIndexIfExists( Database db, DbTransaction trans, string table, IndexType type, params string[] keys ) {

			string typestr = type == IndexType.INDEX_UNIQUE ? "unique" : "";
			string prefstr = type == IndexType.INDEX_UNIQUE ? "UNQ" : "IDX";
			string fieldstr = "";
			string fieldlist = "";
			foreach( string fld in keys ) {
				if( fieldstr.Length > 0 )
					fieldstr += "_";
				if( fieldlist.Length > 0 )
					fieldlist += ",";
				fieldstr += fld.Split( ' ' )[ 0 ].Replace( "`", "" );
				fieldlist += fld;
			}

			string indexname = prefstr + "_" + table + "_" + fieldstr;

			int haveIndex = DataSetActionWith( db, trans,
				@"SHOW INDEX FROM " + table + " WHERE KEY_NAME = @indexName",
				KV( "indexName", indexname ) ).Tables[ 0 ].Rows.Count;

			string sql = @"ALTER TABLE `"+table+"` DROP INDEX " +
					indexname +";";

			if( haveIndex == 1 ) {
				log.info( "Dropping index: {0}", sql );
				Update( db, trans, sql );
				return true;
			}

			log.info( "Do not have an index named {0}, cannot delete", indexname );
			return false;
		}

		/// <summary>
		/// Check for the existance of a function in the schema
		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="function"></param>
		/// <param name="schema"></param>
		/// <returns></returns>
		public static bool FunctionExistsInSchema( Database db, DbTransaction trans, string function, string schema ) {
			return DataRowExistsWith(db, trans, @"
				SELECT * FROM information_schema.routines p WHERE routine_schema = @schema AND routine_name = @routine",
				KV("schema", schema),
				KV("routine", function ));
		}

		/// <summary>
		/// Get a dataset as a mapped set of name/value pairs in a dictionary using a delegate to create the KeyValuePair objects.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <typeparam name="V"></typeparam>
		/// <param name="sql"></param>
		/// <param name="rvc"></param>
		/// <returns></returns>
		public static Dictionary<T, V> MappedDataSet<T, V>( string sql, DelDataSetOperation<KeyValuePair<T, V>> rvc ) {
			return MappedDataSet(sql, ( DataRow row, int idx, int count ) => {
				return rvc(row);
			});
		}

		/// <summary>
		/// Get a dataset as a mapped set of name/value pairs in a dictionary using a delegate to create the KeyValuePair objects, with parameters to the dataset query.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <typeparam name="V"></typeparam>
		/// <param name="sql"></param>
		/// <param name="rvc"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static Dictionary<T, V> MappedDataSetWith<T, V>( string sql, DelDataSetOperation<KeyValuePair<T, V>> rvc, params KeyValuePair<string, object>[] args ) {
			return MappedDataSetWith(sql, ( DataRow row, int idx, int count ) => {
				return rvc(row);
			}, args);
		}

		/// <summary>
		/// Get a dataset as a mapped set of name/value pairs in a dictionary using a delegate to create the KeyValuePair objects.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <typeparam name="V"></typeparam>
		/// <param name="sql"></param>
		/// <param name="rvc"></param>
		/// <returns></returns>
		public static Dictionary<T, V> MappedDataSet<T, V>( string sql, DelDataSetOperationIndexed<KeyValuePair<T, V>> rvc ) {
			Dictionary<T,V> dict = new Dictionary<T, V>();
			List<KeyValuePair<T,V>> v = ListForDataSet<KeyValuePair<T, V>>(
					DataSetAction(sql),
					( DataRow row, int idx, int count ) => {
						return rvc(row, idx, count);
					});
			foreach( KeyValuePair<T,V> kp in v ) {
				dict.Add(kp.Key, kp.Value);
			}
			return dict;
		}

		/// <summary>
		/// Get a dataset as a mapped set of name/value pairs in a dictionary using a delegate to create the KeyValuePair objects, with parameters to the dataset query.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <typeparam name="V"></typeparam>
		/// <param name="sql"></param>
		/// <param name="rvc"></param>
		/// <param name="args"></param>
		/// <returns>returns the dictionary created by 'rvc' as envoked with all returned rows</returns>
		public static Dictionary<T, V> MappedDataSetWith<T, V>( string sql, DelDataSetOperationIndexed<KeyValuePair<T, V>> rvc,
				params KeyValuePair<string, object>[] args ) {
			Dictionary<T,V> dict = new Dictionary<T, V>();
			List<KeyValuePair<T,V>> v = ListForDataSet<KeyValuePair<T, V>>(
					DataSetActionWith(sql, args),
					( DataRow row, int idx, int count ) => {
						return rvc(row, idx, count);
					});
			foreach( KeyValuePair<T,V> kp in v ) {
				dict.Add(kp.Key, kp.Value);
			}
			return dict;
		}

		/// <summary>
		/// Check if there are any rows returned from the passed transaction
		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="args"></param>
		/// <returns>true if there is at least one row returned, false if not</returns>
		public static bool DataRowExistsWith( Database db, DbTransaction trans, string sqlCommand, params KeyValuePair<string, object>[] args ) {
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				dbCommand.Transaction = trans;
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				AddInParameters( db, dbCommand, args );
				DataSet ds = db.ExecuteDataSet( dbCommand );
				log.fine( "DataRowExistsWith( {0} ): {1}", sqlCommand, ds.Tables[ 0 ].Rows.Count );
				return ds.Tables[ 0 ].Rows.Count > 0;

			}
		}

		/// <summary>
		/// Get the first DataSet element from the query with parameters, or null.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="nullValue">The appropriate null value to return when there are no rows, must be null</param>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static T FirstDataSetElementOrNullWith<T>( T nullValue, string sqlCommand, DelDataSetOperationIndexed<T> act,
				params KeyValuePair<string, object>[] args ) {
			if( nullValue != null ) {
				log.warning("non null value {0} in this API should be using FirstDataSetElementOrDefaultWith()", nullValue);
			}
			return DataSetElementOrDefaultAt<T>( nullValue, Transaction<DataSet>(( Database db, DbTransaction trans ) => {
                using (DbCommand dbCommand = db.GetSqlStringCommand(sqlCommand))
                {
				dbCommand.Transaction = trans;
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				AddInParameters(db, dbCommand, args);
				return db.ExecuteDataSet(dbCommand);
                }
			}), 0, act);
		}

		/// <summary>
		/// Get first result row or throw KeyNotFoundException
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <param name="args"></param>
		/// <exception cref="KeyNotFoundException">When there are no rows returned</exception>
		/// <returns></returns>
		public static T FirstDataSetElementOrMissingKeyWith<T>( string sqlCommand, DelDataSetOperation<T> act,
							params KeyValuePair<string, object>[] args) {
			return DataSetElementOrMissingKeyAt<T>( Transaction<DataSet>((Database db, DbTransaction trans) => {
				return DataSetActionWith(db, trans, sqlCommand, args);
			}), 0, act);
		}


		/// <summary>
		/// Get first result row or throw KeyNotFoundException
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <param name="args"></param>
		/// <exception cref="KeyNotFoundException">When there are no rows returned</exception>
		/// <returns></returns>
		public static T FirstDataSetElementOrMissingKeyWith<T>( Database db, DbTransaction trans, string sqlCommand, DelDataSetOperation<T> act,
							params KeyValuePair<string, object>[] args) {
			return DataSetElementOrMissingKeyAt<T>( DataSetAction(db, trans, sqlCommand), 0, act);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="nullValue"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static T FirstDataSetElementOrNullWith<T>( Database db, DbTransaction trans, T nullValue, string sqlCommand, DelDataSetOperationIndexed<T> act,
				params KeyValuePair<string, object>[] args ) {
			if( nullValue != null ) {
				log.warning( "non null value {0} in this API should be using FirstDataSetElementOrDefaultWith()", nullValue );
			}
			return DataSetElementOrDefaultAt<T>( nullValue, DataSetActionWith( db, trans, sqlCommand, args ), 0, act );
		}


		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="nullValue"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <returns></returns>
		public static T FirstDataSetElementOrNull<T>( Database db, DbTransaction trans, T nullValue, string sqlCommand, DelDataSetOperationIndexed<T> act ) {
			if( nullValue != null ) {
				log.warning("non null value {0} in this API should be using FirstDataSetElementOrDefaultWith()", nullValue);
			}
			return DataSetElementOrDefaultAt<T>(nullValue, DataSetAction(db, trans, sqlCommand), 0, act);
		}

		private static IsolationLevel databaseIsolationLevel;
		private static bool isolationLevelSet;

		public static IsolationLevel DatabaseIsolationLevel {
			get {
				if (isolationLevelSet) {
					return databaseIsolationLevel;
				}

				// The default isolation level
				return IsolationLevel.RepeatableRead;
			}
			set {
				databaseIsolationLevel = value;
				isolationLevelSet = true;
			}
		}

		/// <summary>
		/// A global behavior boolean which turns On or Off, the use of timeouts on all transactions created within this
		/// class.  This is used for database schema updates or other places where timeouts are not needed and can, in fact,
		/// break the desired behavior of a long running transaction.
		/// </summary>
		public static bool UseDatabaseCommandTimeout { get; set; }
		private static int databaseCommandTimeout;
		private static bool databaseCommandTimeoutSet;

		/// <summary>
		/// 
		/// </summary>
		public static int DatabaseCommandTimeout {
			get {
				if( databaseCommandTimeoutSet ) {
					return databaseCommandTimeout;
				}
				if( UseDatabaseCommandTimeout ) {
					string str = ConfigurationManager.AppSettings[ "Database:TransactionTimeout" ];
					if( str != null ) {
						return databaseCommandTimeout = int.Parse(str);
					}
					return 900;  // 15 minutes by default.
				}

				// DbCommand.CommandTimeout documentation says that 0 should mean no timeout
				return 0;
			}
			set {
				databaseCommandTimeout = value;
				databaseCommandTimeoutSet = true;
			}
		}

		/// <summary>
		/// Get the first row value for the returned dataset or the passed default value with parameters.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="defaultValue">must be non-null</param>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static T FirstDataSetElementOrDefaultWith<T>( T defaultValue, string sqlCommand, DelDataSetOperationIndexed<T> act, params KeyValuePair<string,object>[] args ) {
			if( defaultValue == null ) {
				log.warning("null value {0} in this API should be using FirstDataSetElementOrNullWith()", "defaultValue");
			}
			return Transaction<T>(( Database db, DbTransaction trans ) =>
					FirstDataSetElementOrDefaultWith<T>(db, trans, defaultValue, sqlCommand, act, args)
			);
		}
		/// <summary>
		/// Get the first row value for the returned dataset or the passed default value with parameters.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="defaultValue"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static T FirstDataSetElementOrDefaultWith<T>( Database db, DbTransaction trans, T defaultValue, string sqlCommand, DelDataSetOperationIndexed<T> act, params KeyValuePair<string, object>[] args) {
			if (defaultValue == null) {
				log.warning("null value {0} in this API should be using FirstDataSetElementOrNullWith()", "defaultValue");
			}
			return DataSetElementOrDefaultAt<T>(defaultValue, DataSetActionWith(db, trans, sqlCommand, args), 0, act);
		}
		/// <summary>
		/// Get the first row value for the returned dataset or the passed default value 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="defaultValue"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <returns></returns>
		public static T FirstDataSetElementOrDefault<T>( T defaultValue, string sqlCommand, DelDataSetOperationIndexed<T> act ) {
			return FirstDataSetElementOrDefaultWith<T>( defaultValue, sqlCommand, act );
		}

		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="defaultValue"></param>
		/// <param name="dataSet"></param>
		/// <param name="act"></param>
		/// <returns></returns>
		public static T FirstDataSetElementOrDefault<T>( T defaultValue, DataSet dataSet, DelDataSetOperation<T> act ) {
			return DataSetElementOrDefaultAt<T>(defaultValue, dataSet, 0, act);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="defaultValue"></param>
		/// <param name="dataSet"></param>
		/// <param name="act"></param>
		/// <returns></returns>
		public static T FirstDataSetElementOrDefault<T>( T defaultValue, DataSet dataSet, DelDataSetOperationIndexed<T> act ) {
			return DataSetElementOrDefaultAt<T>(defaultValue, dataSet, 0, act);
		}

		/// <summary>
		/// Execute the passed action on the indicated row of the passed dataset or return the default value.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="defaultValue"></param>
		/// <param name="ds"></param>
		/// <param name="rowIdx"></param>
		/// <param name="act"></param>
		public static T DataSetElementOrDefaultAt<T>(T defaultValue, DataSet ds, int rowIdx, DelDataSetOperationIndexed<T> act) {

			int cnt = ds.Tables[ 0 ].Rows.Count;
			if( cnt <= rowIdx )
				return defaultValue;
			return act( ds.Tables[ 0 ].Rows[ rowIdx ], rowIdx, cnt );
		}

		/// <summary>
		/// If there is a row at rowIdx, pass it to 'act' and return the result value,
		/// otherwise, return defaultValue when there is no such row.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="defaultValue"></param>
		/// <param name="ds"></param>
		/// <param name="rowIdx"></param>
		/// <param name="act"></param>
		/// <returns></returns>
		public static T DataSetElementOrDefaultAt<T>(T defaultValue, DataSet ds, int rowIdx, DelDataSetOperation<T> act) {
			int cnt = ds.Tables[ 0 ].Rows.Count;
			if( cnt <= rowIdx )
				return defaultValue;
			return act( ds.Tables[ 0 ].Rows[ rowIdx ] );
		}
		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="defaultValue"></param>
		/// <param name="ds"></param>
		/// <param name="rowIdx"></param>
		/// <param name="act"></param>
		/// <exception cref="KeyNotFoundException">When the requested row does not exist</exception>
		/// <returns></returns>
		public static T DataSetElementOrMissingKeyAt<T>( DataSet ds, int rowIdx, DelDataSetOperation<T> act) {
			int cnt = ds.Tables[0].Rows.Count;
			if (cnt <= rowIdx)
				throw new KeyNotFoundException(string.Format("No row at index {0} for DataSet with {1} items", rowIdx, ds.Tables[0].Rows.Count ) );
			return act(ds.Tables[0].Rows[rowIdx]);
		}
		/// <summary>
		/// Use this to create lists of values from columns or combinations/functions on columns in a DataSet.
		/// The simplest example would be getting the 'name' values from a table as shown here.
		/// 
		/// List<typeparamref name="string"/> values = ForDataSet<typeparamref name="string"/>( DataSetAction( "select name from table"), (DataRow row) ==> row["name"].ToString() );
		/// 
		/// A more complex example might include manipulation of the value.
		/// 
		/// List<typeparamref name="string"/> values = ForDataSet<typeparamref name="string"/>( DataSetAction( "select name from table"), (DataRow row) ==> {
		///			string nm = row["name"].ToString();
		///			// nm is of the form "text-data" and we just want "text"
		///			return nm.Substring( 0, nm.LastIndexOf('-') );
		///		}
		///	);
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="ds"></param>
		/// <param name="dos"></param>
		/// <returns></returns>
		public static List<T> ListForDataSet<T>( DataSet ds, DelDataSetOperation<T> dos ) {
			List<T> l = new List<T>();
			int cnt = ds.Tables[ 0 ].Rows.Count;
			for( int i = 0; i < cnt; ++i ) {
				l.Add(dos(ds.Tables[ 0 ].Rows[ i ]));
			}
			return l;
		}

		/// <summary>
		/// This method includes a delegate signature which includes the index of the current row and the total row count.
		/// 
		/// List<typeparamref name="string"/> values = ForDataSet<typeparamref name="string"/>( DataSetAction( "select name from table"), (DataRow row, int row, int count) ==> {
		///			showprogress( row, count );
		///			return row["name"].ToString();
		///		}
		///	);
		///	
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="ds"></param>
		/// <param name="dos"></param>
		/// <returns></returns>
		public static List<T> ListForDataSet<T>( DataSet ds, DelDataSetOperationIndexed<T> dos ) {
			List<T> l = new List<T>();
			int cnt = ds.Tables[ 0 ].Rows.Count;
			for( int i = 0; i < cnt; ++i ) {
				l.Add(dos(ds.Tables[ 0 ].Rows[ i ], i, cnt ));
			}
			return l;
		}

		/// <summary>
		/// This method provides the ability to act on each row in a dataset to perform some function.
		/// 
		/// AcrossDataSet( DataSetAction("select name from table"), (DataRow row) => {
		///			ProcessData( row["name"].ToString() );
		///		}
		///	);
		/// 
		/// or more simply when there is a single function call:
		/// 
		/// AcrossDataSet( DataSetAction("select name from table"), 
		///		(DataRow row) => ProcessData( row["name"].ToString() ) );
		/// 
		/// </summary>
		/// <param name="ds"></param>
		/// <param name="dos"></param>
		public static void AcrossDataSet( DataSet ds, DelDataSetAction dos ) {
			for( int i = 0; i < ds.Tables[ 0 ].Rows.Count; ++i ) {
				dos(ds.Tables[ 0 ].Rows[ i ]);
			}
		}

		/// <summary>
		/// This method provides the ability to act on each row in a dataset to perform some function
		/// with the row and count of rows provided.
		/// 
		/// AcrossDataSet( DataSetAction("select name from table"), (DataRow row, int row, int count) => {
		///			ProcessData( row["name"].ToString(), row, count );
		///		}
		///	);
		/// 
		/// or more simply when there is a single function call:
		/// 
		/// AcrossDataSet( DataSetAction("select name from table"), 
		///		(DataRow row) => ProcessData( row["name"].ToString(), row, count ) );
		/// </summary>
		/// <param name="ds"></param>
		/// <param name="dos"></param>
		public static void AcrossDataSet( DataSet ds, DelDataSetActionIndexed dos ) {
			int cnt = ds.Tables[ 0 ].Rows.Count;
			for( int i = 0; i < cnt; ++i ) {
				DataRow row = ds.Tables[ 0 ].Rows[ i ];
				dos( row, i, cnt );
			}
		}

		/// <summary>
		/// Perform an operation with transactional control
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="oper"></param>
		/// <returns></returns>
		public static T Transaction<T>( DelDatabaseTransaction<T> oper, string descr = "Transaction" ) {
			Database db = null;
				
			try {
				db = CreateDatabase();
				using(DbConnection conn = db.CreateConnection() ) {
					conn.Open();
					using( DbTransaction trans = conn.BeginTransaction( DatabaseIsolationLevel ) ) {
						try {
							log.fine( "Starting transaction with Isolation: {0}", trans.IsolationLevel );
							T v = oper( db, trans );
							trans.Commit();
							return v;
						} catch {
							trans.Rollback();
							throw;
						}
					};
				};
			} catch( Exception ex ) {
				log.fine( ex );
				throw;
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="databaseName"></param>
		/// <param name="oper"></param>
		/// <returns></returns>
		public static T Transaction<T>( string databaseName, DelDatabaseTransaction<T> oper, string descr = "Transaction" ) {
			Database db = null;
				
			try {
				db = CreateDatabase( databaseName );
				using(DbConnection conn = db.CreateConnection() ) {
					conn.Open();
					using( DbTransaction trans = conn.BeginTransaction( DatabaseIsolationLevel ) ) {
						try {
							log.fine( "Starting transaction with Isolation: {0}", trans.IsolationLevel );
							T v = oper( db, trans );
							trans.Commit();
							return v;
						} catch {
							trans.Rollback();
							throw;
						}
					};
				}
			} catch( Exception ex ) {
				log.fine( ex );
				throw;
			}
		}

		/// <summary>
		/// Create a DataSet for a simple query
		/// </summary>
		/// <param name="sqlCommand"></param>
		/// <returns></returns>
		public static DataSet DataSetAction( string sqlCommand ) {
			return Transaction<DataSet>(( Database db, DbTransaction trans ) => {
				return DataSetAction(db, trans, sqlCommand);
			}, "DataSetAction");
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <returns></returns>
		public static DataSet DataSetAction( Database db, DbTransaction trans, string sqlCommand, params KeyValuePair<string, object>[] args ) {
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				dbCommand.Transaction = trans;
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				AddInParameters( db, dbCommand, args );
				return db.ExecuteDataSet( dbCommand, trans );
			}
		}

		/// <summary>
		/// Run a query with a list of objects to be included as a comma separated list.  To use this, the sqlString
		/// value needs to contains a "special" formatted string which will be replaced with the comma separated list
		/// of values.
		/// 
		/// If the method is called as:
		/// 
		/// List&lt;string&gt; myList = new List&lt;string&gt;();
		/// myList.Add("item1");
		/// myList.Add("item2");
		/// myList.Add("item3");
		/// DataSetActionIncluding&lt;string&gt;( db, trans, "select * from table where colname in (@LIST_items)",
		///			"items", myList );
		///	
		/// then the sql statement will be rewritten to read
		/// 
		///		"select * from table where colname in (@items0,@items1,@items2)"
		/// 
		/// and then the db.GetSqlStringCommand() call will be used to get the DbCommand. Then, the parameter values
		/// will be added with Database.AddInParameter().
		/// 
		/// </summary>
		/// <typeparam name="T">Provide a type qualifier to manage that the list type doesn't change</typeparam>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="ofName"></param>
		/// <param name="items"></param>
		/// <returns></returns>
        public static DataSet DataSetActionIncluding<T>(Database db, DbTransaction trans, string sqlCommand, string ofName, List<T> items) {
            sqlCommand = ReplaceListInStatement<T>(sqlCommand, ofName, items);
            using (DbCommand dbCommand = db.GetSqlStringCommand(sqlCommand)) {
                dbCommand.Transaction = trans;
                dbCommand.CommandTimeout = DatabaseCommandTimeout;
                AddInValuesForList<T>(db, dbCommand, ofName, items);

                return db.ExecuteDataSet(dbCommand, trans);
            }
        }

		/// <summary>
		/// Provide name of parameter at idx offset in list, named, 'name'.
		/// </summary>
		/// <param name="sqlCommand"></param>
		/// <param name="idx"></param>
		/// <param name="name"></param>
		/// <returns></returns>
		public delegate string DelIndexNamesProvider( string sqlCommand, int idx, string name );

		/// <summary>
		/// Add the data values for the specified parameter at idx offset in list, named, 'name'.
		/// </summary>
		/// <param name="db"></param>
		/// <param name="cmd"></param>
		/// <param name="idx"></param>
		/// <param name="name"></param>
		public delegate void DelIndexListProvider( Database db, DbCommand cmd, int idx, string name );

		/// <summary>
		/// This method provides a mechanism for substituting multiple lists of objects into
		/// a SQL statement.  The typical use of this would be something like the following:
		/// 
		/// List&lt;string&gt; field1List;
		/// List&lt;int&gt; field2List;
		/// DataSet ds = DataSetActionIncludingMulti( db, trans, @"
		///		select * from table1 where field1 in (@LIST_field1)
		///			and field2 in (@LIST_field2)",
		///			/* number of lists */ 2,
		///			/* replace name lists in sql statement*/( sql, idx, name ) {
		///				if( name.equals("field1") )
		///					return BaseDAO.ReplaceListInStatement&lt;string&gt;( sql, "field1", field1List );
		///				return BaseDAO.ReplaceListInStatement&lt;int&gt;( sql, "field2", field2List );
		///			},
		///			/* add values in lists */ (db, dbcmd, idx, name ) {
		///				if( name.equals("field1") )
		///					BaseDAO.AddInValuesForList&lt;string&gt;( db, cmd, "field1", field1List );
		///				BaseDAO.AddInValuesForList&lt;int&gt;( db, cmd, "field2", field2List );
		///			});
		/// </summary>
		/// 
		/// <param name="db">The database instance to use</param>
		/// <param name="trans">The transaction to use</param>
		/// <param name="sqlCommand">the base SQL statement</param>
		/// <param name="itemNames">the names of each type of item having an "in" clause</param>
		/// <param name="names">the names provider callback delegate</param>
		/// <param name="list">the values provided callback delegate</param>
		/// <param name="args">Any other parameters to the SQL query that need to be added as values</param>
		/// <returns>The result DataSet from the database query</returns>
		public static DataSet DataSetActionIncludingMulti( Database db, DbTransaction trans, string sqlCommand, List<string> itemNames,
					DelIndexNamesProvider names, DelIndexListProvider list, params KeyValuePair<string, object>[] args ) {
			for( int i = 0; i < itemNames.Count; ++i ) {
				sqlCommand = names( sqlCommand, i, itemNames[ i ] );
			}
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				dbCommand.Transaction = trans;
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				for( int i = 0; i < itemNames.Count; ++i ) {
					list( db, dbCommand, i, itemNames[ i ] );
				}
				AddInParameters( db, dbCommand, args );
				return db.ExecuteDataSet( dbCommand, trans );
			}
		}

		public static int DataSetUpdateIncludingMulti( Database db, DbTransaction trans, string sqlCommand, List<string> itemNames,
			DelIndexNamesProvider names, DelIndexListProvider list, params KeyValuePair<string, object>[] args ) {
			for( int i = 0; i < itemNames.Count; ++i ) {
				sqlCommand = names( sqlCommand, i, itemNames[ i ] );
			}
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				dbCommand.Transaction = trans;
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				for( int i = 0; i < itemNames.Count; ++i ) {
					list( db, dbCommand, i, itemNames[ i ] );
				}

				AddInParameters( db, dbCommand, args );

				return db.ExecuteNonQuery( dbCommand, trans );
			}
		}

		/// <summary>
		/// Populates parameter values into a SQL command string as @ofNameN, where N is the index into
		/// the items list.  Can be used for an 'in' clause or other place where multiple values are included
		/// in a SQL command string.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="db"></param>
		/// <param name="dbCommand"></param>
		/// <param name="ofName"></param>
		/// <param name="items"></param>
		public static void AddInValuesForList<T>(Database db, DbCommand dbCommand, string ofName, List<T>items ) {
			for( int i = 0; i < items.Count; ++i ) {
				// object seems to work correctly for every type
				// except DateTime and DateTimeOffset types which
				// need explicit typing to not be converted to
				// string values which then don't parse correctly 
				// by the SQL statement parser.	
				DbType type = DbType.Object;
				if( items[ i ] is DateTime ) {
					type = DbType.DateTime;
				} else if( items[ i ] is DateTimeOffset ) {
					type = DbType.DateTimeOffset;
				}
				db.AddInParameter(dbCommand, "@" + ofName + ( i.ToString() ), type, items[ i ]);
			}
		}

		public static string ReplaceListInStatement<T>( string sqlCommand, string ofName, List<T> items ) {
			StringBuilder vals = new StringBuilder();
			for( int i = 0; i < items.Count; ++i ) {
				if( i > 0 ) vals.Append( "," );
				vals.Append("@").Append( ofName ).Append( i.ToString() );
			}
			string res = sqlCommand.Replace("@LIST_" + ofName, vals.ToString() );
			if( res.Equals(sqlCommand) ) {
				log.warning("No \"@LIST_{0}\" pattern was found to replace with {1}:\n{2}",
					ofName, vals, Environment.StackTrace);
			}
			return res;
		}

		public static DataSet DataSetActionIncludingWith<T>( Database db, DbTransaction trans, string sqlCommand, string ofName, List<T> items, params KeyValuePair<string, object>[] args ) {
			sqlCommand = ReplaceListInStatement<T>( sqlCommand, ofName, items );
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				dbCommand.Transaction = trans;
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				AddInValuesForList<T>( db, dbCommand, ofName, items );
				AddInParameters( db, dbCommand, args );

				return db.ExecuteDataSet( dbCommand, trans );
			}
		}

		/// <summary>
		/// Processes KeyValuePair values to set parameters on SQL statement.
		/// </summary>
		/// <param name="db"></param>
		/// <param name="dbCommand"></param>
		/// <param name="args"></param>
		internal static void AddInParameters( Database db, DbCommand dbCommand, params KeyValuePair<string, object>[] args ) {
			foreach( KeyValuePair<string,object> arg in args ) {
				if( arg.Key == null )
					continue;
				if( arg.Value is DateTime ) {
					db.AddInParameter(dbCommand, arg.Key, DbType.DateTime, arg.Value);
				} else if( arg.Value is DateTimeOffset ) {
					db.AddInParameter( dbCommand, arg.Key, DbType.DateTimeOffset, arg.Value );
				} else if( arg.Value is bool ) {
					db.AddInParameter( dbCommand, arg.Key, DbType.Boolean, arg.Value );
				} else {
					db.AddInParameter(dbCommand, arg.Key, DbType.Object, arg.Value);
				}
			}
		}

		/// <summary>
		/// Create a DataSet from a SQL command with arguments.
		/// </summary>
		/// <param name="sqlCommand"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static DataSet DataSetActionWith( string sqlCommand, params KeyValuePair<string, object>[] args ) {
			return Transaction<DataSet>(( Database db, DbTransaction trans ) => {
				return DataSetActionWith(db, trans, sqlCommand, args);
			}, "DataSetActionWith");
		}

		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static DataSet DataSetActionWith( Database db, DbTransaction trans, string sqlCommand, params KeyValuePair<string, object>[] args ) {
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				//dbCommand.Transaction = trans;
				dbCommand.Connection = trans.Connection;
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				AddInParameters(db, dbCommand, args);
				return db.ExecuteDataSet(dbCommand, trans);
			}
		}

		/// <summary>
		/// Create a DataSet with optional and non-optional parameters
		/// </summary>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static DataSet DatasetOptionActionWith( string sqlCommand, DelDatabaseOptionTask act, params KeyValuePair<string, object>[] args ) {
			return Transaction<DataSet>( ( Database db, DbTransaction trans ) => {
				using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
					dbCommand.Transaction = trans;
					dbCommand.CommandTimeout = DatabaseCommandTimeout;
					AddInParameters( db, dbCommand, args );
					act( db, dbCommand );
					return db.ExecuteDataSet( dbCommand, trans );
				}
			} );
		}

		/// <summary>
		/// Create a DataSet with passed database context.
		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static DataSet DatasetOptionActionWith( Database db, DbTransaction trans, string sqlCommand, DelDatabaseOptionTask act, params KeyValuePair<string, object>[] args ) {
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				dbCommand.Transaction = trans;
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				AddInParameters(db, dbCommand, args);
				act(db, dbCommand);
				return db.ExecuteDataSet(dbCommand, trans);

			}
		}

		/// <summary>
		/// Process database operations with a transaction
		/// </summary>
		/// <param name="oper"></param>
		public static void DatabaseAction( DelDatabaseActionTransaction oper ) {
			Database db = CreateDatabase();
			try {
				using(DbConnection conn = db.CreateConnection() ) {
					conn.Open();
						using( DbTransaction trans = conn.BeginTransaction( DatabaseIsolationLevel ) ) {
							try {
								log.fine( "Starting transaction with Isolation: {0}", trans.IsolationLevel );
								oper( db, trans );
								trans.Commit();
								return;
							} catch {
								trans.Rollback();
								throw;
							}
						}
				};

			} catch( Exception ex ) {
				log.severe( ex );
				throw;
			}
		}

		public static string DefaultDatabaseName { get; set; }

		/// <summary>
		/// Create a database instance using the designated database found in the
		/// DefaultDatabaseName property
		/// </summary>
		/// <returns></returns>
		public static Database CreateDatabase() {
			string conn = "<not found>";
			if( DefaultDatabaseName != null ) {
				log.fine("Creating database instance for {0}", DefaultDatabaseName);
				foreach( ConnectionStringSettings c in ConfigurationManager.ConnectionStrings ) {
					if( c.Name.Equals(DefaultDatabaseName) ) {
						conn = c.ConnectionString;
						break;
					}
				}
				log.fine("Creating database instance using app.config default: {0}: {1}",
					DefaultDatabaseName, conn);
				return DatabaseFactory.CreateDatabase(DefaultDatabaseName);
			}
			DatabaseSettings db = (DatabaseSettings)ConfigurationManager.GetSection("dataConfiguration");
			log.fine("Creating database instance for {0}", db.DefaultDatabase);
			foreach( ConnectionStringSettings c in ConfigurationManager.ConnectionStrings ) {
				if( c.Name.Equals(db.DefaultDatabase) ) {
					conn = c.ConnectionString;
					break;
				}
			}
			log.fine("Creating database instance using app.config default: {0}: {1}",
				db.DefaultDatabase, conn);
			return DatabaseFactory.CreateDatabase();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="databaseName"></param>
		/// <returns></returns>
		public static Database CreateDatabase( string databaseName ) {
			DatabaseSettings db = (DatabaseSettings)ConfigurationManager.GetSection( "dataConfiguration" );
			log.fine( "Creating database instance for {0}", databaseName );
			string conn = "<not found>";
			foreach( ConnectionStringSettings c in ConfigurationManager.ConnectionStrings ) {
				if( c.Name.Equals( db.DefaultDatabase ) ) {
					conn = c.ConnectionString;
					break;
				}
			}
			log.fine( "Creating database instance using app.config default: {0}: {1}",
				db.DefaultDatabase, conn );
			//			return new SqlDatabase(conn);
			return DatabaseFactory.CreateDatabase( databaseName );
		}

		/// <summary>
		/// Process database actions with just the database, no transactions
		/// </summary>
		/// <param name="oper"></param>
		public static void DatabaseAction( DelDatabaseActionTask oper, string descr = "DatabaseAction" ) {
            Database db = CreateDatabase();
				
			using(DbConnection conn = db.CreateConnection() ) {
				conn.Open();
					oper( db );
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="oper"></param>
		public static void DatabaseAction( string databaseName, DelDatabaseActionTask oper, string descr="DatabaseAction" ) {
			Database db = CreateDatabase( databaseName );
				
			using(DbConnection conn = db.CreateConnection() ) {
				conn.Open();
				    oper( db );
					
			}
		}

		/// <summary>
		/// Process a database action with a return value
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="oper"></param>
		/// <returns></returns>
		public static T Operation<T>( DelDatabaseTask<T> oper, string descr="Operation" ) {
			Database db = CreateDatabase();
			using(DbConnection conn = db.CreateConnection() ) {
                conn.Open();
                try {
					T v = oper( db );
					return v;
				} catch (Exception ex) {
                    log.severe("Error in Operation {0}", ex, ex.Message);
                    throw;
				} 
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="oper"></param>
		/// <returns></returns>
		public static T Operation<T>( string databaseName, DelDatabaseTask<T> oper, string descr = "Operation" ) {
			Database db = CreateDatabase( databaseName );
			using(DbConnection conn = db.CreateConnection() ) {
				conn.Open();
				try {
					T v = oper( db );
					return v;
				} catch( Exception ex ) {
					log.severe( "Error in Operation {0}", ex, ex.Message );
					throw;
				} 
			}
		}

		/// <summary>
		/// Get a scalar value result from a simple query
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="sqlCommand"></param>
		/// <returns></returns>
		public static T OperationForScalar<T>( string sqlCommand ) {
            return Operation<T>((Database db) => {
                using (DbCommand dbCommand = db.GetSqlStringCommand(sqlCommand))
                {
                    dbCommand.CommandTimeout = DatabaseCommandTimeout;
                    object val = db.ExecuteScalar(dbCommand);
                    log.fine("have {0} need to convert to type {1}", val, typeof(T));
                    try
                    {
                        return (T) Convert.ChangeType(val, typeof(T));
                    }
                    catch (Exception ex)
                    {
                        log.severe("Can not convert {0} to type {1}: {2}", ex, val, typeof(T), ex.Message);
                        throw;
                    }
                }
            }, "OperationForScalar");
		}

		/// <summary>
		/// Get a scalar result from a query with parameters
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="sqlCommand"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static T OperationForScalarWith<T>( string sqlCommand, params KeyValuePair<string, object>[] args ) {
			return Transaction<T>(( Database db, DbTransaction trans ) => {
				return OperationForScalarWith<T>(db, trans, sqlCommand, args);
			}, "OperationForScalarWith" );
		}

		/// <summary>
		/// Get a scalar result from a query with parameters
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="sqlCommand"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static T OperationForScalarWith<T>( Database db, DbTransaction trans, string sqlCommand, params KeyValuePair<string, object>[] args ) {
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				AddInParameters( db, dbCommand, args );
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				object val = db.ExecuteScalar( dbCommand, trans );
				log.fine( "have {0} need to convert to type {1}", val == null ? "<null>" : val.ToString(), typeof( T ) );
				try {
					return (T)Convert.ChangeType( val, typeof( T ) );
				} catch( Exception ex ) {
					log.severe( "Can not convert {0} to type {1}: {2}", ex, val == null ? "<null>" : val.ToString(), typeof( T ), ex.Message );
					throw;
				}
			}
		}

		/// <summary>
		/// Perform an operation on a scalar value to return
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <returns></returns>
		public static T OperationForScalarAction<T>( string sqlCommand, DelScalarAction<T> act ) {
			return Operation<T>( ( Database db ) => {
				using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
					dbCommand.CommandTimeout = DatabaseCommandTimeout;
					object val = db.ExecuteScalar( dbCommand );
					log.fine( "have {0} need to convert to type {1}", val, typeof( T ) );
					try {
						return act( (T)Convert.ChangeType( val, typeof( T ) ) );
					} catch( Exception ex ) {
						log.severe( "Can not convert {0} to type {1}: {2}", ex, val, typeof( T ), ex.Message );
						throw;
					}
				}
			}, "OperationForScalarAction" );
		}

		/// <summary>
		/// Perform an operation on a scalar value to return with parameters
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static T OperationForScalarActionWith<T>( string sqlCommand, DelScalarAction<T> act, params KeyValuePair<string, object>[] args ) {
			return Operation<T>(( Database db ) => {
				using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
					AddInParameters( db, dbCommand, args );
					dbCommand.CommandTimeout = DatabaseCommandTimeout;
					object val = db.ExecuteScalar( dbCommand );
					log.fine( "have {0} need to convert to type {1}", val, typeof( T ) );
					try {
						return act( (T)Convert.ChangeType( val, typeof( T ) ) );
					} catch( Exception ex ) {
						log.severe( "Can not convert {0} to type {1}: {2}", ex, val, typeof( T ), ex.Message );
						throw;
					}
				}
			}, "OperationForScalarActionWith" );
		}

		/// <summary>
		/// Get a scalar value within a transaction
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <returns></returns>
		public static T TransactionForScalar<T>( Database db, DbTransaction trans, string sqlCommand ) {
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				dbCommand.Transaction = trans;
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				object val = db.ExecuteScalar( dbCommand, trans );
				log.fine( "have {0} need to convert to type {1}", val, typeof( T ) );
				try {
					return (T)Convert.ChangeType( val, typeof( T ) );
				} catch( Exception ex ) {
					log.severe( "Can not convert {0} to type {1}: {2}", ex, val, typeof( T ), ex.Message );
					throw;
				}
			}
		}

		/// <summary>
		/// get a scalar value within a transaction with parameters
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static T TransactionForScalarWith<T>( Database db, DbTransaction trans, string sqlCommand, params KeyValuePair<string, object>[] args ) {
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				dbCommand.Transaction = trans;
				AddInParameters( db, dbCommand, args );
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				object val = db.ExecuteScalar( dbCommand, trans );
				log.fine( "have {0} need to convert to type {1}", val, typeof( T ) );
				try {
					return (T)Convert.ChangeType( val, typeof( T ) );
				} catch( Exception ex ) {
					log.severe( "Can not convert {0} to type {1}: {2}", ex, val, typeof( T ), ex.Message );
					throw;
				}
			}
		}

		/// <summary>
		/// get and process a scalar value in a transaction
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <returns></returns>
		public static T TransactionForScalarWithDataSet<T>(Database db, DbTransaction trans, string sqlCommand, params KeyValuePair<string, object>[] args)
        {
            using (DbCommand dbCommand = db.GetSqlStringCommand(sqlCommand)) {
                dbCommand.Transaction = trans;
                AddInParameters(db, dbCommand, args);
                dbCommand.CommandTimeout = DatabaseCommandTimeout;
                object val = db.ExecuteDataSet(dbCommand, trans);
                log.fine("have {0} need to convert to type {1}", val, typeof(T));
                try {
                    return (T)Convert.ChangeType(val, typeof(T));
                } catch (Exception ex) {
                    log.severe("Can not convert {0} to type {1}: {2}", ex, val, typeof(T), ex.Message);
                    throw;
                }
            }
        }

		public static T TransactionForScalar<T>( Database db, DbTransaction trans, string sqlCommand, DelScalarAction<T> act ) {
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				dbCommand.Transaction = trans;
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				object val = db.ExecuteScalar( dbCommand, trans );
				log.fine( "have {0} need to convert to type {1}", val, typeof( T ) );
				try {
					return act( (T)Convert.ChangeType( val, typeof( T ) ) );
				} catch( Exception ex ) {
					log.severe( "Can not convert {0} to type {1}: {2}", ex, val, typeof( T ), ex.Message );
					throw;
				}
			}
		}

		/// <summary>
		/// get and process a scalar value in a transaction with parameters
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static T TransactionForScalarWith<T>( Database db, DbTransaction trans, string sqlCommand, DelScalarAction<T> act, params KeyValuePair<string, object>[] args ) {
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				dbCommand.Transaction = trans;
				AddInParameters( db, dbCommand, args );
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				object val = db.ExecuteScalar( dbCommand, trans );
				log.fine( "have {0} need to convert to type {1}", val, typeof( T ) );
				try {
					return act( (T)Convert.ChangeType( val, typeof( T ) ) );
				} catch( Exception ex ) {
					log.severe( "Can not convert {0} to type {1}: {2}", ex, val, typeof( T ), ex.Message );
					throw;
				}
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="table"></param>
		/// <param name="columnName"></param>
		/// <param name="dataType"></param>
		/// <returns>true on insert, false on just update</returns>
		public static bool AlterTableAddColumn( Database db, DbTransaction trans, string table, string columnName, string dataType, string schema = "iwellscada" ) {
			if( DataSetActionWith( db, trans, @"
						SELECT data_type 
						FROM information_schema.columns
						WHERE 
							table_schema = @schema
						AND
							table_name = @table
						AND 
							column_name = @column;
					",
					KV( "table", table ),
					KV( "schema", schema ),
					KV( "column", columnName )
					 ).Tables[ 0 ].Rows.Count == 0 ) {
				Update( db, trans, "alter table `" + table + "` add column `" + columnName + "` " + dataType );
				return true;
			}
			Update( db, trans, "alter table `" + table + "` modify column `" + columnName + "` " + dataType );
			return false;
		}

		/// <summary>
		/// perform a simple update operation with the associated transaction
		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <returns>the number of rows updated</returns>
		public static int Update( Database db, DbTransaction trans, string sqlCommand ) {
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				return db.ExecuteNonQuery( dbCommand, trans );
			}
		}

		/// <summary>
		/// perform a simple update operation
		/// </summary>
		/// <param name="sqlCommand"></param>
		/// <returns>the number of rows updated</returns>
		public static int Update( string sqlCommand ) {
			return Transaction<int>(( Database db, DbTransaction trans ) => {
				return Update(db, trans, sqlCommand);
			});
		}

		/// <summary>
		/// perform an update with parameters using the passed transaction
		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static int UpdateWith( Database db, DbTransaction trans, string sqlCommand, params KeyValuePair<string, object>[] args ) {
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				//dbCommand.Transaction = trans;
				AddInParameters( db, dbCommand, args );
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				return db.ExecuteNonQuery( dbCommand, trans );
			}
		}

		/// <summary>
		/// perform an update with parameters and optional processing/parameters using the passed transaction
		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static int UpdateOptionWith( Database db, DbTransaction trans, string sqlCommand, DelDatabaseOptionTask act, params KeyValuePair<string, object>[] args ) {
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
			    dbCommand.Transaction = trans;
			    dbCommand.CommandTimeout = DatabaseCommandTimeout;
			    AddInParameters(db, dbCommand, args);
			    act(db, dbCommand);
			    dbCommand.CommandTimeout = DatabaseCommandTimeout;
			    return db.ExecuteNonQuery(dbCommand, trans);
		    }
		}

		/// <summary>
		/// perform an update with parameters
		/// </summary>
		/// <param name="sqlCommand"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static int UpdateWith( string sqlCommand, params KeyValuePair<string, object>[] args ) {
			return Transaction<int>( ( Database db, DbTransaction trans ) => {
				return UpdateWith( db, trans, sqlCommand, args );
			} );
		}

		/// <summary>
		/// insert with the passed transaction and get the last_insert_id value associated with that insert
		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <returns></returns>
		public static Int64 InsertIdForUpdate( Database db, DbTransaction trans, string sqlCommand ) {
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				dbCommand.Transaction = trans;
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				db.ExecuteNonQuery( dbCommand, trans );

				using( DbCommand dbCommandLastInsert = db.GetSqlStringCommand( "select last_insert_id()" ) ) {
					object d = db.ExecuteScalar( dbCommandLastInsert, trans );
					if( d == null ) {
						throw new InvalidDataException( "no last_insert_id() found for: \"" + sqlCommand + "\"" );
					}
					return (Int64)Convert.ChangeType( d, typeof( Int64 ) );
				}
			}
		}

		/// <summary>
		/// insert and get the last_insert_id value associated with that insert
		/// </summary>
		/// <param name="sqlCommand"></param>
		/// <returns></returns>
		public static Int64 InsertIdForUpdate( string sqlCommand ) {
			return Transaction<Int64>( ( Database db, DbTransaction trans ) => {
				return InsertIdForUpdate( db, trans, sqlCommand );
			} );
		}

		/// <summary>
		/// insert with the passed transaction with parameters
		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static Int64 InsertIdForUpdateWith( Database db, DbTransaction trans, string sqlCommand, params KeyValuePair<string, object>[] args ) {
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				dbCommand.Transaction = trans;
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				AddInParameters( db, dbCommand, args );
				db.ExecuteNonQuery( dbCommand, trans );
				dbCommand.Parameters.Clear();

				using( DbCommand dbCommandLastInsert = db.GetSqlStringCommand( "select last_insert_id()" ) ) {
					object d = db.ExecuteScalar( dbCommandLastInsert, trans );
					if( d == null ) {
						string parms = "";
						for( int i = 0; i < args.Length; ++i ) {
							if( i > 0 ) {
								parms += ", ";
							}
							parms += args[ i ].Key + "=" + args[ i ].Value;
						}
						throw new InvalidDataException( "no last_insert_id() found for: \"" + sqlCommand + "\" with " + args.Length + " parameters: " + parms );
					}
					return (Int64)Convert.ChangeType( d, typeof( Int64 ) );
				}
			}
		}

		/// <summary>
		/// insert with the passed transaction getting the last_insert_id associated with the insert, using parameters and optional parameter processing
		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static Int64 InsertIdForUpdateOptionWith( Database db, DbTransaction trans,
			                string sqlCommand, DelDatabaseOptionTask act, params KeyValuePair<string, object>[] args ) {
			using( DbCommand dbCommand = db.GetSqlStringCommand( sqlCommand ) ) {
				dbCommand.Transaction = trans;
				dbCommand.CommandTimeout = DatabaseCommandTimeout;
				AddInParameters( db, dbCommand, args );
				act( db, dbCommand );
				db.ExecuteNonQuery( dbCommand, trans );
				using( DbCommand dbCommandLastInsert = db.GetSqlStringCommand( "select last_insert_id()" ) ) {
					object d = db.ExecuteScalar( dbCommandLastInsert, trans );
					if( d == null ) {
						string parms = "";
						for( int i = 0; i < args.Length; ++i ) {
							if( i > 0 ) {
								parms += ", ";
							}
							parms += args[ i ].Key + "=" + args[ i ].Value;
						}
						throw new InvalidDataException( "no last_insert_id() found for: \"" + sqlCommand + "\" with " + args.Length + " parameters: " + parms );
					}
					return (Int64)Convert.ChangeType( d, typeof( Int64 ) );
				}
			}
		}

		/// <summary>
		/// insert and get the last_insert_id with parameters
		/// </summary>
		/// <param name="sqlCommand"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static Int64 InsertIdForUpdateWith( string sqlCommand, params KeyValuePair<string, object>[] args ) {
			return Transaction<Int64>( ( Database db, DbTransaction trans ) => {
				return InsertIdForUpdateWith( db, trans, sqlCommand, args );
			} );
		}

		/// <summary>
		/// insert and get the last_insert_id with parameters and optional processing.
		/// </summary>
		/// <param name="sqlCommand"></param>
		/// <param name="act"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static Int64 InsertIdForUpdateOptionWith( string sqlCommand, DelDatabaseOptionTask act, params KeyValuePair<string, object>[] args ) {
			return Transaction<Int64>( ( Database db, DbTransaction trans ) => {
				return InsertIdForUpdateOptionWith( db, trans, sqlCommand, act, args );
			} );
		}

		/// <summary>
		/// execute mysql script that may contain client specific commands, such as Delimiter
		/// </summary>
		/// <param name="db"></param>
		/// <param name="trans"></param>
		/// <param name="sqlCommand"></param>
		/// <param name="args"></param>
		/// <returns></returns>
		public static int Script(Database db, DbTransaction trans, string sqlCommand)
		{
			/*
			 *  Not sure how to use existing transaction with this script 
			 */ 
            using (MySqlConnection conn = new MySqlConnection(db.ConnectionString))
            {
                conn.Open();
                try
                {
                    MySqlScript script = new MySqlScript(conn, sqlCommand);
                    script.Execute();
                } catch (Exception ex)
                {
                    log.severe(ex);
                }
                
            }
			return 0;
		}

		public static void CopyRowColumnsToObject(DataSet ds, DataRow row, Newtonsoft.Json.Linq.JObject joReadType)
		{
			foreach (DataColumn dc in ds.Tables[0].Columns)
			{
				string col = dc.ColumnName;
				joReadType[col] = row[col].ToString();
			}
		}

		public static void CopyRowColumnsToArray(DataSet ds, Newtonsoft.Json.Linq.JArray joReadTypes)
		{
			AcrossDataSet(ds, (row) =>
			{
				Newtonsoft.Json.Linq.JObject joReadType = new Newtonsoft.Json.Linq.JObject();
				foreach (DataColumn dc in ds.Tables[0].Columns)
				{
					string col = dc.ColumnName;
					joReadType[col] = row[col].ToString();
				}
				joReadTypes.Add(joReadType);
			});
		}

		public delegate object ColumnValueFilter(int idx, string name, object value);
		public static void CopyRowColumnsToArrayFiltered(DataSet ds, Newtonsoft.Json.Linq.JArray joReadTypes, ColumnValueFilter filt)
		{
			AcrossDataSet(ds, (row, idx, cnt) =>
			{
				Newtonsoft.Json.Linq.JObject joReadType = new Newtonsoft.Json.Linq.JObject();
				foreach (DataColumn dc in ds.Tables[0].Columns)
				{
					string col = dc.ColumnName;
					joReadType[col] = filt(idx, col, row[col]).ToString();
				}
				joReadTypes.Add(joReadType);
			});
		}

		public delegate object ConditionalColumnValueFilter(int idx, string name, object value, out bool skipRow);
		public static void CopyRowColumnsToArrayFiltered(DataSet ds, Newtonsoft.Json.Linq.JArray joReadTypes, ConditionalColumnValueFilter filt)
		{
			AcrossDataSet(ds, (row, idx, cnt) =>
			{
				Newtonsoft.Json.Linq.JObject joReadType = new Newtonsoft.Json.Linq.JObject();
				bool skipRow = false;
				foreach (DataColumn dc in ds.Tables[0].Columns)
				{
					string col = dc.ColumnName;
					joReadType[col] = filt(idx, col, row[col], out skipRow).ToString();
					if (skipRow) break;
				}
				if (!skipRow)
					joReadTypes.Add(joReadType);
			});
		}

		public static Dictionary<string, object> CopyRowColumnsToDictionary(DataSet ds, DataRow row)
		{
			Dictionary<string, object> dictRowResult = new Dictionary<string, object>();
			CopyRowColumnsToDictionary(ds, row, dictRowResult);
			return dictRowResult;
		}

		public static void CopyRowColumnsToDictionary(DataSet ds, DataRow row, Dictionary<string,object> dictRowResult)
		{
			foreach (DataColumn dc in ds.Tables[0].Columns)
			{
				string col = dc.ColumnName;
				dictRowResult.Add(col, row[col].ToString());
			}
		}

		public static void CopyRowColumnsToDictionaryFiltered(DataSet ds, Dictionary<string, object> dictResult, string KeyColumn, ColumnValueFilter filt)
		{
			AcrossDataSet(ds, (row, idx, cnt) =>
			{
				Dictionary<string, object> dictRowResult = new Dictionary<string, object>();
				foreach (DataColumn dc in ds.Tables[0].Columns)
				{
					string col = dc.ColumnName;
					dictRowResult.Add(col,filt(idx, col, row[col]).ToString());
				}
				dictResult.Add(row[KeyColumn].ToString(), dictRowResult);
			});
		}

		public static void CopyRowColumnsToDictionaryFiltered(DataSet ds, Dictionary<string, object> dictResult, string KeyColumn, ConditionalColumnValueFilter filt)
		{
			AcrossDataSet(ds, (row, idx, cnt) =>
			{
				Dictionary<string, object> dictRowResult = new Dictionary<string, object>();
				bool skipRow = false;
				foreach (DataColumn dc in ds.Tables[0].Columns)
				{
					string col = dc.ColumnName;
					dictRowResult.Add(col, filt(idx, col, row[col], out skipRow).ToString());
					if (skipRow) break;
				}
				if (!skipRow)
					dictResult.Add(row[KeyColumn].ToString(), dictRowResult);
			});
		}


		public static Dictionary<string, List<string>> DataSetToKeyedList( DataSet dataSet, string key, string valCol  ) {
			Dictionary<string,List<string>> res = new Dictionary<string, List<string>>();
			Dictionary<string,string> last = new Dictionary<string, string>();
			AcrossDataSet( dataSet, ( row ) => {
				Dictionary<string,string> cur = new Dictionary<string, string>();
				string colVal = row[ key ].ToString();
				if( res.ContainsKey( colVal ) == false ) {
					res[colVal] = new List<string>();
				}
				res[ colVal ].Add( row[ valCol ].ToString() );
			});
			return res;
		}


		protected static bool ReplaceOldIndexWithNewIndexIfNotExists( Database db, DbTransaction trans, string oldIndexName, string tableName, IndexType indexType, params string[] keys ) {
			
			string typestr = indexType == IndexType.INDEX_UNIQUE ? "unique" : "";
			string prefstr = indexType == IndexType.INDEX_UNIQUE ? "UNQ" : "IDX";
			string fieldstr = "";
			string fieldlist = "";
			foreach( string fld in keys ) {
				if( fieldstr.Length > 0 )
					fieldstr += "_";
				if( fieldlist.Length > 0 )
					fieldlist += ",";
				fieldstr += fld.Split(' ')[ 0 ].Replace("`", "");
				fieldlist += fld;
			}

			string indexname = prefstr + "_" + tableName + "_" + fieldstr;

			int haveOldIndex = DataSetActionWith(db, trans,
				@"SHOW INDEX FROM "+tableName+" WHERE KEY_NAME = @indexName",
				KV("indexName", oldIndexName)).Tables[ 0 ].Rows.Count;

			int haveIndex = DataSetActionWith(db, trans,
				@"SHOW INDEX FROM "+tableName+" WHERE KEY_NAME = @indexName",
				KV("indexName", indexname)).Tables[ 0 ].Rows.Count;

			string sql = @"CREATE " + typestr + " INDEX "+
					indexname+" ON "+tableName+"("+fieldlist+");";

			if( haveOldIndex == 0 ) {
				log.info( "No longer have index for {0}", oldIndexName );
				if( haveIndex == 0 ) {
					log.info("Creating new index: {0}", sql);
					Update(db, trans, sql);
					return true;
				}
			} else {
				sql = @"ALTER TABLE `" +tableName+@"`
								drop index `"+oldIndexName+@"`,
						ADD "+typestr+@" index "+indexname+@"("+fieldlist+");";
				log.info( "Dropping index {0} and creating new index: {1}", oldIndexName, sql  );
				Update( db, trans, sql );
				return true;
			}

			log.info("Already have index for {0}", indexname);
			return false;
		}
	}

	/// <summary>
	/// This class wraps a number of boolean settings which control which parts of
	/// existing readtype definitions get overwritten by the readtype manager/services
	/// through the DAOs.  Additional settings may be required for certain circumstances.
	/// </summary>
	public class ReadtypeOverrideSettings : Dictionary<string, bool> {
		/// <summary>
		///  Name for title setting
		/// </summary>
		public string OverrideTitle { get { return "title"; } }
		/// <summary>
		/// Name for scanRate setting
		/// </summary>
		public string OverrideScanRate { get { return "scanRate"; } }
		/// <summary>
		/// Name for scanTime setting
		/// </summary>
		public string OverrideScanTime { get { return "scanTime"; } }
		/// <summary>
		/// Name for longDescription settting
		/// </summary>
		public string OverrideLongDescription { get { return "longDescription"; } }
		/// <summary>
		/// Name for isActive setting
		/// </summary>
		public string OverrideActive { get { return "isActive"; } }
		/// <summary>
		/// Name for scaleFactor setting
		/// </summary>
		public string OverrideScaleFactor { get { return "scaleFactor"; } }
		/// <summary>
		/// Name for deadBand setting
		/// </summary>
		public string OverrideDeadBand { get { return "deadBand"; } }

		/// <summary>
		/// Checks if value is present and returns value if so, false if not present.
		/// </summary>
		/// <param name="name">One of the Override* string names provided by this class, or
		/// another appropriate property name.</param>
		/// <returns></returns>
		public bool For( string name ) {
			return ContainsKey( name ) ? this[ name ] : false;
		}
	}
}