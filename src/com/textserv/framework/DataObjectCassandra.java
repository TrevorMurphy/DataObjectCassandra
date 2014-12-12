package com.textserv.framework;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;

public class DataObjectCassandra {
	public static String[] metaDataCols = {"MetaData" };

	public static DataObject readAllNoMeta( Keyspace keyspace, String columnFamily, String key) throws DataObjectException {
		StringSerializer se = StringSerializer.get();
		SliceQuery<String, String, String> innery_query = HFactory.createSliceQuery(keyspace, se, se, se);
		innery_query.setColumnFamily(columnFamily).setKey(key).setRange("","",false,Integer.MAX_VALUE);
		QueryResult<ColumnSlice<String, String>> inner_result = innery_query.execute();
		DataObject object = null;

		ColumnSlice<String, String> inner_col_slice = inner_result.get();
		if (inner_col_slice.getColumns().size() > 0) {
			object = new DataObject();
			List<HColumn<String, String>> columns = inner_col_slice.getColumns();
			for ( HColumn<String, String> column : columns) {
				object.setFromString(column.getName(), column.getValue());
			}
		}
		return object;
	}
	
	public static DataObject read( Keyspace keyspace, String columnFamily, String key) throws DataObjectException {
		StringSerializer se = StringSerializer.get();
		SliceQuery<String, String, String> meta_query = HFactory.createSliceQuery(keyspace, se, se, se);
		meta_query.setColumnFamily(columnFamily).setKey(key).setColumnNames(DataObjectCassandra.metaDataCols);
		QueryResult<ColumnSlice<String, String>> meta_result = meta_query.execute();
		ColumnSlice<String, String> meta_slice = meta_result.get();
		
		DataObject object = null;
		
		if (meta_slice.getColumns().size() > 0) {
			DataObject metaData = new DataObject();
			metaData.fromStringEncoded(meta_slice.getColumnByName("MetaData").getValue());

			SliceQuery<String, String, String> innery_query = HFactory.createSliceQuery(keyspace, se, se, se); 
			innery_query.setColumnFamily(columnFamily).setKey(key).setColumnNames(metaData.keySet().toArray(new String[0]));
			QueryResult<ColumnSlice<String, String>> inner_result = innery_query.execute();
			ColumnSlice<String, String> inner_col_slice = inner_result.get();
			if (inner_col_slice.getColumns().size() > 0) {
				object = new DataObject();
				List<HColumn<String, String>> columns = inner_col_slice.getColumns();
				for ( HColumn<String, String> column : columns) {
					object.setFromString(column.getName(), column.getValue());
				}
				object.setDataObject("MetaData", metaData, false);
			}
		}
		return object;
	}

	public static DataObject read( Keyspace keyspace, String columnFamily, String key, DataObject object, String prefix) throws DataObjectException {
		StringSerializer se = StringSerializer.get();
		SliceQuery<String, String, String> meta_query = HFactory.createSliceQuery(keyspace, se, se, se);
		meta_query.setColumnFamily(columnFamily).setKey(key).setColumnNames(DataObjectCassandra.metaDataCols);
		QueryResult<ColumnSlice<String, String>> meta_result = meta_query.execute();
		ColumnSlice<String, String> meta_slice = meta_result.get();
		
		DataObject mergeMeta = object.getDataObject("MetaData");
		if (meta_slice.getColumns().size() > 0) {
			DataObject metaData = new DataObject();
			metaData.fromStringEncoded(meta_slice.getColumnByName("MetaData").getValue());

			SliceQuery<String, String, String> innery_query = HFactory.createSliceQuery(keyspace, se, se, se); 
			innery_query.setColumnFamily(columnFamily).setKey(key).setColumnNames(metaData.keySet().toArray(new String[0]));
			QueryResult<ColumnSlice<String, String>> inner_result = innery_query.execute();
			ColumnSlice<String, String> inner_col_slice = inner_result.get();
			if (inner_col_slice.getColumns().size() > 0) {
				object = new DataObject();
				List<HColumn<String, String>> columns = inner_col_slice.getColumns();
				for ( HColumn<String, String> column : columns) {
					object.setFromString(column.getName(), column.getValue());
				}
				mergeMeta.merge(metaData, false, prefix);
			}
		}
		return object;
	}

	//returns a first page DataObject with more flag, results dataobject list and new end date for next query
	public DataObject timeSeriesFindFirstPage(Keyspace keyspace, String timeSeriesColumnFamily, String objectColumnFamily, String key, int pageSize) {
		DataObject page = new DataObject();
		
		List<DataObject> results = page.getDataObjectList("results", true);
		
		StringSerializer se = StringSerializer.get();
		UUIDSerializer ue = UUIDSerializer.get();
		SuperSliceQuery<String, UUID, String, String> timeseries_query = HFactory.createSuperSliceQuery(keyspace, se, ue, se, se);
		timeseries_query.setColumnFamily(timeSeriesColumnFamily).setKey(key).setRange(null, null, true, pageSize);
		QueryResult<SuperSlice<UUID, String, String>> timeseries_result = timeseries_query.execute();
		List<HSuperColumn<UUID, String, String>> superColumns = timeseries_result.get().getSuperColumns();
		if ( superColumns.size() < pageSize ) {
			page.setBoolean("more", false);
		} else {
			page.setBoolean("more", true);
		}
		UUID lastUUId = null;
		for ( HSuperColumn<UUID, String, String> superColumn : superColumns ) {
			//get the key column and lookup the transaction
			lastUUId = superColumn.getName();
			List<HColumn<String,String>> columns = superColumn.getColumns();
			HColumn<String,String> keyColumn = columns.get(0);
			String objectkey = keyColumn.getValue();
			DataObject object = DataObjectCassandra.read(keyspace, objectColumnFamily, objectkey);
			if ( object != null ) {
				results.add( object);
			}
		}
		if ( lastUUId != null ) {
			long lastEndDate = TimeUUIDUtils.getTimeFromUUID(lastUUId);
			page.setLong("nextEndDate", lastEndDate -1);
		}
		return page;
	}

	public DataObject timeSeriesFindNextPage(Keyspace keyspace, String timeSeriesColumnFamily, String objectColumnFamily, String key, long endTime, int pageSize) {
		DataObject page = new DataObject();
		
		List<DataObject> results = page.getDataObjectList("results", true);
		
		StringSerializer se = StringSerializer.get();
		UUIDSerializer ue = UUIDSerializer.get();
		SuperSliceQuery<String, UUID, String, String> timeseries_query = HFactory.createSuperSliceQuery(keyspace, se, ue, se, se);
		UUID endID = TimeUUIDUtils.getTimeUUID(endTime);
		timeseries_query.setColumnFamily(timeSeriesColumnFamily).setKey(key).setRange(endID, null, true, pageSize);
		QueryResult<SuperSlice<UUID, String, String>> timeseries_result = timeseries_query.execute();
		List<HSuperColumn<UUID, String, String>> superColumns = timeseries_result.get().getSuperColumns();
		if ( superColumns.size() < pageSize ) {
			page.setBoolean("more", false);
		} else {
			page.setBoolean("more", true);
		}
		UUID lastUUId = null;
		for ( HSuperColumn<UUID, String, String> superColumn : superColumns ) {
			//get the key column and lookup the transaction
			lastUUId = superColumn.getName();
			List<HColumn<String,String>> columns = superColumn.getColumns();
			HColumn<String,String> keyColumn = columns.get(0);
			String objectkey = keyColumn.getValue();
			DataObject object = DataObjectCassandra.read(keyspace, objectColumnFamily, objectkey);
			if ( object != null ) {
				results.add( object);
			}
		}
		if ( lastUUId != null ) {
			long lastEndDate = TimeUUIDUtils.getTimeFromUUID(lastUUId);
			page.setLong("nextEndDate", lastEndDate -1);
		}
		return page;
	}
	//returns a page DataObject with more flag, results dataobject list and new end date for next query
	public DataObject timeSeriesFindPage(Keyspace keyspace, String timeSeriesColumnFamily, String objectColumnFamily, String key, long startTime, long endTime, int pageSize) {
		DataObject page = new DataObject();
		
		List<DataObject> results = page.getDataObjectList("results", true);
		
		StringSerializer se = StringSerializer.get();
		UUIDSerializer ue = UUIDSerializer.get();
		SuperSliceQuery<String, UUID, String, String> timeseries_query = HFactory.createSuperSliceQuery(keyspace, se, ue, se, se);
		UUID startID = TimeUUIDUtils.getTimeUUID(startTime);
		UUID endID = TimeUUIDUtils.getTimeUUID(endTime);
		timeseries_query.setColumnFamily(timeSeriesColumnFamily).setKey(key).setRange(endID, startID, true, pageSize);
		QueryResult<SuperSlice<UUID, String, String>> timeseries_result = timeseries_query.execute();
		List<HSuperColumn<UUID, String, String>> superColumns = timeseries_result.get().getSuperColumns();
		if ( superColumns.size() < pageSize ) {
			page.setBoolean("more", false);
		} else {
			page.setBoolean("more", true);
		}
		UUID lastUUId = null;
		for ( HSuperColumn<UUID, String, String> superColumn : superColumns ) {
			//get the key column and lookup the transaction
			lastUUId = superColumn.getName();
			List<HColumn<String,String>> columns = superColumn.getColumns();
			HColumn<String,String> keyColumn = columns.get(0);
			String objectkey = keyColumn.getValue();
			DataObject object = DataObjectCassandra.read(keyspace, objectColumnFamily, objectkey);
			if ( object != null ) {
				results.add( object);
			}
		}
		if ( lastUUId != null ) {
			long lastEndDate = TimeUUIDUtils.getTimeFromUUID(lastUUId);
			page.setLong("nextEndDate", lastEndDate -1);
			page.setLong("nextStartDate", startTime);
		}
		return page;
	}
	
	public List<DataObject> timeSeriesFindAll(Keyspace keyspace, String timeSeriesColumnFamily, String objectColumnFamily, String key, long startTime, long endTime) {
		List<DataObject> results = new ArrayList<DataObject>();
		StringSerializer se = StringSerializer.get();
		UUIDSerializer ue = UUIDSerializer.get();
		SuperSliceQuery<String, UUID, String, String> timeseries_query = HFactory.createSuperSliceQuery(keyspace, se, ue, se, se);
		UUID startID = TimeUUIDUtils.getTimeUUID(startTime);
		UUID endID = TimeUUIDUtils.getTimeUUID(endTime);
		timeseries_query.setColumnFamily(timeSeriesColumnFamily).setKey(key).setRange(endID, startID, true, Integer.MAX_VALUE);
		QueryResult<SuperSlice<UUID, String, String>> timeseries_result = timeseries_query.execute();
		List<HSuperColumn<UUID, String, String>> superColumns = timeseries_result.get().getSuperColumns();
		for ( HSuperColumn<UUID, String, String> superColumn : superColumns ) {
			//get the key column and lookup the transaction
			List<HColumn<String,String>> columns = superColumn.getColumns();
			HColumn<String,String> keyColumn = columns.get(0);
			String objectkey = keyColumn.getValue();
			DataObject object = DataObjectCassandra.read(keyspace, objectColumnFamily, objectkey);
			if ( object != null ) {
				results.add( object);
			}
		}
		return results;
	}

	public static int timeSeriesCount( Keyspace keyspace, String timeSeriesColumnFamily, String key) {
		StringSerializer se = StringSerializer.get();
		UUIDSerializer ue = UUIDSerializer.get();
		SuperSliceQuery<String, UUID, String, String> timeseries_query = HFactory.createSuperSliceQuery(keyspace, se, ue, se, se);
		timeseries_query.setColumnFamily(timeSeriesColumnFamily).setKey(key).setRange(null, null, true, Integer.MAX_VALUE);
		QueryResult<SuperSlice<UUID, String, String>> timeseries_result = timeseries_query.execute();
		return timeseries_result.get().getSuperColumns().size();
	}

	public static int timeSeriesCount( Keyspace keyspace, String timeSeriesColumnFamily, String key, long startTime, long endTime) {
		StringSerializer se = StringSerializer.get();
		UUIDSerializer ue = UUIDSerializer.get();
		SuperSliceQuery<String, UUID, String, String> timeseries_query = HFactory.createSuperSliceQuery(keyspace, se, ue, se, se);
		UUID startID = TimeUUIDUtils.getTimeUUID(startTime);
		UUID endID = TimeUUIDUtils.getTimeUUID(endTime);
		timeseries_query.setColumnFamily(timeSeriesColumnFamily).setKey(key).setRange(endID, startID, true, Integer.MAX_VALUE);
		QueryResult<SuperSlice<UUID, String, String>> timeseries_result = timeseries_query.execute();
		return timeseries_result.get().getSuperColumns().size();
	}

	public static String write( Keyspace keyspace, String columnFamily, DataObject dataObject, int ttl) throws DataObjectException {
		StringSerializer se = StringSerializer.get();
		DataObject meta = dataObject.getDataObject("MetaData");
		if ( meta == null ) {
			throw new DataObjectException("Must pass a DataObject that contains MetaData");
		}
		Mutator<String> mutator = HFactory.createMutator(keyspace, se);
		String key = TimeUUIDUtils.getUniqueTimeUUIDinMillis().toString();
		HColumn<String, String> column = HFactory.createColumn("key", key, se, se);
		column.setTtl(ttl);
		mutator.addInsertion(key, columnFamily, column);
		column = HFactory.createColumn("MetaData", meta.toStringEncoded(), se, se);
		column.setTtl(ttl);
		mutator.addInsertion(key, columnFamily, column);
		for ( String metaKey : meta.keySet() ) {
			column = HFactory.createColumn(metaKey, dataObject.getAsString(metaKey), se, se);
			column.setTtl(ttl);
			mutator.addInsertion(key, columnFamily, column);			
		}
		mutator.execute();
		return key;
	}

	public static void write( Keyspace keyspace, String columnFamily, String key, DataObject dataObject, int ttl) throws DataObjectException  {
		StringSerializer se = StringSerializer.get();
		DataObject meta = dataObject.getDataObject("MetaData");
		if ( meta == null ) {
			throw new DataObjectException("Must pass a DataObject that contains MetaData");
		}
		Mutator<String> mutator = HFactory.createMutator(keyspace, se);
		HColumn<String, String> column = HFactory.createColumn("key", key, se, se);
		column.setTtl(ttl);
		mutator.addInsertion(key, columnFamily, column);
		column = HFactory.createColumn("MetaData", meta.toStringEncoded(), se, se);
		column.setTtl(ttl);
		mutator.addInsertion(key, columnFamily, column);
		for ( String metaKey : meta.keySet() ) {
			column = HFactory.createColumn(metaKey, dataObject.getAsString(metaKey), se, se);
			column.setTtl(ttl);
			mutator.addInsertion(key, columnFamily, column);			
		}
		mutator.execute();
	}

	public static void writeWithExistingMerge( Keyspace keyspace, String columnFamily, String key, DataObject dataObject, int ttl) throws DataObjectException  {
		DataObject existingObj = read(keyspace, columnFamily, key);
		DataObject old_object = null;
		if ( existingObj != null ) {
			old_object = existingObj.createCopy();
			dataObject.merge( existingObj, true);
		}		
		write(keyspace, columnFamily, key, dataObject, ttl);
		dataObject.setDataObject("old", old_object, false);
	}

	public static void writeTimeSeries( Keyspace keyspace, String columnFamily, String key, DataObject dataObject, Date date, int ttl) throws DataObjectException  {
		StringSerializer se = StringSerializer.get();
		UUIDSerializer ue = UUIDSerializer.get();
		DataObject meta = dataObject.getDataObject("MetaData");
		if ( meta == null ) {
			throw new DataObjectException("Must pass a DataObject that contains MetaData");
		}
		Mutator<String> mutator = HFactory.createMutator(keyspace, se);
		HColumn<String, String> column = HFactory.createColumn("key", key, se, se);
		column.setTtl(ttl);
		List<HColumn<String, String>> columns = new ArrayList<HColumn<String, String>>();
		columns.add(column);
		column = HFactory.createColumn("MetaData", meta.toStringEncoded(), se, se);
		column.setTtl(ttl);
		columns.add(column);
		for ( String metaKey : meta.keySet() ) {
			column = HFactory.createColumn(metaKey, dataObject.getAsString(metaKey), se, se);
			column.setTtl(ttl);
			columns.add(column);
		}
		mutator.addInsertion(key, columnFamily, HFactory.createSuperColumn(TimeUUIDUtils.getTimeUUID(date.getTime()), columns, ue, se, se));
		mutator.execute();
	}

	public static void writeTimeSeriesKey( Keyspace keyspace, String columnFamily, String key, String objectKey, Date date, int ttl) throws DataObjectException  {
		StringSerializer se = StringSerializer.get();
		UUIDSerializer ue = UUIDSerializer.get();
		Mutator<String> mutator = HFactory.createMutator(keyspace, se);
		HColumn<String, String> column = HFactory.createColumn("key", objectKey, se, se);
		column.setTtl(ttl);
		List<HColumn<String, String>> columns = new ArrayList<HColumn<String, String>>();
		columns.add(column);
		mutator.addInsertion(key, columnFamily, HFactory.createSuperColumn(TimeUUIDUtils.getTimeUUID(date.getTime()), columns, ue, se, se));
		mutator.execute();	
	}
}
