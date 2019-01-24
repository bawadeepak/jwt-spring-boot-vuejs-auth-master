package au.gov.acic.dp.common.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;
import org.springframework.util.StopWatch;

import au.gov.acic.dp.common.HbaseConfigProperties;
import au.gov.acic.dp.common.controller.exception.EntityNotFoundException;
import au.gov.acic.dp.common.mapper.format.Format;
import au.gov.acic.dp.common.mapper.format.InputDataHbase;
import au.gov.acic.dp.common.mapper.impl.HbaseToJSONMapper;
import au.gov.acic.dp.common.model.pole.Pole;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

/**
 * Central class for accessing the HBase API. Simplifies the use of HBase and helps to avoid common errors.
 * It executes core HBase workflow, leaving application code to invoke actions and extract results.
 *
 * @reference https://github.com/startup-ml/spring-boot-starter-hbase
 */
public class HbaseTemplate implements HbaseOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseTemplate.class);
    
    private static final String RELATIONSHIPS = "relationships";
    private static final String TYPE = "type";
    private static final String KEY = "key";
    private static final String RELATIONSHIP_TYPE = "relationshipType";

//    private Configuration configuration;

    private Connection connection;
    private HbaseConfigProperties hbaseConfigProperties;
    private String hbaseTable;

	@Autowired
	private HbaseToJSONMapper hbaseToJSONMapper;

    public HbaseTemplate(@NotNull HbaseConfigProperties hbaseConfigProperties, @NotNull String hbaseTable) {
		setHbaseConfigProperties(hbaseConfigProperties);
    	setHbaseTable(hbaseTable);
	}
	
    @Override
	public Map<String, String> retrieveHbaseRow(String rowKey) {
    	return retrieveHbaseRow(this.hbaseTable, rowKey);
	}

	@Override
	public Map<String, String> retrieveHbaseRow(String tableName, String rowKey) {
		return get(tableName, rowKey, new RowMapper<Map<String, String>>() {
			@Override
			public Map<String, String> mapRow(Result result, int rowNum) throws Exception {
				Map<String, String> rowData = new HashMap<>();
				if (result.getMap() != null && !result.getMap().isEmpty()) {
					result.getMap().entrySet().stream().forEach(columnFamily -> {
							NavigableMap<byte[], NavigableMap<Long, byte[]>> columnQualifiers = columnFamily.getValue();
							if (columnQualifiers!=null && !columnQualifiers.isEmpty()) {
								columnQualifiers.entrySet().stream().forEach(columnQualifier -> {
									NavigableMap<Long, byte[]> columnQualifierValues = columnQualifier.getValue();
									columnQualifierValues.entrySet().stream().forEach(cellData -> {
										rowData.put(Bytes.toString(columnQualifier.getKey()), Bytes.toString(cellData.getValue()));
									});
								});
							}
						}
					);
				}
				return rowData;
			}
		});
	}

	@Override
    public <T> T execute(String tableName, TableCallback<T> action) {
        Assert.notNull(action, "Callback object must not be null");
        Assert.notNull(tableName, "No table specified");

        StopWatch sw = new StopWatch();
        sw.start();
        Table table = null;
        try {
            table = this.getConnection().getTable(TableName.valueOf(tableName));
            return action.doInTable(table);
        } catch (Throwable throwable) {
            throw new HbaseSystemException(throwable);
        } finally {
            if (null != table) {
                try {
                    table.close();
                    sw.stop();
                } catch (IOException e) {
                    LOGGER.error("Hbase exception: {}", e.getMessage());
                }
            }
        }
    }

    @Override
    public <T> List<T> find(String tableName, String family, final RowMapper<T> action) {
        Scan scan = new Scan();
        scan.setCaching(5000);
        scan.addFamily(Bytes.toBytes(family));
        return this.find(tableName, scan, action);
    }

    @Override
    public <T> List<T> find(String tableName, String family, String qualifier, final RowMapper<T> action) {
        Scan scan = new Scan();
        scan.setCaching(5000);
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        return this.find(tableName, scan, action);
    }

    @Override
    public <T> List<T> find(String tableName, final Scan scan, final RowMapper<T> action) {
        return this.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(Table table) throws Throwable {
                int caching = scan.getCaching();
                if (caching == 1) {
                    scan.setCaching(5000);
                }
                ResultScanner scanner = table.getScanner(scan);
                try {
                    List<T> rs = new ArrayList<T>();
                    int rowNum = 0;
                    for (Result result : scanner) {
                        rs.add(action.mapRow(result, rowNum++));
                    }
                    return rs;
                } finally {
                    scanner.close();
                }
            }
        });
    }

    @Override
    public <T> T get(String tableName, String rowName, final RowMapper<T> mapper) {
        return this.get(tableName, rowName, null, null, mapper);
    }

    @Override
    public <T> T get(String tableName, String rowName, String familyName, final RowMapper<T> mapper) {
        return this.get(tableName, rowName, familyName, null, mapper);
    }

    @Override
    public <T> T get(String tableName, final String rowName, final String familyName, final String qualifier, final RowMapper<T> mapper) {
        return this.execute(tableName, new TableCallback<T>() {
            @Override
            public T doInTable(Table table) throws Throwable {
                Get get = new Get(Bytes.toBytes(rowName));
                if (StringUtils.isNotBlank(familyName)) {
                    byte[] family = Bytes.toBytes(familyName);
                    if (StringUtils.isNotBlank(qualifier)) {
                        get.addColumn(family, Bytes.toBytes(qualifier));
                    }
                    else {
                        get.addFamily(family);
                    }
                }
                Result result = table.get(get);
                return mapper.mapRow(result, 0);
            }
        });
    }

    @Override
    public void execute(String tableName, MutatorCallback action) {
        Assert.notNull(action, "Callback object must not be null");
        Assert.notNull(tableName, "No table specified");

        StopWatch sw = new StopWatch();
        sw.start();
        BufferedMutator mutator = null;
        try {
            BufferedMutatorParams mutatorParams = new BufferedMutatorParams(TableName.valueOf(tableName));
            mutator = this.getConnection().getBufferedMutator(mutatorParams.writeBufferSize(3 * 1024 * 1024));
            action.doInMutator(mutator);
        } catch (Throwable throwable) {
            sw.stop();
            throw new HbaseSystemException(throwable);
        } finally {
            if (null != mutator) {
                try {
                    mutator.flush();
                    mutator.close();
                    sw.stop();
                } catch (IOException e) {
                    LOGGER.error("hbase mutator error: {}", e.getMessage());
                }
            }
        }
    }

    @Override
    public void saveOrUpdate(String tableName, final Mutation mutation) {
        this.execute(tableName, new MutatorCallback() {
            @Override
            public void doInMutator(BufferedMutator mutator) throws Throwable {
                mutator.mutate(mutation);
            }
        });
    }

    @Override
    public void saveOrUpdates(String tableName, final List<Mutation> mutations) {
        this.execute(tableName, new MutatorCallback() {
            @Override
            public void doInMutator(BufferedMutator mutator) throws Throwable {
                mutator.mutate(mutations);
            }
        });
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Connection getConnection() {
        if (null == this.connection) {
            synchronized (this) {
                if (null == this.connection) {
                    try {
                		Configuration configuration = HBaseConfiguration.create();
                		configuration.set("dfs.domain.socket.path", hbaseConfigProperties.getDfsDomainSocketPath());
                		configuration.set("hbase.bulkload.staging.dir", hbaseConfigProperties.getBulkloadStagingDir());
                		configuration.set("hbase.zookeeper.quorum", hbaseConfigProperties.getZookeeperQuorum());
                		configuration.set("hbase.local.dir", hbaseConfigProperties.getLocalDir());
                		configuration.set("hbase.rootdir", hbaseConfigProperties.getRootDir());
                		configuration.set("hbase.tmp.dir", hbaseConfigProperties.getTmpDir());
                		configuration.set("zookeeper.znode.parent", hbaseConfigProperties.getZnodeParent());
                		try {
                			HBaseAdmin.available(configuration);
                		} catch (MasterNotRunningException e) {
                			LOGGER.debug("HBase is not running: {}", e.getMessage());
                		}
                        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(200, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
                        // init pool
                        poolExecutor.prestartCoreThread();
                        this.connection = ConnectionFactory.createConnection(configuration, poolExecutor);
                    } catch (IOException e) {
                        LOGGER.error("Hbase connection error: {}", e.getMessage());
                    }
                }
            }
        }
        return this.connection;
    }

	public String getHbaseTable() {
		return this.hbaseTable;
	}

	public void setHbaseTable(String hbaseTable) {
		this.hbaseTable = hbaseTable;
	}

	@Override
	public JSONObject convertHbaseRowDataToJSON(String rowKey, Format outputFormat) {
		return convertHbaseRowDataToJSON(rowKey, outputFormat, new ArrayList<String>());
	}

	@Override
	public JSONObject convertHbaseRowDataToJSON(String rowKey, Format outputFormat, List<String> alreadyProcessedRelations) {
		InputDataHbase inputDataHbase = new InputDataHbase();
		Map<String, String> hbaseRowData = retrieveHbaseRow(rowKey);
		if (Objects.isNull(hbaseRowData) || hbaseRowData.isEmpty()) {
			throw new EntityNotFoundException("Record does not exist or is not accessible.");
		}
		LOGGER.debug("Data for rowKey: {} found", rowKey);
		inputDataHbase.setRowData(hbaseRowData);
		hbaseToJSONMapper.setOutputFormat(outputFormat);
		JSONObject jsonObject = hbaseToJSONMapper.convert(inputDataHbase);
		handleRelationships(jsonObject, alreadyProcessedRelations);
		return jsonObject;
	}

	private void handleRelationships(JSONObject jsonObject, List<String> alreadyProcessedRelations) {
		Map<String, JSONObject> relationshipsArrayMap = new HashMap<>();
		if (jsonObject.containsKey(RELATIONSHIPS)) { // Check if any relationships exist for this POLE item
			JSONObject relationships = (JSONObject) jsonObject.get(RELATIONSHIPS);
			for (Map.Entry<String, Object> entry : relationships.entrySet()) { // Loop through all the relationships
				LOGGER.debug("Relationship: {}", entry.getKey());
				JSONObject relationshipsDetails = new JSONObject(); // Each relationship type can have one or more individual relationship items
				relationshipsArrayMap.put(entry.getKey(), relationshipsDetails);
				JSONArray relationshipDetails = (JSONArray) entry.getValue();
				Map<String, JSONArray> relationshipsSubTypeArrayMap = prepareRelationshipsSubTypeArrayMap(entry, relationshipDetails, alreadyProcessedRelations);
				for (Map.Entry<String, JSONArray> subtypeArray : relationshipsSubTypeArrayMap.entrySet()) { 
					relationshipsDetails.put(subtypeArray.getKey(), subtypeArray.getValue());
				}
			}
			jsonObject.remove(RELATIONSHIPS); // Remove relationships key/type information as expanded data retrieval from HBase has already been done 
			for (Map.Entry<String, JSONObject> subTypeArray : relationshipsArrayMap.entrySet()) { // Loop through all the relationships data details and store in top level object
				jsonObject.put(subTypeArray.getKey(), subTypeArray.getValue());
			}
		}
	}

	private Map<String, JSONArray> prepareRelationshipsSubTypeArrayMap(Map.Entry<String, Object> entry, JSONArray relationshipDetails, List<String> alreadyProcessedRelations) {
		Map<String, JSONArray> relationshipsSubTypeArrayMap = new HashMap<>();
		for(Object relationshipDetail : relationshipDetails) { // Iterate through each of the relationships for current relationship type
			JSONObject detail = (JSONObject) relationshipDetail;
			if (StringUtils.isNotEmpty(detail.getAsString(KEY))) { // If key exists then retrieve data from HBase using the key value as HBase row key
				JSONObject rdJSONObject = convertHbaseRowDataToJSON(detail.getAsString(KEY), new Format() {
					@Override
					public String getFormatKey() {
						return entry.getKey().toUpperCase();
					}
				}, alreadyProcessedRelations);
				if (Objects.nonNull(rdJSONObject)) { // If JSON Object is returned from HBase, store its details into relationship JSONArray
					JSONObject relationshipJSONObject = null;
					if (rdJSONObject.size() >= 1) { // Check that at least one JSON Element is returned by HBase
						relationshipJSONObject = getFirstJSONObjectElement(rdJSONObject, entry.getKey().toUpperCase()); // There should be only one top level JSON element for the data returned by HBase
					} else { // If no JSON Element has been returned then initialize empty JSONObject that will only be storing relationship type with no other details
						relationshipJSONObject = new JSONObject();
					}
					if (Objects.nonNull(relationshipJSONObject)) {
						LOGGER.debug("EntitySubtype: {}", relationshipJSONObject.getAsString(Pole.MetadataField.ENTITY_SUBTYPE.value()));
						if (Objects.nonNull(relationshipJSONObject.getAsString(Pole.MetadataField.ENTITY_SUBTYPE.value()))) {
							if (!relationshipsSubTypeArrayMap.containsKey(relationshipJSONObject.getAsString(Pole.MetadataField.ENTITY_SUBTYPE.value()))) {
								JSONArray subTypeArray = new JSONArray();
								relationshipsSubTypeArrayMap.put(relationshipJSONObject.getAsString(Pole.MetadataField.ENTITY_SUBTYPE.value()), subTypeArray);
							}
							relationshipJSONObject.put(RELATIONSHIP_TYPE, detail.getAsString(TYPE));
							relationshipsSubTypeArrayMap.get(relationshipJSONObject.getAsString(Pole.MetadataField.ENTITY_SUBTYPE.value())).add(rdJSONObject);
						}
					}
				}
			}
		}
		return relationshipsSubTypeArrayMap;
	}

	private static JSONObject getFirstJSONObjectElement(JSONObject rdJSONObject, String formatType) {
		for (Entry<String, Object> entry : rdJSONObject.entrySet()) {
			if (entry.getKey().equalsIgnoreCase(formatType)) {
				return (JSONObject) entry.getValue();
			}
		}
		return null;
	}

	public HbaseConfigProperties getHbaseConfigProperties() {
		return hbaseConfigProperties;
	}

	public void setHbaseConfigProperties(HbaseConfigProperties hbaseConfigProperties) {
		this.hbaseConfigProperties = hbaseConfigProperties;
	}

//    public Configuration getConfiguration() {
//        return configuration;
//    }
//
//    public void setConfiguration(Configuration configuration) {
//        this.configuration = configuration;
//    }

}
