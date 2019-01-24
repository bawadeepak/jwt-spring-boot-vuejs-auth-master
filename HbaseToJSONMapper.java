package au.gov.acic.dp.common.mapper.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import au.gov.acic.dp.common.mapper.Mapper;
import au.gov.acic.dp.common.mapper.converter.Converter;
import au.gov.acic.dp.common.mapper.converter.ConverterLocator;
import au.gov.acic.dp.common.mapper.format.Format;
import au.gov.acic.dp.common.mapper.format.InputDataHbase;
import au.gov.acic.dp.common.mapper.repository.PropertiesRepository;
import au.gov.acic.dp.common.mapper.repository.model.JsonMappingDetails;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

@Component
public class HbaseToJSONMapper implements Mapper<InputDataHbase, JSONObject> {

    private static final Logger LOG = LoggerFactory.getLogger(HbaseToJSONMapper.class);
    
    private Format inputFormat;
    private Format outputFormat;

	@Autowired
	private PropertiesRepository propertiesRepository;

	@Override
	public JSONObject convert(InputDataHbase inputDataHbase) {
        if (inputDataHbase != null) {
      		JSONObject jsonData = new JSONObject();
    		Map<String, JSONObject> existingJSONObjects = new HashMap<String, JSONObject>();
    		Map<String, JSONArray> existingJSONArrays = new HashMap<String, JSONArray>();
    		Map<String, Map<String, JsonMappingDetails>> jsonMappingInformation = propertiesRepository.getJSONMappingInformation();
    		if (jsonMappingInformation != null) {
    			Map<String, JsonMappingDetails> personJsonMappingInformation = jsonMappingInformation.get(outputFormat.getFormatKey());
    			if (personJsonMappingInformation != null) {
	        		if (inputDataHbase.getRowData()!=null && !inputDataHbase.getRowData().isEmpty()) {
	    	    		inputDataHbase.getRowData().forEach((key, value)  -> {
	    	    			populateJSON(key, value, jsonData, existingJSONObjects, existingJSONArrays, personJsonMappingInformation);
	    	    		});
	        		}
    			}
        		LOG.debug("JSON Data: {}", jsonData.toJSONString());
        		LOG.debug("JSON Array: {}", existingJSONArrays);
        		return jsonData;
    		}
        }
		return null;
	}

	private void populateJSON(String key, String value, JSONObject outputJSONData, 
			Map<String, JSONObject> existingJSONObjects, Map<String, JSONArray> existingJSONArrays, Map<String, JsonMappingDetails> jsonMappingInformation) {
		// Starting from the first two elements, find if the elements are arrays or objects.
		// Split Data Key to get key portions. This is used to retrieve the JSON Mapping relevant for particular key.
		Map<Integer, Integer> arrayKeyPosition = new HashMap<Integer, Integer>();
		List<String> hbaseDataKeys = analyzeCurrentHbaseKey(key, arrayKeyPosition, jsonMappingInformation);
		processKeyData(hbaseDataKeys, arrayKeyPosition, value, outputJSONData, existingJSONObjects, existingJSONArrays, jsonMappingInformation);
	}

	@SuppressWarnings("static-method")
	private List<String> analyzeCurrentHbaseKey(String hbaseDataKey, Map<Integer, Integer> arrayKeyPosition, Map<String, JsonMappingDetails> jsonMappingInformation) {
		int j = 0;
		String[] hbaseDataKeySplits = hbaseDataKey.split("~");
		StringBuilder justHbaseDataKey = new StringBuilder("");
		List<String> hbaseDataKeys = new ArrayList<String>();
		for (int i = 0; i < hbaseDataKeySplits.length; i++) {
			if (isInteger(hbaseDataKeySplits[i])) {
				arrayKeyPosition.put(j, Integer.parseInt(hbaseDataKeySplits[i]));
				if (jsonMappingInformation.containsKey(justHbaseDataKey.toString())) {
					jsonMappingInformation.get(justHbaseDataKey.toString()).setMultipleValues(true);
				}
				j++;
			} else {
				if (i == 0) {
					justHbaseDataKey.append(hbaseDataKeySplits[i]);
				} else {
					justHbaseDataKey.append("~");
					justHbaseDataKey.append(hbaseDataKeySplits[i]);
				}
				hbaseDataKeys.add(justHbaseDataKey.toString());
			}
		}
		return hbaseDataKeys;
	}

	private void processKeyData(List<String> hbaseDataKeys, Map<Integer, Integer> arrayKeyPosition, String value, JSONObject outputJSONData, 
			Map<String, JSONObject> existingJSONObjects, Map<String, JSONArray> existingJSONArrays, Map<String, JsonMappingDetails> jsonMappingInformation) {
		for (int hbaseDateKeyPosition=0; hbaseDateKeyPosition < hbaseDataKeys.size(); hbaseDateKeyPosition++) {
			String hbaseDataKey = hbaseDataKeys.get(hbaseDateKeyPosition);
			if (jsonMappingInformation.containsKey(hbaseDataKey)) {
				AtomicInteger arrayPosition = new AtomicInteger();
				StringBuilder jsonObjKey = new StringBuilder("");
				StringBuilder arrayKeyAppender = new StringBuilder("");
				JsonMappingDetails jsonMapping = jsonMappingInformation.get(hbaseDataKey);
				if (jsonMapping != null) {
					String[] jsonPositionSplits = jsonMapping.getJsonDepth().split("[, ?.@]+");
					StringBuilder jsonMappingKey = new StringBuilder("");
					JSONObject prevObj = outputJSONData;
					for (int splitPosition=0; splitPosition < jsonPositionSplits.length; splitPosition++) {
						prevObj = processCurrentSplit(hbaseDataKeys, arrayKeyPosition, value,
								existingJSONObjects, existingJSONArrays, jsonMappingInformation, hbaseDateKeyPosition,
								arrayPosition, jsonObjKey, arrayKeyAppender, jsonPositionSplits, jsonMappingKey,
								prevObj, splitPosition);
					}
				}
			}
		}
	}

	private JSONObject processCurrentSplit(List<String> hbaseDataKeys, Map<Integer, Integer> arrayKeyPosition,
			String value, Map<String, JSONObject> existingJSONObjects, Map<String, JSONArray> existingJSONArrays,
			Map<String, JsonMappingDetails> jsonMappingInformation, int hbaseDateKeyPosition, AtomicInteger arrayPosition,
			StringBuilder jsonObjKey, StringBuilder arrayKeyAppender, String[] jsonPositionSplits,
			StringBuilder jsonMappingKey, JSONObject prevObj, int splitPosition) {
		jsonObjKey.append(jsonPositionSplits[splitPosition]);
		jsonMappingKey.append(jsonPositionSplits[splitPosition]);
		JsonMappingDetails jsonObjMapping = jsonMappingInformation.get(jsonMappingKey.toString());
		JSONObject currentJSONObject = null;
		if ((hbaseDateKeyPosition == (hbaseDataKeys.size()-1)) && (splitPosition == (jsonPositionSplits.length-1))) {
			if (jsonObjMapping != null) {
				storeValue(arrayKeyPosition, value, existingJSONArrays, prevObj, arrayPosition.get(),
						arrayKeyAppender, jsonPositionSplits, splitPosition, jsonObjMapping);
			}
		} else if (jsonObjMapping != null) {
			if (jsonObjMapping.isMultipleValues()) {
				currentJSONObject = handleJSONArrayTypeMapping(arrayKeyPosition, existingJSONObjects,
						existingJSONArrays, jsonObjKey, prevObj, arrayPosition.getAndIncrement(), arrayKeyAppender,
						jsonPositionSplits, splitPosition);
			} else {
				currentJSONObject = handleJSONObjectTypeMapping(existingJSONObjects, jsonObjKey,
						prevObj, jsonPositionSplits, splitPosition);
			}
		} else {
			currentJSONObject = handleJSONObjectTypeMapping(existingJSONObjects, jsonObjKey,
					prevObj, jsonPositionSplits, splitPosition);
		}
		jsonObjKey.append(".");
		jsonMappingKey.append(".");
		return currentJSONObject;
	}

	@SuppressWarnings({ "static-method" })
	private void storeValue(Map<Integer, Integer> arrayKeyPosition, String value,
			Map<String, JSONArray> existingJSONArrays, JSONObject prevObj, int arrayPosition,
			StringBuilder arrayKeyAppender, String[] jsonPositionSplits, int splitPosition, JsonMappingDetails jsonObjMapping) {
		Object convertedValue = value;
		if (StringUtils.isNotEmpty(jsonObjMapping.getConverter())) {
			Converter<String,?> converter = ConverterLocator.locateConverter(jsonObjMapping.getConverter());
			convertedValue = converter.convert(value);
		}
		if (jsonObjMapping.isMultipleValues()) {
			arrayKeyAppender.append(arrayKeyPosition.get(arrayPosition - 1));
			JSONArray jsonArray = null;
			if (existingJSONArrays.containsKey(createArrayUniqueKey(jsonPositionSplits, splitPosition) + arrayKeyAppender.toString())) {
				jsonArray = existingJSONArrays.get(createArrayUniqueKey(jsonPositionSplits, splitPosition) + arrayKeyAppender.toString());
			} else {
				jsonArray = new JSONArray();
				existingJSONArrays.put(createArrayUniqueKey(jsonPositionSplits, splitPosition) + arrayKeyAppender.toString(), jsonArray);
				prevObj.put(jsonPositionSplits[splitPosition], jsonArray);
			}
			jsonArray.add(convertedValue);
		} else {
			prevObj.put(jsonPositionSplits[splitPosition], convertedValue);
		}
	}

	@SuppressWarnings({ "static-method" })
	private JSONObject handleJSONObjectTypeMapping(Map<String, JSONObject> existingJSONObjects,
			StringBuilder jsonObjKey, JSONObject prevObj, String[] jsonPositionSplits, int splitPosition) {
		JSONObject jsonObject = null;
		if (!existingJSONObjects.containsKey(jsonObjKey.toString())) {
			jsonObject = new JSONObject();
			existingJSONObjects.put(jsonObjKey.toString(), jsonObject);
			prevObj.put(jsonPositionSplits[splitPosition], jsonObject);
		} else {
			jsonObject = existingJSONObjects.get(jsonObjKey.toString());
		}
		return jsonObject;
	}

	private JSONObject handleJSONArrayTypeMapping(Map<Integer, Integer> arrayKeyPosition,
			Map<String, JSONObject> existingJSONObjects, Map<String, JSONArray> existingJSONArrays,
			StringBuilder jsonObjKey, JSONObject prevObj, int arrayPosition, StringBuilder arrayKeyAppender,
			String[] jsonPositionSplits, int splitPosition) {
		JSONObject currentJSONObject;
		boolean arrayExists = findIfArrayAlreadyExists(arrayKeyPosition, existingJSONArrays, arrayPosition,
				arrayKeyAppender, jsonPositionSplits, splitPosition);
		if (arrayExists) {
			currentJSONObject = createOrRetrieveJSONArrayObject(arrayKeyPosition,
					existingJSONObjects, existingJSONArrays, jsonObjKey, arrayPosition,
					arrayKeyAppender, jsonPositionSplits, splitPosition);
		} else {
			currentJSONObject = createJSONArrayWithObject(arrayKeyPosition, existingJSONObjects,
					existingJSONArrays, jsonObjKey, prevObj, arrayPosition, arrayKeyAppender,
					jsonPositionSplits, splitPosition);
		}
		return currentJSONObject;
	}

	private JSONObject createJSONArrayWithObject(Map<Integer, Integer> arrayKeyPosition,
			Map<String, JSONObject> existingJSONObjects, Map<String, JSONArray> existingJSONArrays,
			StringBuilder jsonObjKey, JSONObject prevObj, int arrayPosition, StringBuilder arrayKeyAppender,
			String[] jsonPositionSplits, int splitPosition) {
		JSONObject currentJSONObject;
		JSONArray jsonArray;
		jsonArray = new JSONArray();
		storeArray(existingJSONArrays, arrayPosition, arrayKeyAppender, jsonPositionSplits, splitPosition,
				jsonArray);
		prevObj.put(jsonPositionSplits[splitPosition], jsonArray);
		jsonObjKey.append(arrayKeyPosition.get(arrayPosition));
		JSONObject jsonObject = new JSONObject();
		jsonArray.add(jsonObject);
		if (!existingJSONObjects.containsKey(jsonObjKey.toString())) {
			existingJSONObjects.put(jsonObjKey.toString(), jsonObject);
		} else {
			jsonObject = existingJSONObjects.get(jsonObjKey.toString());
		}
		currentJSONObject = jsonObject;
		return currentJSONObject;
	}

	private JSONObject createOrRetrieveJSONArrayObject(Map<Integer, Integer> arrayKeyPosition,
			Map<String, JSONObject> existingJSONObjects, Map<String, JSONArray> existingJSONArrays,
			StringBuilder jsonObjKey, int arrayPosition, StringBuilder arrayKeyAppender, String[] jsonPositionSplits,
			int splitPosition) {
		JSONObject currentJSONObject;
		JSONArray jsonArray;
		jsonArray = getExistingJSONArray(existingJSONArrays, arrayPosition, arrayKeyAppender,
				jsonPositionSplits, splitPosition);
		jsonObjKey.append(arrayKeyPosition.get(arrayPosition));
		JSONObject jsonObject = null;
		if (!existingJSONObjects.containsKey(jsonObjKey.toString())) {
			jsonObject = new JSONObject();
			existingJSONObjects.put(jsonObjKey.toString(), jsonObject);
			jsonArray.add(jsonObject);
		} else {
			jsonObject = existingJSONObjects.get(jsonObjKey.toString());
		}
		currentJSONObject = jsonObject;
		return currentJSONObject;
	}

	@SuppressWarnings("static-method")
	private void storeArray(Map<String, JSONArray> existingJSONArrays, int arrayPosition,
			StringBuilder arrayKeyAppender, String[] jsonPositionSplits, int splitPosition, JSONArray jsonArray) {
		if (arrayPosition == 0) {
			existingJSONArrays.put(jsonPositionSplits[splitPosition], jsonArray);
		} else {
			existingJSONArrays.put(createArrayUniqueKey(jsonPositionSplits, splitPosition) + arrayKeyAppender.toString(), jsonArray);
		}
	}

	@SuppressWarnings("static-method")
	private JSONArray getExistingJSONArray(Map<String, JSONArray> existingJSONArrays, int arrayPosition,
			StringBuilder arrayKeyAppender, String[] jsonPositionSplits, int splitPosition) {
		JSONArray jsonArray;
		if (arrayPosition == 0) {
			jsonArray = existingJSONArrays.get(jsonPositionSplits[splitPosition]);
		} else {
			jsonArray = existingJSONArrays.get(createArrayUniqueKey(jsonPositionSplits, splitPosition) + arrayKeyAppender.toString());
		}
		return jsonArray;
	}

	@SuppressWarnings("static-method")
	private boolean findIfArrayAlreadyExists(Map<Integer, Integer> arrayKeyPosition,
			Map<String, JSONArray> existingJSONArrays, int arrayPosition, StringBuilder arrayKeyAppender,
			String[] jsonPositionSplits, int splitPosition) {
		boolean arrayExists;
		if (arrayPosition == 0) {
			arrayExists = existingJSONArrays.containsKey(jsonPositionSplits[splitPosition]);
		} else {
			arrayKeyAppender.append(arrayKeyPosition.get(arrayPosition - 1));
			arrayExists = existingJSONArrays.containsKey(createArrayUniqueKey(jsonPositionSplits, splitPosition) + arrayKeyAppender.toString());
		}
		return arrayExists;
	}
	
	private String createArrayUniqueKey(String[] jsonPositionSplits, int splitPosition) {
		StringBuilder uniqueKey = new StringBuilder("");
		for (int i=0; i < splitPosition; i++) {
			uniqueKey.append(jsonPositionSplits[splitPosition] + ".");
		}
		return uniqueKey.toString();
	}
	
	public static boolean isInteger(String s) {
	    try { 
	        Integer.parseInt(s);
	    } catch(NumberFormatException e) { 
	        return false; 
	    } catch(NullPointerException e) {
	        return false;
	    }
	    return true;
	}

	@Override
	public Format fromFormatSupported() {
		return inputFormat;
	}

	@Override
	public Format toFormatSupported() {
		return outputFormat;
	}

	public void setInputFormat(Format inputFormat) {
		this.inputFormat = inputFormat;
	}

	public void setOutputFormat(Format outputFormat) {
		this.outputFormat = outputFormat;
	}

}