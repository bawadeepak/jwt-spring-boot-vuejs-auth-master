package au.gov.acic.dp.common.mapper.impl;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import au.gov.acic.dp.common.mapper.format.Format;
import au.gov.acic.dp.common.mapper.format.InputDataHbase;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"test","hbase"})
public class HbaseToJSONMapperTest {
    
    @Autowired
	private HbaseToJSONMapper hbaseToJSONMapper;
	
	@Test
	public void testInputDataHbaseNull() {
		JSONObject jsonObject = hbaseToJSONMapper.convert(null);
		assertNull(jsonObject);
	}
	
	@Test
	public void testTestPerson() {
		InputDataHbase inputDataHbase = new InputDataHbase();
		inputDataHbase.setRowData(getTestRowData());
		hbaseToJSONMapper.setOutputFormat(new Format() {
			@Override
			public String getFormatKey() {
				return "PERSON";
			}
		});
		JSONObject jsonObject = hbaseToJSONMapper.convert(inputDataHbase);
		assertTrue(!jsonObject.isEmpty());
	}

	private Map<String, String> getTestRowData() {
		Map<String, String> rowData = new HashMap<String, String>();
		rowData.put("P~details~0~createdDate", "2006-01-26");
		rowData.put("P~details~1~estimatedBirthYearIndicator", "false");
		rowData.put("P~details~2~estimatedBirthYearIndicator", "true");
		rowData.put("P~standard~alternateNames~0", "ANTHONY00");
		rowData.put("P~standard~birthDate", "1960-11-11");
		rowData.put("P~standardbirthDate", "1961-11-11");
		rowData.put("P~standard~alternateNames~0", "ANTHONY20");
		rowData.put("P~standard~alternateNames~1", "ANTHONY21");
		return rowData;
	}
	
	@Test
	public void testTestPersonOtherIdentifiers() {
		InputDataHbase inputDataHbase = new InputDataHbase();
		inputDataHbase.setRowData(getTestRowDataOtherIdentifiers());
		hbaseToJSONMapper.setOutputFormat(new Format() {
			@Override
			public String getFormatKey() {
				return "PERSON";
			}
		});
		JSONObject jsonObject = hbaseToJSONMapper.convert(inputDataHbase);
		assertTrue(!jsonObject.isEmpty());
	}

	private Map<String, String> getTestRowDataOtherIdentifiers() {
		Map<String, String> rowData = new HashMap<String, String>();
		rowData.put("P~createdDate", "2002-08-19");
		rowData.put("P~dnasampleProvidedIndicator", "false");
		rowData.put("P~sourceAgencyCode", "WAPOL");
		rowData.put("P~sourceReferenceId", "862555");
		rowData.put("P~standard~birthDate", "1960-11-11");
		rowData.put("P~standard~alternateNames~0~birthDate", "1962-03-17");
		rowData.put("P~standard~alternateNames~0~familyName", "CITIZEN");
		rowData.put("P~standard~alternateNames~0~gender", "MALE");
		rowData.put("P~standard~alternateNames~0~givenName", "Joe");
		rowData.put("P~standard~alternateNames~0~name", "CITIZEN, Joe");
		rowData.put("P~standard~alternateNames~0~nameUsageType", "Preferred name");
		rowData.put("P~standard~alternateNames~0~otherIdentifiers~0", "sourceId:AN0");
 		rowData.put("P~standard~alternateNames~1~additionalNames~0", "givenName2");
		rowData.put("P~standard~alternateNames~1~additionalNames~1", "givenName3");
		rowData.put("P~standard~alternateNames~1~birthDate", "1962-03-17");
		rowData.put("P~standard~alternateNames~1~familyName", "CONTI");
		rowData.put("P~standard~alternateNames~1~gender", "UNKNOWN");
		rowData.put("P~standard~alternateNames~1~givenName", "John");
		rowData.put("P~standard~alternateNames~1~name", "CONTI, John");
		rowData.put("P~standard~alternateNames~1~nameUsageType", "Also known as or alias");
		rowData.put("P~standard~alternateNames~1~otherIdentifiers~0", "sourceId:AN1");
		rowData.put("P~standard~alternateNames~2~birthDate", "1962-03-17");
		rowData.put("P~standard~alternateNames~2~familyName", "DIXON");
		rowData.put("P~standard~alternateNames~2~gender", "UNKNOWN");
		rowData.put("P~standard~alternateNames~2~givenName", "Joe");
		rowData.put("P~standard~alternateNames~2~name", "DIXON, Joe");
		rowData.put("P~standard~alternateNames~2~nameUsageType", "Also known as or alias");
		rowData.put("P~standard~alternateNames~2~otherIdentifiers~0", "sourceId:AN2");
		rowData.put("P~standard~birthDate", "1962-03-17");
		rowData.put("P~standard~deathDate", "2017-10-11");
		rowData.put("P~standard~familyName", "CITIZEN");
		rowData.put("P~standard~gender", "MALE");
		rowData.put("P~standard~givenName", "Joe");
		rowData.put("P~standard~height", "183.0");
		rowData.put("P~standard~name", "CITIZEN, Joe");
		rowData.put("P~standard~nameUsageType", "Preferred name");
		rowData.put("P~standard~otherIdentifiers~0", "sourceId:S0");
		rowData.put("P~standard~otherIdentifiers~1", "sourceReference:S1");
		rowData.put("P~standard~otherIdentifiers~2", "cniIdentifier:S2");
		addFeatures(rowData);
		addPhysicalDescriptions(rowData);
		return rowData;
	}
	
	private void addPhysicalDescriptions(Map<String, String> rowData) {
		rowData.put("P~physicalDescriptions~0~baldness", "EXTENSIVELY BALD");
		rowData.put("P~physicalDescriptions~0~build", "THIN");
		rowData.put("P~physicalDescriptions~0~complexion", "OLIVE");
		rowData.put("P~physicalDescriptions~0~descriptionDate", "2016-04-19");
		rowData.put("P~physicalDescriptions~0~ethnicAppearance", "ABORIGINAL");
		rowData.put("P~physicalDescriptions~0~eyeColour", "BROWN");
		rowData.put("P~physicalDescriptions~0~facialHairStyle", "NO FACIAL HAIR");
		rowData.put("P~physicalDescriptions~0~hairColour", "BLACK");
		rowData.put("P~physicalDescriptions~0~hairLength", "OTHER");
		rowData.put("P~physicalDescriptions~0~hairStyle", "TIED BACK");
		rowData.put("P~physicalDescriptions~0~height", "183.0");
		rowData.put("P~physicalDescriptions~0~indigenousGroup", "ABORIGINAL");
		rowData.put("P~physicalDescriptions~0~otherIdentifiers~0", "sourceId:PD0");
		rowData.put("P~physicalDescriptions~0~remarks", "remarks");
		rowData.put("P~physicalDescriptions~0~weight", "80.0");
	}

	private void addFeatures(Map<String, String> rowData) {

		rowData.put("P~physicalFeatures~0~category", "BODY PIERCING");
		rowData.put("P~physicalFeatures~0~description", "descriptionText");
		rowData.put("P~physicalFeatures~0~featureDate", "2009-09-20");
		rowData.put("P~physicalFeatures~0~location", "EAR");
		rowData.put("P~physicalFeatures~0~locationSide", "UNRECOGNISED");
		rowData.put("P~physicalFeatures~0~locationText", "featureLocationText");
		rowData.put("P~physicalFeatures~0~otherIdentifiers~0", "sourceId:PF0");

		rowData.put("P~physicalFeatures~1~category", "UNKNOWN");
		rowData.put("P~physicalFeatures~1~description", "ANOREXIC");
		rowData.put("P~physicalFeatures~1~featureDate", "2006-12-15");
		rowData.put("P~physicalFeatures~1~otherIdentifiers~0", "sourceId:PF1");

		rowData.put("P~physicalFeatures~2~category", "SCAR");
		rowData.put("P~physicalFeatures~2~description", "Scar; 2CM SCAR");
		rowData.put("P~physicalFeatures~2~featureDate", "2007-09-17");
		rowData.put("P~physicalFeatures~2~location", "FOREHEAD");
		rowData.put("P~physicalFeatures~2~locationSide", "UNRECOGNISED");
		rowData.put("P~physicalFeatures~2~locationText", "Brow - gen");
		rowData.put("P~physicalFeatures~2~otherIdentifiers~0", "sourceId:PF2");
		rowData.put("P~physicalFeatures~2~otherIdentifiers~1", "sourceId:PF21");
		
		rowData.put("P~physicalFeatures~3~category", "NATURAL TEETH");
		rowData.put("P~physicalFeatures~3~description", "");
		rowData.put("P~physicalFeatures~3~featureDate", "2006-11-08");
		rowData.put("P~physicalFeatures~3~otherIdentifiers~0", "sourceId:PF3");
		
		rowData.put("P~physicalFeatures~4~category", "SPEECH - SLOW/HESITANT");
		rowData.put("P~physicalFeatures~4~description", "");
		rowData.put("P~physicalFeatures~4~featureDate", "2006-11-08");
		rowData.put("P~physicalFeatures~4~otherIdentifiers~0", "sourceId:PF4");
		
		rowData.put("P~physicalFeatures~5~category", "TATTOO");
		rowData.put("P~physicalFeatures~5~description", "Flames/fire; FLAMES/EAGLE");
		rowData.put("P~physicalFeatures~5~location", "UPPER ARM");
		rowData.put("P~physicalFeatures~5~locationSide", "LEFT");
		rowData.put("P~physicalFeatures~5~locationText", "featureLocationText");
		rowData.put("P~physicalFeatures~5~otherIdentifiers~0", "sourceId:PF5");
		rowData.put("P~physicalFeatures~5~tattooCategory", "OTHER");
		rowData.put("P~physicalFeatures~5~tattooSubcategory", "FIRE FLAME SMOKE");
	}
	
	@Test
	public void testTestPerson1() {
		InputDataHbase inputDataHbase = new InputDataHbase();
		inputDataHbase.setRowData(getTestRowData1());
		hbaseToJSONMapper.setOutputFormat(new Format() {
			@Override
			public String getFormatKey() {
				return "PERSON";
			}
		});
		JSONObject jsonObject = hbaseToJSONMapper.convert(inputDataHbase);
		assertNotNull(jsonObject);
		JSONObject jsonObjectPoi = (JSONObject)jsonObject.get("person");
		assertNotNull(jsonObjectPoi);
		JSONArray jsonArrayPD = (JSONArray)jsonObjectPoi.get("alternateNames");
		assertNotNull(jsonArrayPD);
		JSONArray jsonObjectPD = (JSONArray)jsonObjectPoi.get("gender");
		assertNull(jsonObjectPD);
	}

	@Test
	public void testTestPerson2() {
		InputDataHbase inputDataHbase = new InputDataHbase();
		inputDataHbase.setRowData(getTestRowData1());
		hbaseToJSONMapper.setOutputFormat(new Format() {
			@Override
			public String getFormatKey() {
				return "PERSON";
			}
		});
		JSONObject jsonObject = hbaseToJSONMapper.convert(inputDataHbase);
		assertNotNull(jsonObject);
		JSONObject jsonObjectPoi = (JSONObject)jsonObject.get("person");
		assertNotNull(jsonObjectPoi);
	}

	private Map<String, String> getTestRowData1() {
		Map<String, String> rowData = new HashMap<String, String>();
		rowData.put("P~createdDate", "2006-01-26");
		rowData.put("P~details~0~createdDate", "2006-01-26");
		rowData.put("P~details~0~estimatedBirthYearIndicator", "false");
		rowData.put("P~details~0~nameUsageCategory", "PREFERRED NAME");
		rowData.put("P~details~1~createdDate", "2006-01-26");
		rowData.put("P~details~1~estimatedBirthYearIndicator", "false");
		rowData.put("P~details~1~nameUsageCategory", "ALIAS");
		rowData.put("P~details~2~createdDate", "2006-01-26");
		rowData.put("P~details~2~estimatedBirthYearIndicator", "false");
		rowData.put("P~details~2~nameUsageCategory", "ALIAS");
		rowData.put("P~standard~alternateNames~0", "ANTHONY");
		rowData.put("P~standard~birthDate", "1967-04-17");
		rowData.put("P~standard~gender", "MALE");
		rowData.put("P~standard~familyName", "OSHEA");
		rowData.put("P~standard~height", "0.0");
		rowData.put("P~standard~weight", "0.0");
		return rowData;
	}

}
