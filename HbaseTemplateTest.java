package au.gov.acic.dp.common.hbase;


import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import au.gov.acic.dp.common.controller.exception.EntityNotFoundException;
import au.gov.acic.dp.common.mapper.format.Format;
import net.minidev.json.JSONObject;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"hbase"})
public class HbaseTemplateTest {

    @Autowired 
	private HbaseOperations hbaseTemplate;
	
	public static Format outputFormat = new Format() {
		@Override
		public String getFormatKey() {
			return "TEST.PERSON";
		}
	};

	@Test
	public void testRetrieveRowDataFromHbaseNull() throws Exception {
		try {
			hbaseTemplate.retrieveHbaseRow(null);
			fail("Data must not be retrieved for null rowkey");
		} catch(Exception e) {
		}
	}

	@Test
	public void testRetrieveRowDataFromHbaseEmpty() throws Exception {
		try {
			hbaseTemplate.retrieveHbaseRow("");
			fail("Data must not be retrieved for empty rowkey");
		} catch(Exception e) {
		}
	}

	@Test
	public void testConvertHbaseRowDataToJSONNullException() throws Exception {
		try {
			hbaseTemplate.convertHbaseRowDataToJSON(null, outputFormat, new ArrayList<String>());
			fail("Data must not be retrieved for null rowkey");
		} catch(Exception e) {
		}
	}

	@Test
	public void testConvertHbaseRowDataToJSONInvalidRowKey() throws Exception {
		try {
			hbaseTemplate.convertHbaseRowDataToJSON("2426d3935b5b4231b2f6b96f83cfcb51", outputFormat, new ArrayList<String>());
			fail("Data must not be retrieved for invalid rowkey");
		} catch(EntityNotFoundException e) {
		}
	}

	@Test
	public void testConvertHbaseRowDataToJSONValidRowKey() throws Exception {
		try {
			System.out.println(hbaseTemplate.convertHbaseRowDataToJSON("2426d3935b5b4231b2f6b96f83cfcb51", () -> "PERSON", new ArrayList<String>()).toJSONString());
			fail("Data must not be retrieved for invalid rowkey");
		} catch(EntityNotFoundException e) {
		}
	}

}