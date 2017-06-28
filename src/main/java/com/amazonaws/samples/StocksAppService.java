package com.amazonaws.samples;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Attributes;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class StocksAppService {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	
    	//retrieve data
    	String dataBaseString = getData();
    	
    	organizeData( dataBaseString );
	}

	private static String getData() {
		
		String TODAYS_DATE = getLastTradingDate();
        TODAYS_DATE = "20170606"; //TODO: change to find previous trading day. This requires determining if weekday and if American holiday.

        //url where we get daily stock info from
        String urlLink = "https://www.quandl.com/api/v3/datatables/WIKI/PRICES.json?date=" + TODAYS_DATE + "&api_key=7hsNV69CDn_8SrPG2tqQ";
        
        String dataBaseString = "";

        try {
        	
            URL url = new URL( urlLink );
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.connect();

            InputStream is = conn.getInputStream();
            
            Scanner sc = new Scanner(is);

            dataBaseString = sc.next();
            
            //remove filler from formatted JSON data
            dataBaseString = dataBaseString.substring(22, dataBaseString.length() - 642);


            conn.disconnect();
            sc.close();
            is.close();

        } catch (Exception e) {
            e.printStackTrace();
        } 
        
        
		return dataBaseString;
	}

	private static String getLastTradingDate() {
		//to format for todays date
        Calendar c = Calendar.getInstance();
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String TODAYS_DATE = df.format(c.getTime()); //-YY//MM//DD


        return TODAYS_DATE;
	}
	
	 private static void organizeData(String dataBaseString) {

		AmazonDynamoDBClient dynamoDB;
		
		AWSCredentials credentials = new ProfileCredentialsProvider("default").getCredentials();
        dynamoDB = new AmazonDynamoDBClient(credentials);
        
        Region usEast2 = Region.getRegion(Regions.US_EAST_2);
        dynamoDB.setRegion(usEast2);

    	String tableName = "Stocks";
        	
	 
        //extract data
        while (!dataBaseString.isEmpty()) {

            int lowIndex = dataBaseString.indexOf("[");
            int highIndex = dataBaseString.indexOf("]") + 1;

            //gets all data between two brackets
            String stockInfo = dataBaseString.substring(lowIndex, highIndex);

            //create new item
            Map<String,AttributeValue> newStock = newItem( stockInfo );
            
            PutItemRequest putItemRequest = new PutItemRequest( tableName, newStock );

            //Stock object is stored in database
            dynamoDB.putItem( putItemRequest );
            
            //gets data after this set and comma
            dataBaseString = dataBaseString.substring(highIndex + 1);
        }
    }
 
    private static Map<String,AttributeValue> newItem( String stockInfo )
    {
    	 Map<String, AttributeValue> newStock = new HashMap<String, AttributeValue>();
    	 
    	 //remove redundant quotes
        stockInfo = stockInfo.replace( "\"", "" );

        //get ticker symbol
        int index = stockInfo.indexOf( ',' );
        String tickerSymbol = stockInfo.substring(1, index); //after '[' and before first ','
        stockInfo = stockInfo.substring( index + 1 ); //update to be string after ','
        stockInfo = stockInfo.substring( stockInfo.indexOf( ',' ) + 1 ); //update to be string after ','
        newStock.put( "Ticker Symbol", new AttributeValue( tickerSymbol ) );
        
        //get open price
        index = stockInfo.indexOf(',');
        double openPrice = Double.parseDouble(stockInfo.substring(0, index));
        stockInfo = stockInfo.substring(index + 1);
        newStock.put( "Open", new AttributeValue().withN( Double.toString( openPrice )  ) );

        //get high price
        index = stockInfo.indexOf(',');
        double highPrice = Double.parseDouble(stockInfo.substring(0, index));
        stockInfo = stockInfo.substring(index + 1);
        newStock.put( "High", new AttributeValue().withN( Double.toString( highPrice )  ) );

        //get low price
        index = stockInfo.indexOf(',');
        double lowPrice = Double.parseDouble(stockInfo.substring(0, index));
        stockInfo = stockInfo.substring(index + 1);
        newStock.put( "Low", new AttributeValue().withN( Double.toString( lowPrice )  ) );

        //get close price
        index = stockInfo.indexOf(',');
        double closePrice = Double.parseDouble(stockInfo.substring(0, index));
        stockInfo = stockInfo.substring(index + 1);
        newStock.put( "Close", new AttributeValue().withN( Double.toString( closePrice )  ) );

        //get volume
        index = stockInfo.indexOf(',');
        long volume = (long) Double.parseDouble(stockInfo.substring(0, index));
        newStock.put( "Volume", new AttributeValue().withN( Long.toString( volume )  ) );
        
        //get name
		try {
			Document doc = Jsoup.connect("https://finance.yahoo.com/quote/" + tickerSymbol ).get();
	        String name = doc.title();
	        name = name.substring( name.indexOf( "for" ) + 4, name.lastIndexOf( '-' ) );
	        newStock.put( "Name", new AttributeValue( name ) );
		}
		catch (IOException e) {
			e.printStackTrace();
		}
        
        return newStock;
    }
}
