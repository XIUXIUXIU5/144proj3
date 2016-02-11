package edu.ucla.cs.cs144;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;

import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import edu.ucla.cs.cs144.DbManager;
import edu.ucla.cs.cs144.SearchRegion;
import edu.ucla.cs.cs144.SearchResult;

public class AuctionSearch implements IAuctionSearch {

	/* 
         * You will probably have to use JDBC to access MySQL data
         * Lucene IndexSearcher class to lookup Lucene index.
         * Read the corresponding tutorial to learn about how to use these.
         *
	 * You may create helper functions or classes to simplify writing these
	 * methods. Make sure that your helper functions are not public,
         * so that they are not exposed to outside of this class.
         *
         * Any new classes that you create should be part of
         * edu.ucla.cs.cs144 package and their source files should be
         * placed at src/edu/ucla/cs/cs144.
         *
         */

	private IndexSearcher searcher = null;
	private QueryParser parser = null;

	public IndexSearcher getSearchEngine() throws IOException {
		if (searcher == null) {
			searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(new File("/var/lib/lucene/index1"))));
		}
		return searcher;
	}

	public QueryParser getQueryParser() {
		if (parser == null) {
			parser = new QueryParser("content", new StandardAnalyzer());
		}
		return parser;
	}
	
	public SearchResult[] basicSearch(String query, int numResultsToSkip, 
		int numResultsToReturn) {
		IndexSearcher s = null;
		Query q = null;
		SearchResult[] result = null;

		try {
			s = getSearchEngine();
		} catch (IOException ex) {
			System.out.println(ex);
		}
		QueryParser p = getQueryParser();

		try {
			q = p.parse(query);
		} catch (ParseException ex) {
			System.out.println(ex);
		}

		try{
			TopDocs topDocs = s.search(q, numResultsToSkip + numResultsToReturn);
			ScoreDoc[] hits = topDocs.scoreDocs;
			
			if (hits.length - numResultsToSkip > 0) {
				result = new SearchResult[hits.length - numResultsToSkip];
				for (int i = numResultsToSkip; i < hits.length; i++) {
					Document doc = s.doc(hits[i].doc);
					result[i - numResultsToSkip] = new SearchResult(doc.get("id"), doc.get("name"));
				}
			}
		} catch (IOException ex) {
			System.out.println(ex);
		}

		return result;
	}

	public SearchResult[] spatialSearch(String query, SearchRegion region,
		int numResultsToSkip, int numResultsToReturn) {
		// TODO: Your code here!
		String spactialQ = "select ItemID from ItemRegion where " + region.getLx() +"< x(LocationPoint) and x(LocationPoint) < "+ region.getRx()+" and y(LocationPoint) < "+region.getRy()+" and y(LocationPoint) > "+region.getLy()+";";
		Connection conn = null;
		Statement s = null;
		ResultSet rs = null;
		System.out.println(spactialQ);
        // create a connection to the database to retrieve Items from MySQL
		HashSet<String> hash = new HashSet<String>();
		SearchResult[] basicResult = basicSearch(query,numResultsToSkip,numResultsToReturn);
		ArrayList<SearchResult> resultlist = new ArrayList<SearchResult>();

		try {
			conn = DbManager.getConnection(true);
			s = conn.createStatement();
			rs = s.executeQuery(spactialQ);
			while( rs.next() ){
				String itemid = ""+rs.getInt("ItemID");
				hash.add(itemid);

			}
			rs.close();
			s.close();
			conn.close();
		} catch (SQLException ex) {
			System.out.println(ex);
		}

		System.out.println(hash.size());
		for (int i = 0; i < basicResult.length;i++ ) {
			if(hash.contains(basicResult[i].getItemId()))
			{
				resultlist.add(basicResult[i]);
			}
		}

		System.out.println(resultlist.size());
		SearchResult[] result = new SearchResult[resultlist.size()];
		result = resultlist.toArray(result);
		return result;
	}

	public String getXMLDataForItemId(String itemId) {
		// TODO: Your code here!
		String result = "";
		Connection conn = null;
		Statement s = null;
		Statement sc = null;
		ResultSet rs = null;
		ResultSet rc = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        SimpleDateFormat ss = new SimpleDateFormat("MMM-dd-yy hh:mm:ss");

		try {
			String query = "select * from Item where ItemID = " + itemId + ";";
			String queryc = "select Category from ItemCategory where ItemID = " +itemId+";";
			conn = DbManager.getConnection(true);
			s = conn.createStatement();
			sc = conn.createStatement();

			rs = s.executeQuery(query);
			rc = sc.executeQuery(queryc);
			if (rs.next()) {
				result += "<Item ItemID=\""+itemId+"\">\n  <Name>"+rs.getString("Name")+ "</Name>";
				while(rc.next())
				{
					result += "\n  <Category>"+rc.getString("Category")+ "</Category>";
				}

				sc.close();
				rc.close();
				
				String element = Double.toString(rs.getDouble("Currently"));
				if (element.length() != 0) {
					result += "\n  <Currently>$"+element+ "</Currently>";
				}
				
				element = Double.toString(rs.getDouble("First_Bid"));
				if (element.length() != 0) {
					result += "\n  <First_Bid>$"+element+ "</First_Bid>";
				}

				element = Integer.toString(rs.getInt("Number_of_Bids"));
				if (element.length() != 0) {
					result += "\n  <Number_of_Bids>"+element+"</Number_of_Bids>";
				}

				if (rs.getInt("Number_of_Bids") == 0) {
					result += "\n  <Bids />";
				}
				else
				{
					String queryb = "select * from ItemBid where ItemID = " + itemId+ ";";
					Statement sb = conn.createStatement();
					ResultSet rb = sb.executeQuery(queryb);
					result += "\n  <Bids>";
					while(rb.next())
					{
						result += "\n    <Bid>";
						String querybidder = "select * from Bidder where BidderID = \"" +rb.getString("BidderID")+ "\";";
						Statement sbidder = conn.createStatement();
						ResultSet rbidder = sbidder.executeQuery(querybidder);
						rbidder.next();
						result += "\n      <Bidder Rating=\""+Double.toString(rbidder.getDouble("Rating"))+"\" UserID=\""+rbidder.getString("BidderID")+"\">";
						result += "\n        <Location>" +rbidder.getString("Location")+"</Location>";
						result += "\n        <Country>" +rbidder.getString("Country")+"</Country>";
						result += "\n      </Bidder>";

						Date timeDate = sdf.parse(rb.getString("Time"));
						result += "\n      <Time>"+ss.format(timeDate)+"</Time>";
						result += "\n      <Amount>$"+rb.getString("Amount")+"</Amount>";
						result += "\n    </Bid>";
						sbidder.close();
						rbidder.close();
					}
					result += "\n  </Bids>";
					sb.close();
					rb.close();
				}

				
				Double latitude = rs.getDouble("Latitude");
				Double logitude = rs.getDouble("Logitude");
				if (latitude== 0 && logitude == 0) {
					result += "\n  <Location>" +rs.getString("Location")+"</Location>";
				}
				else
				{
					result += "\n  <Location";
					if (latitude != 0) {
						result += " Latitude=\"" +Double.toString(latitude)+"\"";
						if (logitude != 0) {
							result += " Logitude=\""+ Double.toString(logitude) + "\">";
						}
						else
							result += ">";
					}
					else
					{
						result +=" Logitude=\""+ Double.toString(logitude) + "\">";
					}
					result += rs.getString("Location") + "</Location>";
				}

				element = rs.getString("Country");
				if (element.length() != 0) {
					result += "\n  <Country>"+element+"</Country>";
				}
				element = rs.getString("Started");
				if (element.length() != 0) {
					Date startdate = sdf.parse(element);
					result += "\n  <Started>"+ss.format(startdate)+"</Started>";
				}
				element = rs.getString("Ends");
				if (element.length() != 0) {
					Date enddate = sdf.parse(element);

					result += "\n  <Ends>"+ss.format(enddate)+"</Ends>";
				}

				String queryseller = "select * from Seller where SellerID = \"" + rs.getString("SellerID")+ "\";";
				Statement sseller = conn.createStatement();
				ResultSet rseller = sseller.executeQuery(queryseller);
				rseller.next();
				result += "\n  <Seller Rating=\""+Double.toString(rseller.getDouble("Rating"))+ "\" UserID=\""+rseller.getString("SellerID")+"\" />";

				sseller.close();
				rseller.close();

				result += "\n  <Description>"+rs.getString("Description")+"</Description>";
				result += "\n</Item>";

				s.close();
				rs.close();
				conn.close();
			}
			else
				return "";



		} catch (Exception e) {
			System.out.println(e);
		}
		return result;
	}
	
	public String echo(String message) {
		return message;
	}

}
