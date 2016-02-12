package edu.ucla.cs.cs144;

import java.io.IOException;
import java.io.StringReader;
import java.io.File;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class Indexer {

    /** Creates a new instance of Indexer */
    public Indexer() {
    }

    private IndexWriter indexWriter = null;

    public IndexWriter getIndexWriter(boolean create) throws IOException {
        if (indexWriter == null) {
            Directory indexDir = FSDirectory.open(new File("/var/lib/lucene/index1"));
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_2, new StandardAnalyzer());
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            indexWriter = new IndexWriter(indexDir, config);
        }
        return indexWriter;
    }

    public void closeIndexWriter() throws IOException {
        if (indexWriter != null) {
            indexWriter.close();
        }
    }

    public void indexItem(ResultSet item, String category) throws IOException, SQLException {

        IndexWriter writer = getIndexWriter(false);
        Document doc = new Document();
        int id = item.getInt("ItemID");
        doc.add(new StringField("id", Integer.toString(id), Field.Store.YES));
        doc.add(new StringField("name", item.getString("Name"), Field.Store.YES));
        String fullSearchableText = item.getString("Name") + " " + item.getString("Description") + category;

        doc.add(new TextField("content", fullSearchableText, Field.Store.NO));
        writer.addDocument(doc);
    }

    public void rebuildIndexes() throws SQLException, IOException {

        Connection conn = null;

        // create a connection to the database to retrieve Items from MySQL
        try {
           conn = DbManager.getConnection(true);
       } catch (SQLException ex) {
           System.out.println(ex);
       }

       getIndexWriter(true);

       Statement s = conn.createStatement();
       Statement sc = conn.createStatement();
       ResultSet rs = s.executeQuery("SELECT * FROM Item");

       while( rs.next() ){
        int itemid = rs.getInt("ItemID");
        String query = "SELECT * FROM ItemCategory Where ItemID = '" + Integer.toString(itemid) + "';"; 
        ResultSet rsc = sc.executeQuery(query);
        String category = "";
        while(rsc.next())    
            category = category  + " " + rsc.getString("Category");
        rsc.close();
        indexItem(rs, category);
    }

    rs.close();
    sc.close();
    s.close();
    closeIndexWriter();

        // close the database connection
    try {
       conn.close();
   } catch (SQLException ex) {
       System.out.println(ex);
   }
}    

public static void main(String args[]) throws SQLException, IOException {
    Indexer idx = new Indexer();
    idx.rebuildIndexes();
}   
}
