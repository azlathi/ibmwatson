import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;

public class QuestionEngine {
    private static String indexDirectory = "question-index";
    String query;
    String thisDir;
    Analyzer analyzer;

    public QuestionEngine(HashMap<String, String> docs, String question, int num) throws IOException {
//        File indexDir = new File(indexDirectory + "/dir" + num);
//        if (!indexDir.exists()) {
//            indexDir.mkdir();
//        }

        thisDir = indexDirectory + "/" + num;
        query = question;

        Directory index = FSDirectory.open(Paths.get(thisDir));
        analyzer = CustomAnalyzer.builder()
                .withTokenizer("standard")
                .addTokenFilter("lowercase")
                .addTokenFilter("stop")
                .addTokenFilter("porterstem")
                .build();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
//        config.setSimilarity(new BooleanSimilarity());

        IndexWriter writer = new IndexWriter(index, config);

        for (String docid : docs.keySet()) {
            Document doc = new Document();
            doc.add(new StringField("docid", docid, Field.Store.YES));
            doc.add(new TextField("summary", docs.get(docid), Field.Store.YES));

            writer.addDocument(doc);
        }

        writer.close();
    }

    public String getAnswer() throws IOException, ParseException {
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(thisDir)));
        IndexSearcher searcher = new IndexSearcher(reader);
//        searcher.setSimilarity(new BooleanSimilarity());

        TopDocs topDocs = searcher.search(new QueryParser("summary", analyzer).parse(query), 1);

        return searcher.doc(topDocs.scoreDocs[0].doc).getField("docid").stringValue();
    }
}
