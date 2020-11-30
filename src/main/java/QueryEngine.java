import org.apache.lucene.analysis.standard.StandardAnalyzer;
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
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;

public class QueryEngine {
    private static String indexDirectory = "lucene-index";
    boolean indexExists = false;
    private static String inputFiles = "src/main/resources/wiki-subset-20140602";
    private static String questions = "src/main/resources/questions.txt";

    IndexWriter writer;
    StandardAnalyzer analyzer;

    public QueryEngine() {
        analyzer = new StandardAnalyzer();
        if (!indexExists) {
            try {
                buildIndex();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private void parseFile(File file) throws IOException {
        Scanner sc = null;
        try {
            sc = new Scanner(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Document cur = null;
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            if (line.contains("[[File:")) {
                continue;
            }
            if (line.length() > 4 && line.charAt(0) == '[' && line.charAt(1) == '[' &&
                    line.charAt(line.length()-1) == ']' && line.charAt(line.length()-2) == ']') {
                if (cur != null) {
                    writer.addDocument(cur);
                }
                cur = new Document();
                cur.add(new StringField("docid", line.substring(2, line.length()-2), Field.Store.YES));
            } else {
                if (cur != null) {
                    cur.add(new TextField("text", line, Field.Store.YES));
                }
            }
        }
        writer.addDocument(cur);

    }

    private void buildIndex() throws IOException {
        File documents = new File(inputFiles);

        Directory index = FSDirectory.open(Paths.get(indexDirectory));

        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        writer = new IndexWriter(index, config);

        for (File f : Objects.requireNonNull(documents.listFiles())) {
            parseFile(f);
        }
        writer.close();
        indexExists = true;
    }

    private void answerQuestions(File questions) throws IOException, ParseException {
        Scanner sc = new Scanner(questions);

        Query q;
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDirectory)));
        IndexSearcher searcher = new IndexSearcher(reader);

        List<String> answers = new LinkedList<>();
        List<String> responses = new LinkedList<>();

        int i = 0;
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            //This is a category
            if (i % 4 == 0) {
                //Do something with categories
            } else if (i % 4 == 1) {
                //This is the query
                line = QueryParser.escape(line);
                q = new QueryParser("text", analyzer).parse(line);
                TopDocs docs = searcher.search(q, 1);

                if (docs.scoreDocs.length > 0) {
                    responses.add(searcher.doc(docs.scoreDocs[0].doc).getField("docid").stringValue());
                } else {
                    responses.add("");
                }
            } else if (i % 4 == 2) {
                //This is the answer
                answers.add(line);
            }
            i++;
        }

        int totalAnswers = answers.size();
        int totalRight = 0;
        for (i = 0; i < totalAnswers; i++) {
            if (answers.get(i).equals(responses.get(i))) {
                totalRight++;
            }
        }

        System.out.println(totalRight);
        System.out.println(totalAnswers);
        System.out.println((double)totalRight/totalAnswers);
    }

    public static void main(String[] args) throws IOException, ParseException {
        QueryEngine engine = new QueryEngine();

        engine.answerQuestions(new File(questions));
    }
}
