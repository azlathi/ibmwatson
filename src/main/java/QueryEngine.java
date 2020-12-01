import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.xml.builders.BooleanQueryBuilder;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.xpath.operations.Bool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class QueryEngine {
    private static String indexDirectory = "lucene-index";
    boolean indexExists = true;
    private static String inputFiles = "src/main/resources/wiki-subset-20140602";
    private static String questions = "src/main/resources/questions.txt";

    IndexWriter writer;
    Analyzer analyzer;

    public QueryEngine() throws IOException {
        analyzer = CustomAnalyzer.builder()
                .withTokenizer("standard")
                .addTokenFilter("lowercase")
                .addTokenFilter("stop")
                .addTokenFilter("porterstem")
                .build();

        if (!indexExists) {
            try {
                buildIndex();
            } catch (IOException | InterruptedException e) {
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
        StringBuilder text = new StringBuilder();
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            if (line.contains("[[File:")) {
                continue;
            }
            if (line.length() > 4 && line.charAt(0) == '[' && line.charAt(1) == '[' &&
                    line.charAt(line.length()-1) == ']' && line.charAt(line.length()-2) == ']') {
                if (cur != null) {
                    String toAdd = text.toString();

                    cur.add(new TextField("text", toAdd, Field.Store.YES));
                    writer.addDocument(cur);
                }
                cur = new Document();
                text = new StringBuilder();
                cur.add(new StringField("docid", line.substring(2, line.length()-2), Field.Store.YES));
            } else {
                if (cur != null && line.length() > 2) {
                    if (line.charAt(0) == '=' && line.charAt(1) == '=') {
                        continue;
                    }
                    if (line.contains("CATEGORIES: ")) {
                        cur.add(new StringField("cat", line.replace("CATEGORIES: ", ""), Field.Store.YES));
                        continue;
                    }
                    text.append(line);
                }
            }
        }

        String toAdd = QueryParser.escape(text.toString());
        cur.add(new TextField("text", toAdd, Field.Store.YES));
        writer.addDocument(cur);
    }

    private void buildIndex() throws IOException, InterruptedException {
        File documents = new File(inputFiles);

        Directory index = FSDirectory.open(Paths.get(indexDirectory));

        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setSimilarity(new CosineScore());

        writer = new IndexWriter(index, config);

        int t1 = documents.listFiles().length/2;

        File[] files = documents.listFiles();

        Thread thread1 = new Thread(() -> {
            for (int i = 0; i < t1; i++) {
                try {
                    parseFile(files[i]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread thread2 = new Thread(() -> {
           for (int i = t1; i < files.length; i++) {
               try {
                   parseFile(files[i]);
               } catch (IOException e) {
                   e.printStackTrace();
               }
           }
        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        writer.close();
        indexExists = true;
    }

    private void answerQuestions(File questions) throws IOException, ParseException {
        Scanner sc = new Scanner(questions);

        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDirectory)));
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(new CosineScore());

        List<String> answers = new LinkedList<>();
        List<String> responses = new LinkedList<>();

        int i = 0;
        MultiFieldQueryParser queryParser = new MultiFieldQueryParser(new String[]{"cat", "text"}, analyzer);

        StringBuilder query = new StringBuilder();
        int inTop20 = 0;
        ScoreDoc[] curScoreDoc = null;
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            //This is a category
            if (i % 4 == 0) {
                //Do something with categories
                query.append(QueryParser.escape(line) + " ");
            } else if (i % 4 == 1) {
                //This is the query
                query.append(QueryParser.escape(line));

                Query textQuery = queryParser.parse(query.toString());

                TopDocs docs = searcher.search(textQuery, 50);

                if (docs.scoreDocs.length > 0) {
                    responses.add(searcher.doc(docs.scoreDocs[0].doc).getField("docid").stringValue());

                    curScoreDoc = docs.scoreDocs;
                } else {
                    responses.add("");
                }
                query = new StringBuilder();
            } else if (i % 4 == 2) {
                //This is the answer
                answers.add(line);

                for (ScoreDoc scoreDoc : curScoreDoc) {
                    if (line.contains(searcher.doc(scoreDoc.doc).getField("docid").stringValue())) {
                        inTop20++;
                        break;
                    }
                }
            }
            i++;
        }

        int totalAnswers = answers.size();
        int totalRight = 0;
        for (i = 0; i < totalAnswers; i++) {
            System.out.println(responses.get(i));
            System.out.println(answers.get(i));
            if (answers.get(i).contains(responses.get(i))) {
                totalRight++;
            }
        }

        System.out.println(totalRight);
        System.out.println(totalAnswers);
        System.out.println((double)totalRight/totalAnswers);
        System.out.println(inTop20);
    }

    public static void main(String[] args) throws IOException, ParseException {
        QueryEngine engine = new QueryEngine();

        engine.answerQuestions(new File(questions));
    }
}
