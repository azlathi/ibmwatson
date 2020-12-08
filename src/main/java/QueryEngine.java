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
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class QueryEngine {
    private static String indexDirectory = "lucene-index";
    boolean indexExists;
    private static String inputFiles = "src/main/resources/wiki-subset-20140602";
    private static String questions = "src/main/resources/questions.txt";
    private static String questionIndex = "question-index";

    IndexWriter writer;
    Analyzer analyzer;

    public QueryEngine(String indexExist) throws IOException {
        analyzer = CustomAnalyzer.builder()
                .withTokenizer("standard")
                .addTokenFilter("lowercase")
                .addTokenFilter("stop")
                .addTokenFilter("porterstem")
                .build();

        if (indexExist.equals("1")) {
            indexExists = true;
        } else {
            indexExists = false;
        }

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
        StringBuilder summary = new StringBuilder();
        boolean onSummary = true;
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            if (line.contains("[[File:")) {
                continue;
            }
            if (line.length() > 4 && line.charAt(0) == '[' && line.charAt(1) == '[' &&
                    line.charAt(line.length()-1) == ']' && line.charAt(line.length()-2) == ']') {
                if (cur != null) {
                    String toAdd = text.toString();
                    String sumAdd = summary.toString();

                    cur.add(new TextField("text", toAdd, Field.Store.YES));
                    cur.add(new TextField("summary", sumAdd, Field.Store.YES));

                    writer.addDocument(cur);
                    onSummary = true;
                }
                cur = new Document();
                text = new StringBuilder();
                summary = new StringBuilder();
                cur.add(new StringField("docid", line.substring(2, line.length()-2), Field.Store.YES));
            } else {
                if (cur != null && line.length() > 2) {
                    if (line.charAt(0) == '=' && line.charAt(1) == '=') {
                        onSummary = false;
                        continue;
                    }
                    if (line.contains("CATEGORIES:")) {
                        cur.add(new TextField("cat", line.replace("CATEGORIES: ", ""), Field.Store.YES));
                        continue;
                    }
                    if (onSummary) {
                        summary.append(line.replace("#REDIRECT", ""));
                    } else {
                        text.append(line);
                    }
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
        System.out.println("ANSWERING QUESTIONS");
        Scanner sc = new Scanner(questions);

        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDirectory)));
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(new CosineScore());

        List<String> answers = new LinkedList<>();
        List<java.lang.String> responses = new LinkedList<>();

        int i = 0;
        int num = 0;
        MultiFieldQueryParser queryParser = new MultiFieldQueryParser(new String[]{"summary", "text"}, analyzer);
        QueryParser summaryParser = new QueryParser("summary", analyzer);
        QueryParser categoryParser = new QueryParser("cat", analyzer);
        QueryParser textParser = new QueryParser("text", analyzer);

        StringBuilder query = new StringBuilder();
        StringBuilder fuzzQuery = new StringBuilder();
        int inTop20 = 0;
        ScoreDoc[] curScoreDoc = null;
        String category = "";
        String question = "";

        double MRR = 0;

        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            //This is a category
            if (i % 4 == 0) {
                //Do something with categories
//                query.append(QueryParser.escape(line)).append(" ");
                category = line;
                if (category.contains("(Alex: Not")) {
                    category = category.substring(0, category.indexOf("(Alex: Not"));
                }
                category = category.replace(".", "").replace("(Alex: We'll give you the ", "")
                        .replace(" You give us the", "").replace("You tell us the ","")
                .replace(" in which it is located", "");
                if (category.charAt(category.length()-1) == ')') {
                    category = category.substring(0, category.length() - 1);
                }

                query.append(QueryParser.escape(category)).append("~.7 ");
                fuzzQuery.append(QueryParser.escape(category)).append(" ");
            } else if (i % 4 == 1) {
                //This is the query
                query.append(QueryParser.escape(line));
                fuzzQuery.append(QueryParser.escape(line));
                question = QueryParser.escape(line);


                Query textQuery = queryParser.parse(query.toString());
//                Query categoryQuery = categoryParser.parse(QueryParser.escape(category));
                Query summaryQuery = summaryParser.parse(query.toString());
                Query categoryQuery = categoryParser.parse(query.toString());

                BooleanQuery booleanQuery = new BooleanQuery.Builder()
                        .add(categoryQuery, BooleanClause.Occur.SHOULD)
//                        .add(summaryQuery, BooleanClause.Occur.SHOULD)
                        .add(textQuery, BooleanClause.Occur.MUST)
                        .build();

                Query fuzz = categoryParser.parse(fuzzQuery.toString());

                TopDocs docs = searcher.search(booleanQuery, 1000);
                TopDocs secondDocs = QueryRescorer.rescore(searcher, docs, fuzz, 2, 10);

                curScoreDoc = secondDocs.scoreDocs;
//                curScoreDoc = docs.scoreDocs;



                if (docs.scoreDocs.length > 0) {
                    responses.add(searcher.doc(curScoreDoc[0].doc).getField("docid").stringValue());
//                    responses.add(topAnswer);
                } else {
                    System.out.println("NO ANSWER");
                    responses.add("");
                }


                query = new StringBuilder();
                fuzzQuery = new StringBuilder();
            } else if (i % 4 == 2) {
                //This is the answer
                answers.add(line);

                int pos = 1;
                for (ScoreDoc scoreDoc : curScoreDoc) {
                    if (line.contains(searcher.doc(scoreDoc.doc).getField("docid").stringValue())) {
                        inTop20++;
                        MRR += 1.0/pos;
                        break;
                    }
                    pos++;
                }
            }
            i++;
        }

        MRR /= 100;


        int totalAnswers = answers.size();
        int totalRight = 0;
        List<String> correctAnswers = new LinkedList<>();
        List<String> incorrectAnswers = new LinkedList<>();

        for (i = 0; i < totalAnswers; i++) {
            if (answers.get(i).contains(responses.get(i))) {
                totalRight++;
                correctAnswers.add(answers.get(i));
            } else {
                incorrectAnswers.add("INCORRECT: " + responses.get(i) + "\nCORRECT: " + answers.get(i));
            }
        }

        System.out.println("CORRECT ANSWERS: ");
        for (String answer : correctAnswers) {
            System.out.println(answer);
        }
        System.out.println();
        System.out.println("INCORRECT ANSWERS: ");
        for (String answer : incorrectAnswers) {
            System.out.println(answer);
        }

        System.out.println(totalRight);
        System.out.println(totalAnswers);
        System.out.println(inTop20);
        System.out.println("MRR: " + MRR);
    }

    public static void main(String[] args) throws IOException, ParseException {
        QueryEngine engine = new QueryEngine(args[0]);

        engine.answerQuestions(new File(questions));
    }
}
