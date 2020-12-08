import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.simple.Sentence;
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

    IndexWriter writer;
    Analyzer analyzer;

    Properties prop;
    StanfordCoreNLP pipeline;

    /**
     * Creates a QueryEngine instance that is used to index documents and retrieve queries
     * @param indexExist Command line argument, determines whether to build or skip building index
     * @throws IOException
     */
    public QueryEngine(String indexExist) throws IOException {
        //Create the analyzer with porter stemmer and stop word removal
        analyzer = CustomAnalyzer.builder()
                .withTokenizer("standard")
                .addTokenFilter("lowercase")
                .addTokenFilter("stop")
                .addTokenFilter("porterstem")
                .build();

        prop = new Properties();
        prop.setProperty("annotators", "tokenize,ssplit,pos,lemma");

        pipeline = new StanfordCoreNLP(prop);

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

    /**
     * Given a .txt file that contain multiple wiki pages, parse them individually and create documents
     * to store in the Lucene index
     * @param file The .txt to parse
     * @throws IOException
     */
    private void parseFile(File file) throws IOException {
        Scanner sc = null;
        try {
            sc = new Scanner(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        // The current document we are on
        Document cur = null;
        // To append text to line by line
        StringBuilder text = new StringBuilder();
        StringBuilder summary = new StringBuilder();

        // Determines if we are still in the summary section of the wiki page
        boolean onSummary = true;

        // Go through each line
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            // Skip file attachments
            if (line.contains("[[File:")) {
                continue;
            }

            // If we are on a new wiki document
            if (line.length() > 4 && line.charAt(0) == '[' && line.charAt(1) == '[' &&
                    line.charAt(line.length()-1) == ']' && line.charAt(line.length()-2) == ']') {
                // Add the previous tracked document
                if (cur != null) {
                    String toAdd = text.toString();
                    String sumAdd = summary.toString();

                    CoreDocument textLemma = new CoreDocument(toAdd);
                    pipeline.annotate(textLemma);

                    for (CoreSentence sen : textLemma.sentences()) {
                        cur.add(new TextField("text", String.join(" ", sen.lemmas()), Field.Store.YES));
                    }

                    CoreDocument summLemma = new CoreDocument(sumAdd);
                    pipeline.annotate(summLemma);

                    for (CoreSentence sen : summLemma.sentences()) {
                        cur.add(new TextField("summary", String.join(" ", sen.lemmas()), Field.Store.YES));
                    }

                    cur.add(new TextField("text", toAdd, Field.Store.YES));
                    cur.add(new TextField("summary", sumAdd, Field.Store.YES));

                    writer.addDocument(cur);
                    onSummary = true;
                }
                // Create the new document that starts on this line
                cur = new Document();
                text = new StringBuilder();
                summary = new StringBuilder();
                cur.add(new StringField("docid", line.substring(2, line.length()-2), Field.Store.YES));
            } else {
                if (cur != null && line.length() > 2) {
                    // If we are no longer on the summary section
                    if (line.charAt(0) == '=' && line.charAt(1) == '=') {
                        onSummary = false;
                        continue;
                    }
                    // If we are on the categories section
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

        // Add the last document that was being tracked
        String toAdd = QueryParser.escape(text.toString());
        cur.add(new TextField("text", toAdd, Field.Store.YES));
        writer.addDocument(cur);
    }

    /**
     * Builds the Lucene index
     * @throws IOException
     * @throws InterruptedException
     */
    private void buildIndex() throws IOException, InterruptedException {
        File documents = new File(inputFiles);

        Directory index = FSDirectory.open(Paths.get(indexDirectory));

        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setSimilarity(new CosineScore());

        writer = new IndexWriter(index, config);

        int t1 = documents.listFiles().length/2;

        File[] files = documents.listFiles();

        // Create 2 threads so building index goes faster

        //Thread1 does the first half of the text files
        Thread thread1 = new Thread(() -> {
            for (int i = 0; i < t1; i++) {
                try {
                    parseFile(files[i]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        //Thread 2 does the second half
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

    /**
     * Using a text file of questions, use the query with the Index and retrieve top 1 answer
     * @param questions The .txt file formatted for questions
     * @throws IOException
     * @throws ParseException
     */
    private void answerQuestions(File questions) throws IOException, ParseException {
        System.out.println("ANSWERING QUESTIONS");
        Scanner sc = new Scanner(questions);

        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDirectory)));
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(new CosineScore());

        // To compare correct answers and retrieved responses
        List<String> answers = new LinkedList<>();
        List<java.lang.String> responses = new LinkedList<>();

        // Determines which line we are on in the file
        int i = 0;

        // Parser that looks at both summary and text sections
        MultiFieldQueryParser queryParser = new MultiFieldQueryParser(new String[]{"summary", "text"}, analyzer);

        // Parser that looks at only category section
        QueryParser categoryParser = new QueryParser("cat", analyzer);

        // Fuzz query has tolerance for edit distance
        StringBuilder fuzzQuery = new StringBuilder();

        // Query does not
        StringBuilder query = new StringBuilder();
        int inTop20 = 0;
        ScoreDoc[] curScoreDoc = null;
        String category = "";

        double MRR = 0;

        // Go through line by line
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            //This is a category
            if (i % 4 == 0) {
                // Process the category line to remove dialogue text
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

                CoreDocument lemmaQuery = new CoreDocument(category);
                pipeline.annotate(lemmaQuery);
                category = String.join(" ", lemmaQuery.sentences().get(0).lemmas());

                // Add this to the query, fuzz gets edit distance weight of .7
                fuzzQuery.append(QueryParser.escape(category)).append("~.7 ");
                query.append(QueryParser.escape(category)).append(" ");
            } else if (i % 4 == 1) {
                //This is the question
                CoreDocument lemmaQuery = new CoreDocument(line);
                pipeline.annotate(lemmaQuery);
                line = String.join(" ", lemmaQuery.sentences().get(0).lemmas());

                query.append(QueryParser.escape(line));
                fuzzQuery.append(QueryParser.escape(line));


                // These are used in conjunction in the BooleanQuery object provided by Lucene.
                Query textQuery = queryParser.parse(fuzzQuery.toString());
                Query categoryQuery = categoryParser.parse(fuzzQuery.toString());

                // Filters through the category query first, then uses the text query
                BooleanQuery booleanQuery = new BooleanQuery.Builder()
                        .add(categoryQuery, BooleanClause.Occur.SHOULD)
                        .add(textQuery, BooleanClause.Occur.MUST)
                        .build();

                // Re-ranks the top 10 documents with the stricter query over their categories
                Query strict = categoryParser.parse(query.toString());
                TopDocs docs = searcher.search(booleanQuery, 10);
                TopDocs secondDocs = QueryRescorer.rescore(searcher, docs, strict, 2, 10);

                curScoreDoc = secondDocs.scoreDocs;



                if (docs.scoreDocs.length > 0) {
                    responses.add(searcher.doc(curScoreDoc[0].doc).getField("docid").stringValue());
                } else {
                    System.out.println("NO ANSWER");
                    responses.add("");
                }

                // Reset for next question
                query = new StringBuilder();
                fuzzQuery = new StringBuilder();
            } else if (i % 4 == 2) {
                //This is the answer
                answers.add(line);

                // Check if in the top K docs and compute part of MRR score
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

        // Calculate correctness percentage and print out incorrect and correct answers
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

    /**
     * Runs the program
     * @param args "1" if index exists, "2" otherwise
     * @throws IOException
     * @throws ParseException
     */
    public static void main(String[] args) throws IOException, ParseException {
        QueryEngine engine = new QueryEngine(args[0]);

        engine.answerQuestions(new File(questions));
    }
}
