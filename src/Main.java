import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

@SuppressWarnings("UnusedDeclaration")
public class Main {
    public static void main(String[] args) throws Exception {
        checkAsserts();
//        compareIndexSizes();
//        compareSegmentSizes();
//        commitSpeed();
//        commitSpeedWithoutAutomerge();
//        mergeOfFieldNames();
//        differentFieldTypes();
//        useIndexSearcher();
//        useIndexSearcherWithNotStoredField();
//        useCollector();
//        documentsBecomeVisibleAfterACommit();
//        withNearRealTimeSearchDocumentsBecomeVisibleSooner();
        forceCompoundFileFormat();
        forceSeparateFiles();
    }

    private static void forceCompoundFileFormat() throws IOException {
        Directory directory = getCleanDirectory("test-directory-compound");
        IndexWriterConfig conf = getBaseIndexWriterConfig();
        conf.setUseCompoundFile(true);
        IndexWriter indexWriter = new IndexWriter(directory, conf);
        indexWriter.addDocument(new Document());
        indexWriter.close();
    }

    private static void forceSeparateFiles() throws IOException {
        Directory directory = getCleanDirectory("test-directory-separate");
        IndexWriterConfig conf = getBaseIndexWriterConfig();
        conf.setUseCompoundFile(false);
        IndexWriter indexWriter = new IndexWriter(directory, conf);
        indexWriter.addDocument(new Document());
        indexWriter.close();
    }

    private static void withNearRealTimeSearchDocumentsBecomeVisibleSooner() throws IOException, InterruptedException {
        Directory directory = getCleanDirectory("test-directory");
        IndexWriter indexWriter = getIndexWriter(directory);

        DirectoryReader firstReader = DirectoryReader.open(indexWriter, true);
        assert countDocuments(firstReader) == 0;
        indexWriter.addDocument(new Document());
        assert countDocuments(firstReader) == 0;

        DirectoryReader secondReader = DirectoryReader.open(indexWriter, true);
        assert countDocuments(secondReader) == 1;

        indexWriter.addDocument(new Document());
        assert countDocuments(firstReader) == 0;
        assert countDocuments(secondReader) == 1;

        indexWriter.commit();
        assert countDocuments(firstReader) == 0;
        assert countDocuments(secondReader) == 1;
    }

    private static int countDocuments(IndexReader indexReader) {
        return indexReader.numDocs();
    }

    private static void documentsBecomeVisibleAfterACommit() throws IOException {
        Directory directory = getCleanDirectory("test-directory");
        IndexWriter indexWriter = getIndexWriter(directory);

        assert countDocuments("test-directory") == -1;

        indexWriter.addDocument(new Document());

        assert countDocuments("test-directory") == -1;

        indexWriter.commit();
        assert countDocuments("test-directory") == 1;
    }

    private static int countDocuments(String directoryName) throws IOException {
        Directory directory = getDirectory(directoryName);
        try {
            IndexReader reader = DirectoryReader.open(directory);
            int result = reader.numDocs();
            reader.close();
            directory.close();
            return result;
        } catch (IndexNotFoundException ignored) {
            return -1;
        }
    }

    private static void useCollector() throws IOException {
        Directory directory = getCleanDirectory("test-directory");
        IndexWriterConfig conf = getBaseIndexWriterConfig();
        conf.setUseCompoundFile(false);
        IndexWriter indexWriter = new IndexWriter(directory, conf);

        FieldType fieldType = TextField.TYPE_NOT_STORED;
        addDocument(indexWriter, "foo bar", fieldType);
        addDocument(indexWriter, "foobar", fieldType);
        addDocument(indexWriter, "hello", fieldType);
        addDocument(indexWriter, "hallo", fieldType);
        addDocument(indexWriter, "hallo world", fieldType);
        indexWriter.close();

        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);
        TermQuery query = new TermQuery(new Term("subject", "hallo"));
        SumScoreCollector collector = new SumScoreCollector();
        searcher.search(query, collector);
        System.out.println("got a total score of " + collector.score);
        reader.close();
    }

    private static void useIndexSearcherWithNotStoredField() throws IOException {
        Directory directory = getCleanDirectory("test-directory-not-stored");
        IndexWriterConfig conf = getBaseIndexWriterConfig();
        conf.setUseCompoundFile(false);
        IndexWriter indexWriter = new IndexWriter(directory, conf);

        FieldType fieldType = TextField.TYPE_NOT_STORED;
        addDocument(indexWriter, "foo bar", fieldType);
        addDocument(indexWriter, "foobar", fieldType);
        addDocument(indexWriter, "hello", fieldType);
        addDocument(indexWriter, "hallo", fieldType);
        addDocument(indexWriter, "hallo world", fieldType);
        indexWriter.close();

        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("subject", "hallo")), 1000);
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            System.out.println("    " + scoreDoc + " which has subject '" + reader.document(scoreDoc.doc).get("subject") + "'");
        }
        reader.close();

    }

    private static void useIndexSearcher() throws IOException {
        Directory directory = getCleanDirectory("test-directory-stored");
        IndexWriterConfig conf = getBaseIndexWriterConfig();
        conf.setUseCompoundFile(false);
        IndexWriter indexWriter = new IndexWriter(directory, conf);

        FieldType fieldType = TextField.TYPE_STORED;
        addDocument(indexWriter, "foo bar", fieldType);
        addDocument(indexWriter, "foobar", fieldType);
        addDocument(indexWriter, "hello", fieldType);
        addDocument(indexWriter, "hallo", fieldType);
        addDocument(indexWriter, "hallo world", fieldType);
        indexWriter.close();

        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("subject", "hallo")), 1000);
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            System.out.println("    " + scoreDoc + " which has subject '" + reader.document(scoreDoc.doc).get("subject") + "'");
        }
        reader.close();

    }

    private static void addDocument(IndexWriter indexWriter, String subject, FieldType fieldType) throws IOException {
        Document document = new Document();
        document.add(new Field("subject", subject, fieldType));
        indexWriter.addDocument(document);
    }

    private static void differentFieldTypes() throws IOException {
        System.out.println("Test what can be read back for various field types");
        Directory directory = getCleanDirectory("test-directory");
        IndexWriterConfig conf = getBaseIndexWriterConfig();
        conf.setUseCompoundFile(false);
        IndexWriter indexWriter = new IndexWriter(directory, conf);

        for (int i = 0; i < 10; i++) {
            Document document = new Document();
            document.add(new Field("storedName" + i, "hallo welt storedValue" + i, TextField.TYPE_STORED));

            FieldType type = new FieldType();
            type.setStored(true);
            type.setIndexed(false);
            document.add(new Field("storedButNoIndexName" + i, "someValue", type));

            FieldType all = new FieldType();
            all.setStored(true);
            all.setIndexed(true);
            all.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
            all.setStoreTermVectors(true);
            all.setStoreTermVectorOffsets(true);
            all.setStoreTermVectorPayloads(true);
            all.setStoreTermVectorPositions(true);
            document.add(new Field("all" + i, "someValue and someOtherValue", all));

            document.add(new Field("notStoredName" + i, "notStoredValue" + i, TextField.TYPE_NOT_STORED));
            indexWriter.addDocument(document);
        }
        indexWriter.close();

        IndexReader reader = DirectoryReader.open(directory);

        for (int i = 0; i < reader.numDocs(); i++) {
            StoredDocument read = reader.document(i);
            System.out.println("  " + read);
            for (StorableField storableField : read) {
                System.out.println("      " + storableField);
            }
        }
        reader.close();

    }

//    private static void mergeOfFieldNames() throws IOException {
//        int toWrite = 2_000;
//        System.out.println("Test of field names are merges");
//        Directory directory = getCleanDirectory("test-directory");
//        IndexWriter indexWriter = getIndexWriter(directory);
//        long start = System.currentTimeMillis();
//        for (int i = 0; i < toWrite; i++) {
//            Document document = new Document();
//            document.indexableFields()
//            indexWriter.addDocument(document);
//        }
//        indexWriter.close();
//        SizeAndTime sizeAndTime = getSizeAndTime(directory, start);
//        double millisPerCommit = (sizeAndTime.millis) / (double) (commits);
//        double commitsPerSecond = (double) (commits) / ((sizeAndTime.millis / 1000.0));
//        System.out.println("  got " + sizeAndTime + " and " + commits + " commits = " + millisPerCommit + " ms/commit = " + commitsPerSecond + " commits/sec");
//    }

    private static void commitSpeedWithoutAutomerge() throws IOException {
        int toWrite = 2_000;
        System.out.println("Commit after each empty document");
        Directory directory = getCleanDirectory("test-directory");
        IndexWriterConfig conf = getBaseIndexWriterConfig().setMergePolicy(NoMergePolicy.NO_COMPOUND_FILES);
        IndexWriter indexWriter = new IndexWriter(directory, conf);
        long start = System.currentTimeMillis();
        int commits = 0;
        for (int i = 0; i < toWrite; i++) {
            Document document = new Document();
            indexWriter.addDocument(document);
            indexWriter.commit();
            commits++;
        }
        indexWriter.close();
        SizeAndTime sizeAndTime = getSizeAndTime(directory, start);
        double millisPerCommit = (sizeAndTime.millis) / (double) (commits);
        double commitsPerSecond = (double) (commits) / ((sizeAndTime.millis / 1000.0));
        System.out.println("  got " + sizeAndTime + " and " + commits + " commits = " + millisPerCommit + " ms/commit = " + commitsPerSecond + " commits/sec");
    }

    private static void commitSpeed() throws IOException {
        int toWrite = 2_000;
        System.out.println("Commit after each empty document");
        Directory directory = getCleanDirectory("test-directory");
        IndexWriter indexWriter = getIndexWriter(directory);
        long start = System.currentTimeMillis();
        int commits = 0;
        for (int i = 0; i < toWrite; i++) {
            Document document = new Document();
            indexWriter.addDocument(document);
            indexWriter.commit();
            commits++;
        }
        indexWriter.close();
        SizeAndTime sizeAndTime = getSizeAndTime(directory, start);
        double millisPerCommit = (sizeAndTime.millis) / (double) (commits);
        double commitsPerSecond = (double) (commits) / ((sizeAndTime.millis / 1000.0));
        System.out.println("  got " + sizeAndTime + " and " + commits + " commits = " + millisPerCommit + " ms/commit = " + commitsPerSecond + " commits/sec");
    }

    private static void compareSegmentSizes() throws IOException {
        int toWrite = 10_000;
        System.out.println("Write " + toWrite + " empty documents using various commit chunks");
        SizeAndTime singleEmptyDocument = writeEmptyDocuments(toWrite);
        System.out.println("  only one commit: " + singleEmptyDocument);
        for (int commitEvery = toWrite; commitEvery >= 1; commitEvery /= 10) {
            Directory directory = getCleanDirectory("test-directory");
            IndexWriterConfig conf = getBaseIndexWriterConfig();
            IndexWriter indexWriter = new IndexWriter(directory, conf);
            long start = System.currentTimeMillis();
            int commits = 0;
            for (int i = 0; i < toWrite; i++) {
                Document document = new Document();
                indexWriter.addDocument(document);
                if (i > 0 && i % commitEvery == 0) {
                    indexWriter.commit();
                    commits++;
                }
            }
            indexWriter.close();
            SizeAndTime sizeAndTime = getSizeAndTime(directory, start);
            double millisPerCommit = (sizeAndTime.millis) / (double) (commits);
            double commitsPerSecond = (double) (commits) / ((sizeAndTime.millis / 1000.0));
            System.out.println("  committing every " + commitEvery + " results in " + sizeAndTime + " and " + commits + " commits = " + millisPerCommit + " ms/commit = " + commitsPerSecond + " commits/sec");
        }
    }

    private static void compareIndexSizes() throws IOException {
        System.out.println("Examine index size and write time for various empty documents");
        System.out.println("  index with no documents: " + noDocumentsAdded());
        SizeAndTime singleEmptyDocument = singleEmptyDocument();
        System.out.println("  index with 1 empty document: " + singleEmptyDocument);
        for (int n = 10; n < 1000_000_000; n *= 10) {
            SizeAndTime sizeAndTime = writeEmptyDocuments(n);
            double bytesPerDocument = (sizeAndTime.bytes - singleEmptyDocument.bytes) / (n - 1.0);
            double millisPerDocument = (sizeAndTime.millis - singleEmptyDocument.millis) / (n - 1.0);
            double documentPerMs = (n - 1.0) / (sizeAndTime.millis - singleEmptyDocument.millis);
            System.out.println("  index with " + n + " empty document: " + sizeAndTime + " = " + String.format(Locale.ENGLISH, "%.5f", bytesPerDocument) + " bytes per document and " + millisPerDocument + " ms/document = " + documentPerMs + " documents/ms");
        }
    }

    private static SizeAndTime singleEmptyDocument() throws IOException {
        return writeEmptyDocuments(1);
    }

    private static SizeAndTime writeEmptyDocuments(int n) throws IOException {
        Directory directory = getCleanDirectory("test-directory");
        IndexWriter indexWriter = getIndexWriter(directory);
        long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            Document document = new Document();
            indexWriter.addDocument(document);
        }
        indexWriter.close();
        return getSizeAndTime(directory, start);
    }

    private static SizeAndTime noDocumentsAdded() throws IOException {
        return writeEmptyDocuments(0);
    }

    private static SizeAndTime getSizeAndTime(Directory directory, long start) throws IOException {
        return new SizeAndTime(getDirectorySize(directory), System.currentTimeMillis() - start);
    }

    private static long getDirectorySize(Directory directory) throws IOException {
        long result = 0;
        for (String name : directory.listAll()) {
            result += directory.fileLength(name);
        }
        return result;
    }

    private static Directory getCleanDirectory(String pathname) throws IOException {
        File path = new File(pathname);
        removeDirectory(path);
        return new SimpleFSDirectory(path);
    }

    private static Directory getDirectory(String pathname) throws IOException {
        File path = new File(pathname);
        return new SimpleFSDirectory(path);
    }

    private static void removeDirectory(File path) {
        if (path.isDirectory()) {
            File[] files = path.listFiles();
            assert files != null;
            for (File file : files) {
                boolean deleted = file.delete();
                assert deleted;
            }
        }
        if (path.exists()) {
            boolean deleted = path.delete();
            assert deleted;
        }
    }

    private static IndexWriter getIndexWriter(Directory directory) throws IOException {
        IndexWriterConfig conf = getBaseIndexWriterConfig();
        return new IndexWriter(directory, conf);
    }

    private static IndexWriterConfig getBaseIndexWriterConfig() {
        Version luceneVersion = Version.LUCENE_50;
        Analyzer analyzer = new StandardAnalyzer(luceneVersion);
        return new IndexWriterConfig(luceneVersion, analyzer);
    }

    private static void checkAsserts() {
        try {
            assert false;
            throw new RuntimeException("Pass -ea to enable assertions.");
        } catch (AssertionError ignored) {

        }
    }

    private static class SizeAndTime {
        long bytes;
        long millis;

        private SizeAndTime(long bytes, long millis) {
            this.bytes = bytes;
            this.millis = millis;
        }

        @Override
        public String toString() {
            return bytes +
                    " bytes in " + (millis / 1000.0) +
                    " sec";
        }
    }

    private static class SumScoreCollector extends Collector {
        float score;
        private Scorer scorer;

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
            score += scorer.score();
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
//            throw new RuntimeException();
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return true;
        }
    }
}
