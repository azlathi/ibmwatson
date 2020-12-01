import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.util.BytesRef;

public class CosineScore extends TFIDFSimilarity {

    @Override
    public float tf(float term_freq) {
        if (term_freq > 0) {
            return (float) (1 + Math.log10(term_freq));
        } else {
            return 0;
        }
    }

    @Override
    public float idf(long docFreq, long numDocs) {
        if (docFreq == 0) {
            return 0;
        } else {
            return (float) Math.log10((double)numDocs/(double)docFreq);
        }
    }

    @Override
    public float lengthNorm(int i) {
        return 1;
    }

    @Override
    public float sloppyFreq(int i) {
        return (float) (1.0 /( (double)i + 1));
    }

    @Override
    public float scorePayload(int i, int i1, int i2, BytesRef bytesRef) {
        return 1;
    }
}
