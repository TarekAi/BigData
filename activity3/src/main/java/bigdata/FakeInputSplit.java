package bigdata;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FakeInputSplit extends InputSplit implements org.apache.hadoop.io.Writable {
    private int numPoints;

    public FakeInputSplit() {
        // constructeur vide pour Hadoop
    }

    public FakeInputSplit(int numPoints) {
        this.numPoints = numPoints;
    }

    @Override
    public long getLength() throws IOException {
        return 0; // pas de fichier réel, donc longueur 0
    }

    @Override
    public String[] getLocations() throws IOException {
        return new String[0]; // pas de machine spécifique
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(numPoints);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        numPoints = in.readInt();
    }

    public int getNumPoints() {
        return numPoints;
    }
}
