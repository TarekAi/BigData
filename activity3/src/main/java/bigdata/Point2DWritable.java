package bigdata;

import org.apache.hadoop.io.Writable;

import java.awt.geom.Point2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point2DWritable implements Writable {
    private Point2D.Double point;

    // Constructeur par défaut (requis par Hadoop)
    public Point2DWritable() {
        this.point = new Point2D.Double();
    }

    // Constructeur avec valeurs
    public Point2DWritable(double x, double y) {
        this.point = new Point2D.Double(x, y);
    }

    public Point2D.Double getPoint() {
        return point;
    }

    public void setPoint(Point2D.Double point) {
        this.point = point;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(point.getX());
        out.writeDouble(point.getY());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        double x = in.readDouble();
        double y = in.readDouble();
        point.setLocation(x, y);
    }

    @Override
    public String toString() {
        return point.getX() + "\t" + point.getY();
    }
}
