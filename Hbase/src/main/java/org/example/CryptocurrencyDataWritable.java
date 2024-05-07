package org.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CryptocurrencyDataWritable implements Writable {
    private long timestamp;
    private double open;
    private double high;
    private double low;
    private double close;
    private double volume;
    private double vwap;

    // Default constructor (required for MapReduce)
    public CryptocurrencyDataWritable() {}

    // Parameterized constructor
    public CryptocurrencyDataWritable(long timestamp, double open, double high, double low, double close, double volume, double vwap) {
        this.timestamp = timestamp;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
        this.vwap = vwap;
    }

    // Getters and setters
    public long getTimestamp() {
        return timestamp;
    }

    public double getOpen() {
        return open;
    }

    public double getHigh() {
        return high;
    }

    public double getLow() {
        return low;
    }

    public double getClose() {
        return close;
    }

    public double getVolume() {
        return volume;
    }

    public double getVwap() {
        return vwap;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeDouble(open);
        out.writeDouble(high);
        out.writeDouble(low);
        out.writeDouble(close);
        out.writeDouble(volume);
        out.writeDouble(vwap);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        timestamp = in.readLong();
        open = in.readDouble();
        high = in.readDouble();
        low = in.readDouble();
        close = in.readDouble();
        volume = in.readDouble();
        vwap = in.readDouble();
    }

    @Override
    public String toString() {
        return timestamp + "," + open + "," + high + "," + low + "," + close + "," + volume + "," + vwap;
    }
}
