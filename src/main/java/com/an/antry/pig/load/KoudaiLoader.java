package com.an.antry.pig.load;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
//Log format
//"mac:50:A4:C8:D7:10:7D"|"build:5141bc99"|"network:mobile"|"version:2.4.1"|"id:taobao22935952431"|

/**
 * REGISTER /jar/kload.jar;<br>
 * AA = LOAD '/log/load.log' USING kload.KoudaiLoader('mac,build') AS (mac,build);<br>
 * DUMP AA;
 */

// Output: (50:A4:C8:D7:10:7D,5141bc99)
// http://centerqi.github.io/hadoop/2013/04/11/hadoop-pig-loadFunc/

public class KoudaiLoader extends LoadFunc {
    protected RecordReader recordReader = null;
    private String fieldDel = "";
    private String[] reqFildList;
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static final int BUFFER_SIZE = 1024;

    public KoudaiLoader() {
    }

    public KoudaiLoader(String delimiter) {
        if (delimiter == null || delimiter.length() == 0) {
            throw new RuntimeException("empty delimiter");
        }
        this.reqFildList = delimiter.split(",");
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            boolean flag = recordReader.nextKeyValue();
            if (!flag) {
                return null;
            }

            Text value = (Text) recordReader.getCurrentValue();

            Map<String, String> tmpMap = this.sourceToMap(value.toString());
            if (tmpMap == null || tmpMap.size() == 0) {
                return null;
            }

            List lst = new ArrayList<String>();
            int i = 0;
            for (String s : this.reqFildList) {
                String item = tmpMap.get(s);
                if (item == null || item.length() == 0) {
                    item = "";
                }
                lst.add(i++, item);
            }
            return TupleFactory.getInstance().newTuple(lst);
        } catch (InterruptedException e) {
            throw new ExecException("Read data error", PigException.REMOTE_ENVIRONMENT, e);
        }
    }

    public Map<String, String> sourceToMap(String pline) {
        Map<String, String> mapLog = new HashMap<String, String>();
        if (pline == null || pline.length() <= 2) {
            return mapLog;
        }

        String line = pline;
        line = line.trim();
        line = line.substring(1, line.length() - 1);
        String[] strArr = line.split("\"\\|\"");
        if (strArr.length == 0) {
            return mapLog;
        }

        String[] strSubArr;
        for (String s : strArr) {
            if (s != null && s.length() != 0) {
                strSubArr = s.split(":", 2);
                if (strSubArr.length == 2) {
                    mapLog.put(strSubArr[0], strSubArr[1]);
                }
            }
        }
        return mapLog;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new PigTextInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit split) throws IOException {
        this.recordReader = recordReader;
    }
}
