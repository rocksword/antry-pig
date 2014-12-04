package com.an.antry.pig.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * @author adong
 *
 */
public class UPPER extends EvalFunc<String> {
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(
                new Schema.FieldSchema(null, DataType.CHARARRAY))));
        return funcList;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                DataType.CHARARRAY));
    }

    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        int size = input.getAll().size();
        for (int i = 0; i < size; i++) {
            System.out.println("type: " + input.getType(i) + ", val: " + input.get(i));
        }

        try {
            String str = (String) input.get(0);
            return str.toUpperCase();
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            return null;
        }
    }
}
