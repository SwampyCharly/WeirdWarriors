package com.weirdwarriors.hbaseutils.hbaseutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

public class App 
{
	
    public static void main( String[] args )
    {
        PutHBase configurationhbase = new PutHBase();
        GetHBase gethbase = new GetHBase();
        
        String namespaceString = "ns1";
        String tableString = "prueba";
        
        List<String> columnDescriptors = new ArrayList<String>();
        columnDescriptors.add("mum");
        columnDescriptors.add("dad");
        
        List<PutValue> putValues = new ArrayList<PutValue>();
        
        List<ColumnValue> columnValuesOutput = new ArrayList<ColumnValue>();
        
        List<ColumnValue> columnValues = new ArrayList<ColumnValue>();
        ColumnValue cv = new ColumnValue("mum", "name", "Josefina");
        columnValues.add(cv);
        cv = new ColumnValue("mum", "age", "44");
        columnValues.add(cv);
        cv = new ColumnValue("mum", "status", "single");
        columnValues.add(cv);
        cv = new ColumnValue("dad", "name", "Joselis");
        columnValues.add(cv);
        cv = new ColumnValue("dad", "age", "22");
        columnValues.add(cv);
        cv = new ColumnValue("dad", "status", "married");
        columnValues.add(cv);
        
        PutValue pv = new PutValue(0, columnValues);        
        putValues.add(pv);

        columnValues = new ArrayList<ColumnValue>();        
        cv = new ColumnValue("mum", "name", "AAA");
        columnValues.add(cv);
        cv = new ColumnValue("mum", "age", "11");
        columnValues.add(cv);
        cv = new ColumnValue("mum", "status", "widow");
        columnValues.add(cv);
        cv = new ColumnValue("dad", "name", "BBB");
        columnValues.add(cv);
        cv = new ColumnValue("dad", "age", "11");
        columnValues.add(cv);
        cv = new ColumnValue("dad", "status", "dead");
        columnValues.add(cv);
        
        pv = new PutValue(1, columnValues);
        putValues.add(pv);
        
        columnValues = new ArrayList<ColumnValue>();        
        cv = new ColumnValue("mum", "name", "CCC");
        columnValues.add(cv);
        cv = new ColumnValue("mum", "age", "32");
        columnValues.add(cv);
        cv = new ColumnValue("mum", "status", "motherfucked");
        columnValues.add(cv);
        cv = new ColumnValue("dad", "name", "DDD");
        columnValues.add(cv);
        cv = new ColumnValue("dad", "age", "23");
        columnValues.add(cv);
        cv = new ColumnValue("dad", "status", "fatherfucked");
        columnValues.add(cv);
        
        pv = new PutValue(2, columnValues);
        putValues.add(pv);
        
        try {
			configurationhbase.put(namespaceString, tableString, columnDescriptors, putValues);
			
			columnValuesOutput = gethbase.getValue(namespaceString, tableString, columnValues, "1");
			
			for (int i = 0; i < columnValuesOutput.size(); i++)
			{
				columnValuesOutput.get(i).print();
			}
			
			gethbase.scan(namespaceString, tableString, columnValues);
			
			//gethbase.scan(namespaceString, tableString, columnValues, null, "1", "2");
			
			List<ColumnValue> columnValues1 = new ArrayList<ColumnValue>();
	        ColumnValue cv1 = new ColumnValue("mum", "name", null);
	        columnValues1.add(cv1);
			
			FilterList fl = new FilterList();
			Filter fJ = new SingleColumnValueFilter("dad".getBytes(), "name".getBytes(), CompareOp.EQUAL, "BBB".getBytes());
			
			fl.addFilter(fJ);
			
			gethbase.scan(namespaceString, tableString, columnValues1, fl, null, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
}
