package com.weirdwarriors.hbaseutils.hbaseutils;

import java.util.List;

public class PutValue 
{
	private int rowKey;
	private List<ColumnValue> columnValues;
	
	public PutValue(int rowKey, List<ColumnValue> columnValues)
	{
		this.rowKey = rowKey;
		this.columnValues = columnValues;
	}

	public int getRowKey() 
	{
		return rowKey;
	}

	public void setRowKey(int rowKey) 
	{
		this.rowKey = rowKey;
	}

	public List<ColumnValue> getColumnValues() 
	{
		return columnValues;
	}

	public void setColumnValues(List<ColumnValue> columnValues) 
	{
		this.columnValues = columnValues;
	}	
}
