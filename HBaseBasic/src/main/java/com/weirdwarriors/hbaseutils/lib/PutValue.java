package com.weirdwarriors.hbaseutils.lib;

import java.util.List;

public class PutValue 
{
	private String rowKey;
	private List<ColumnValue> columnValues;
	
	public PutValue(String rowKey, List<ColumnValue> columnValues)
	{
		this.rowKey = rowKey;
		this.columnValues = columnValues;
	}

	public String getRowKey() 
	{
		return rowKey;
	}

	public void setRowKey(String rowKey) 
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
