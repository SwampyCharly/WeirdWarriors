package com.weirdwarriors.hbaseutils.lib;

public class ColumnValue 
{
	private String rowKey;
	private String columnFamily;
	private String qualifier;
	private String value;
	
	public ColumnValue(String columnFamily, String qualifier)
	{
		this.columnFamily = columnFamily;
		this.qualifier = qualifier;
	}
	
	public ColumnValue(String columnFamily, String qualifier, String value)
	{
		this.columnFamily = columnFamily;
		this.qualifier = qualifier;
		this.value = value;
	}

	public ColumnValue(String rowKey, String columnFamily, String qualifier, String value)
	{
		this.rowKey = rowKey;
		this.columnFamily = columnFamily;
		this.qualifier = qualifier;
		this.value = value;
	}
	
	public String toString()
	{		
		return rowKey + " - " + columnFamily + ":" + qualifier + " -> " + value;
	}

	public String getColumnFamily() 
	{
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) 
	{
		this.columnFamily = columnFamily;
	}

	public String getColumn() 
	{
		return qualifier;
	}

	public void setColumn(String qualifier) 
	{
		this.qualifier = qualifier;
	}

	public String getValue() 
	{
		return value;
	}

	public void setValue(String value) 
	{
		this.value = value;
	}
	
	public String getRowKey()
	{
		return rowKey;
	}

	public void setRowKey(String rowKey) 
	{
		this.rowKey = rowKey;
	}	
}
