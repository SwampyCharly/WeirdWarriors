package com.weirdwarriors.hbaseutils.hbaseutils;

public class ColumnValue 
{
	private String rowKey;
	private String columnFamily;
	private String column;
	private String value;
	
	public ColumnValue(String columnFamily, String column)
	{
		this.columnFamily = columnFamily;
		this.column = column;
	}
	
	public ColumnValue(String columnFamily, String column, String value)
	{
		this.columnFamily = columnFamily;
		this.column = column;
		this.value = value;
	}

	public ColumnValue(String rowKey, String columnFamily, String column, String value)
	{
		this.rowKey = rowKey;
		this.columnFamily = columnFamily;
		this.column = column;
		this.value = value;
	}
	
	public void print()
	{
		System.out.println("RowKey: " + rowKey);
		System.out.println("ColumnFamily: " + columnFamily);
		System.out.println("Column: " + column);
		System.out.println("Value: " + value);
		System.out.println();
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
		return column;
	}

	public void setColumn(String column) 
	{
		this.column = column;
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
