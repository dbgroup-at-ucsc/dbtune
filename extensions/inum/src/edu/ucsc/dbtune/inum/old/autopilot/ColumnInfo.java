package edu.ucsc.dbtune.inum.old.autopilot;

public class ColumnInfo 
{
	public String colName;
	public String colType;

	public int attnum;
    public int atttypid;

    public int sysindexes_id;
    public int sysindexes_indid;
    public int col_size;
    public int precision;
    public int scale;
	public boolean nullable;
    public boolean is_pk;
    
    public ColumnInfo() {
    	sysindexes_id = 0;
        sysindexes_indid = 0;
        col_size = 0;
        precision = 0;
        scale = 0;
        nullable = false;
        is_pk = false;
    }
    
    public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

    public String getColType() {
		return colType;
	}

	public void setColType(String colType) {
		this.colType = colType;
	}

	public int getAttnum() {
		return attnum;
	}

	public void setAttnum(int attnum) {
		this.attnum = attnum;
	}

	public int getAtttypid() {
		return atttypid;
	}

	public void setAtttypid(int atttypid) {
		this.atttypid = atttypid;
	}

    public boolean isNullable() {
		return nullable;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

//    public String toString() {
//        return "\n" + colName + " " + attnum + " " + atttypid + " " + nullable +" \n";
//    }

	public String toString() {
        return colName;
    }
}

