package br.com.assemblenewtechnologies.ANTLogSync.constants;

public class ErrorCodes {
	public static final int STARTUP_ERROR = 0;
	public static final int GET_MACHINESTATE_ERROR = 010;
	public static final int RECOVER_MACHINESTATE_ERROR = 020;
	public static final int CSV_LOAD_PATH_ERROR = 100;
	public static final int CSV_LOAD_PERMISSION_ERROR = 101;
	public static final int CSV_INTEGRETY_DATA_ERROR = 102;
	public static final int CSV_ARCHIVE_PATH_ERROR = 103;
	public static final int CSV_ARCHIVE_PERMISSION_ERROR = 104;
	public static final int CSV_ARCHIVE_ZIP_ERROR = 105;
	public static final int DB_CONNECT_ERROR = 200;
	public static final int DB_EXCEPTION_ERROR = 201;
	public static final int ETL_SANITY_DROPSTEP_ERROR = 300;
	public static final int ETL_SANITY_INFO_ERROR = 301;
	public static final int ETL_POPULATE_ASSETS_ERROR = 302;
	public static final int ETL_NORMALIZATION_ERROR = 303;
	public static final int ETL_PRECO_MEDIO_ERROR = 304;
	public static final int ETL_BLACKSCHOLES_ERROR = 305;

}
