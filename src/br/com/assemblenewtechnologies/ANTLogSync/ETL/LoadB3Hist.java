package br.com.assemblenewtechnologies.ANTLogSync.ETL;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;

import org.sadun.text.ffp.FFPParseException;
import org.sadun.text.ffp.FlatFileParser;
import org.sadun.text.ffp.LineFormat;
import org.sadun.text.ffp.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.Helpers.DBConnectionHelper;
import br.com.assemblenewtechnologies.ANTLogSync.model.B3.B3Historico;


public class LoadB3Hist {

	private final static String FILE_TO_PARSE = "/home/mcloc/Documents/ANT-resources/historical-data/tmp/COTAHIST_A2021.TXT";
	private static Logger LOGGER =  LoggerFactory.getLogger(LoadB3Hist.class);
	private static long start_time;
	private static Connection connection;
	
	
	public static void main(String[] args) throws Exception {
		try {
			connection = DBConnectionHelper.getNewConn();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new Exception("No database connection...");
			
		}
		start_time = System.currentTimeMillis();
		LOGGER.info("Initializing B3 Historical Loader.");
		LOGGER.info("File to load: ." + FILE_TO_PARSE);
		
		FlatFileParser parser;
		try {
			// Declare and define the line format(s)
			LineFormat format = new LineFormat(); // The format is named "test format"..

			// ..has one header of 40 characters..
//			format.defineNextField("header", 245);
			// ..has and a constant TEST0000
//			format.defineNextField("transaction code", 48, Type.CONSTANT, "COTAHIST");
			// ... further definition of format ...

//			format.defineField(0, 5); // declares a field from position 0 to 4 (length 5)
//			format.defineField(5, 7); // declares a field at positions 5 and 6 (length 2)
//			format.defineField(7, 12); // declares a field from position 7 to 11 (length 5)

			format.defineNextField("TIPREG", 2, Type.NUMERIC); // END on column 1 (0 and 1)
			format.defineNextField("DATA_PREGAO", 10, Type.NUMERIC); // END on column 9 (2 to 9)
			format.defineNextField("CODBDI", 12, Type.ALFA); // END on column 11 (10 to 11)
			format.defineNextField("CODNEG", 24, Type.ALFA); // END on column 23 (12 to 23)
			format.defineNextField("TPMERC", 27, Type.NUMERIC); // END on column 26 (24 to 26)
			format.defineNextField("NOMRES", 39, Type.ALFA); // END on column 38 (27 to 38)
			format.defineNextField("ESPECI", 49, Type.ALFA); // END on column 48 (39 to 48)
			format.defineNextField("PRAZOT", 52, Type.ALFA); // END on column 51 (49 to 51)
			format.defineNextField("MODREF", 56, Type.ALFA); // END on column 55 (52 to 55)
			format.defineNextField("PREABE", 69, Type.NUMERIC); // END on column 68 (56 to 68)
			format.defineNextField("PREMAX", 82, Type.NUMERIC); // END on column 81 (69 to 81)
			format.defineNextField("PREMIN", 95, Type.NUMERIC); // END on column 94 (82 to 94)
			format.defineNextField("PREMED", 108, Type.NUMERIC); // END on column 107 (95 to 107)
			format.defineNextField("PREULT", 121, Type.NUMERIC); // END on column 120 (108 to 120)
			format.defineNextField("PREOFC", 134, Type.NUMERIC); // END on column 133 (121 to 133)
			format.defineNextField("PREOFV", 147, Type.NUMERIC); // END on column 146 (134 to 146)
			format.defineNextField("TOTNEG", 152, Type.NUMERIC); // END on column 151 (147 to 151)
			format.defineNextField("QUATOT", 170, Type.NUMERIC); // END on column 169 (152 to 169)
			format.defineNextField("VOLTOT", 188, Type.NUMERIC); // END on column 187 (170 to 187)
			format.defineNextField("PREEXE", 201, Type.NUMERIC); // END on column 200 (188 to 200)
			format.defineNextField("INDOPC", 202, Type.NUMERIC); // END on column 201 (201 to 201)
			format.defineNextField("DATVEN", 210, Type.NUMERIC); // END on column 209 (202 to 209)
			format.defineNextField("FATCOT", 217, Type.NUMERIC); // END on column 216 (210 to 216)
			format.defineNextField("PTOEXE", 230, Type.NUMERIC); // END on column 229 (217 to 229)
			format.defineNextField("CODISI", 242, Type.ALFA); // END on column 241 (230 to 241)
			format.defineNextField("DISMES", 246, Type.NUMERIC); // END on column 244 (242 to 245)

			 
			
			parser = new FlatFileParser(format);
			parser.setAutoTrimMode(true);
			
			parser.addListener(new FlatFileParser.Listener() {
				  public void lineParsed(LineFormat format,int logicalLinecount,int physicalLineCount,String[] values) {
					  B3Historico b3_hist = new B3Historico(connection, physicalLineCount);

					  b3_hist.setTIPREG(Integer.valueOf(values[0])); 
					  try {
						b3_hist.setDATA_PREGAO(Integer.valueOf(values[1]));
					} catch (NumberFormatException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
					  b3_hist.setCODBDI(values[2]); 
					  b3_hist.setCODNEG(values[3]); 
					  b3_hist.setTPMERC(values[4]); 
					  b3_hist.setNOMRES(values[5]); 
					  b3_hist.setESPECI(values[6]); 
					  b3_hist.setPRAZOT(values[7]); 
					  b3_hist.setMODREF(values[8]); 
					  b3_hist.setPREABE(Double.valueOf(values[9])); 
					  b3_hist.setPREMAX(Double.valueOf(values[19]));
					  b3_hist.setPREMIN(Double.valueOf(values[11]));
					  b3_hist.setPREMED(Double.valueOf(values[12]));
					  b3_hist.setPREULT(Double.valueOf(values[13]));
					  b3_hist.setPREOFC(Double.valueOf(values[14]));
					  b3_hist.setPREOFV(Double.valueOf(values[15]));
					  b3_hist.setTOTNEG(Integer.valueOf(values[16]));
					  b3_hist.setQUATOT(Long.valueOf(values[17]));
					  b3_hist.setVOLTOT(Long.valueOf(values[18]));
					  b3_hist.setPREEXE(Double.valueOf(values[19]));
					  b3_hist.setINDOPC(Integer.valueOf(values[20]));
					  b3_hist.setDATVEN(Integer.valueOf(values[21]));
					  b3_hist.setFATCOT(Integer.valueOf(values[22]));
					  b3_hist.setPTOEXE(Double.valueOf(values[23]));
					  b3_hist.setCODISI(values[24]); 
					  b3_hist.setDISMES(Integer.valueOf(values[25]));

					  
					  b3_hist.save();
					  
//				    for(int i=0;i<values.length;i++) {
//				    	System.out.println(values[i]);
				    	
//				    }
				  }
			});
			
			
//			parser.addListener(new EchoListener());
			parser.parse(new File(FILE_TO_PARSE));
		} catch (IOException | FFPParseException e1) {
			e1.printStackTrace();
		}

	}
	


}
