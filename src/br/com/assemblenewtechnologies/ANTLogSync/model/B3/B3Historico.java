package br.com.assemblenewtechnologies.ANTLogSync.model.B3;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class B3Historico {
	private static Logger LOGGER =  LoggerFactory.getLogger(B3Historico.class);
	
	private int line_number;
	
	private int TIPREG; 
	private Date DATA_PREGAO; 
	private String CODBDI; 
	private String CODNEG; 
	private String TPMERC; 
	private String NOMRES; 
	private String ESPECI; 
	private String PRAZOT; 
	private String MODREF; 
	private Double PREABE; 
	private Double PREMAX; 
	private Double PREMIN; 
	private Double PREMED; 
	private Double PREULT; 
	private Double PREOFC; 
	private Double PREOFV; 
	private int TOTNEG; 
	private Long QUATOT; 
	private Long VOLTOT; 
	private Double PREEXE; 
	private int INDOPC; 
	private int DATVEN; 
	private int FATCOT; 
	private Double PTOEXE; 
	private String CODISI; 
	private int DISMES;
	private Connection connection;
	private int physicalLineCount;
	
	public B3Historico(Connection jdbcConnection2, int physicalLineCount2) {
		connection = jdbcConnection2;
		physicalLineCount = physicalLineCount2;
	}
	/**
	 * @return the tIPREG
	 */
	public int getTIPREG() {
		return TIPREG;
	}
	/**
	 * @param tIPREG the tIPREG to set
	 */
	public void setTIPREG(int tIPREG) {
		TIPREG = tIPREG;
	}
	/**
	 * @return the dATA_PREGAO
	 */
	public Date getDATA_PREGAO() {
		return DATA_PREGAO;
	}
	
	/**
	 * @return the dATA_PREGAO
	 */
	public String getDATA_PREGAO_String() {
		return DATA_PREGAO.toString();
	}
	/**
	 * @param dATA_PREGAO the dATA_PREGAO to set
	 * @throws ParseException 
	 */
	public void setDATA_PREGAO(int dATA_PREGAO) throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        java.util.Date parsed = format.parse(String.valueOf(dATA_PREGAO));
        java.sql.Date sql_date = new java.sql.Date(parsed.getTime());
//        String x = parsed.toString();
//        x = sql_date.toString();
		DATA_PREGAO = sql_date;
	}
	/**
	 * @return the cODBDI
	 */
	public String getCODBDI() {
		return CODBDI;
	}
	/**
	 * @param cODBDI the cODBDI to set
	 */
	public void setCODBDI(String cODBDI) {
		CODBDI = cODBDI;
	}
	/**
	 * @return the cODNEG
	 */
	public String getCODNEG() {
		return CODNEG;
	}
	/**
	 * @param cODNEG the cODNEG to set
	 */
	public void setCODNEG(String cODNEG) {
		CODNEG = cODNEG;
	}
	/**
	 * @return the tPMERC
	 */
	public String getTPMERC() {
		return TPMERC;
	}
	/**
	 * @param tPMERC the tPMERC to set
	 */
	public void setTPMERC(String tPMERC) {
		TPMERC = tPMERC;
	}
	/**
	 * @return the nOMRES
	 */
	public String getNOMRES() {
		return NOMRES;
	}
	/**
	 * @param nOMRES the nOMRES to set
	 */
	public void setNOMRES(String nOMRES) {
		NOMRES = nOMRES;
	}
	/**
	 * @return the eSPECI
	 */
	public String getESPECI() {
		return ESPECI;
	}
	/**
	 * @param eSPECI the eSPECI to set
	 */
	public void setESPECI(String eSPECI) {
		ESPECI = eSPECI;
	}
	/**
	 * @return the pRAZOT
	 */
	public String getPRAZOT() {
		return PRAZOT;
	}
	/**
	 * @param pRAZOT the pRAZOT to set
	 */
	public void setPRAZOT(String pRAZOT) {
		PRAZOT = pRAZOT;
	}
	/**
	 * @return the mODREF
	 */
	public String getMODREF() {
		return MODREF;
	}
	/**
	 * @param mODREF the mODREF to set
	 */
	public void setMODREF(String mODREF) {
		MODREF = mODREF;
	}
	/**
	 * @return the pREABE
	 */
	public Double getPREABE() {
		return PREABE;
	}
	/**
	 * @param pREABE the pREABE to set
	 */
	public void setPREABE(Double pREABE) {
		PREABE = pREABE/100;
	}
	/**
	 * @return the pREMAX
	 */
	public Double getPREMAX() {
		return PREMAX;
	}
	/**
	 * @param pREMAX the pREMAX to set
	 */
	public void setPREMAX(Double pREMAX) {
		PREMAX = pREMAX/100;
	}
	/**
	 * @return the pREMIN
	 */
	public Double getPREMIN() {
		return PREMIN;
	}
	/**
	 * @param pREMIN the pREMIN to set
	 */
	public void setPREMIN(Double pREMIN) {
		PREMIN = pREMIN/100;
	}
	/**
	 * @return the pREMED
	 */
	public Double getPREMED() {
		return PREMED;
	}
	/**
	 * @param pREMED the pREMED to set
	 */
	public void setPREMED(Double pREMED) {
		PREMED = pREMED/100;
	}
	/**
	 * @return the pREULT
	 */
	public Double getPREULT() {
		return PREULT;
	}
	/**
	 * @param pREULT the pREULT to set
	 */
	public void setPREULT(Double pREULT) {
		PREULT = pREULT/100;
	}
	/**
	 * @return the pREOFC
	 */
	public Double getPREOFC() {
		return PREOFC;
	}
	/**
	 * @param pREOFC the pREOFC to set
	 */
	public void setPREOFC(Double pREOFC) {
		PREOFC = pREOFC/100;
	}
	/**
	 * @return the pREOFV
	 */
	public Double getPREOFV() {
		return PREOFV;
	}
	/**
	 * @param pREOFV the pREOFV to set
	 */
	public void setPREOFV(Double pREOFV) {
		PREOFV = pREOFV/100;
	}
	/**
	 * @return the tOTNEG
	 */
	public int getTOTNEG() {
		return TOTNEG;
	}
	/**
	 * @param tOTNEG the tOTNEG to set
	 */
	public void setTOTNEG(int tOTNEG) {
		TOTNEG = tOTNEG;
	}
	/**
	 * @return the qUATOT
	 */
	public Long getQUATOT() {
		return QUATOT;
	}
	/**
	 * @param qUATOT the qUATOT to set
	 */
	public void setQUATOT(Long qUATOT) {
		QUATOT = qUATOT;
	}
	/**
	 * @return the vOLTOT
	 */
	public Long getVOLTOT() {
		return VOLTOT;
	}
	/**
	 * @param vOLTOT the vOLTOT to set
	 */
	public void setVOLTOT(Long vOLTOT) {
		VOLTOT = vOLTOT/100;
	}
	/**
	 * @return the pREEXE
	 */
	public Double getPREEXE() {
		return PREEXE;
	}
	/**
	 * @param pREEXE the pREEXE to set
	 */
	public void setPREEXE(Double pREEXE) {
		PREEXE = pREEXE/100;
	}
	/**
	 * @return the iNDOPC
	 */
	public int getINDOPC() {
		return INDOPC;
	}
	/**
	 * @param iNDOPC the iNDOPC to set
	 */
	public void setINDOPC(int iNDOPC) {
		INDOPC = iNDOPC;
	}
	/**
	 * @return the dATVEN
	 */
	public int getDATVEN() {
		return DATVEN;
	}
	/**
	 * @param dATVEN the dATVEN to set
	 */
	public void setDATVEN(int dATVEN) {
		DATVEN = dATVEN;
	}
	/**
	 * @return the fATCOT
	 */
	public int getFATCOT() {
		return FATCOT;
	}
	/**
	 * @param fATCOT the fATCOT to set
	 */
	public void setFATCOT(int fATCOT) {
		FATCOT = fATCOT;
	}
	/**
	 * @return the pTOEXE
	 */
	public Double getPTOEXE() {
		return PTOEXE;
	}
	/**
	 * @param pTOEXE the pTOEXE to set
	 */
	public void setPTOEXE(Double pTOEXE) {
		PTOEXE = pTOEXE/1000000;
	}
	/**
	 * @return the cODISI
	 */
	public String getCODISI() {
		return CODISI;
	}
	/**
	 * @param cODISI the cODISI to set
	 */
	public void setCODISI(String cODISI) {
		CODISI = cODISI;
	}
	/**
	 * @return the dISMES
	 */
	public int getDISMES() {
		return DISMES;
	}
	/**
	 * @param dISMES the dISMES to set
	 */
	public void setDISMES(int dISMES) {
		DISMES = dISMES;
	}

	
	public void save() {
		try {
			String sql = "INSERT INTO B3Log.B3Historical " +
					"("
					+"TIPREG," 
					+"DATA_PREGAO,"
					+ "CODBDI, "
					+ "CODNEG, "
					+ "TPMERC, "
					+ "NOMRES, "
					+ "ESPECI, "
					+ "PRAZOT, "
					+ "MODREF, "
					+ "PREABE, "
					+ "PREMAX, "
					+ "PREMIN, "
					+ "PREMED, "
					+ "PREULT, "
					+ "PREOFC, "
					+ "PREOFV, "
					+ "TOTNEG, "
					+ "QUATOT, "
					+ "VOLTOT, "
					+ "PREEXE, "
					+ "INDOPC, "
					+ "DATVEN, "
					+ "FATCOT, "
					+ "PTOEXE, "
					+ "CODISI, "
					+ "DISMES"
					+ ") " 
	                   
	                +   "VALUES (" +
	                   this.getTIPREG() + "," + 
	                   "'"+this.getDATA_PREGAO_String() + "'," + 
	                   "'"+this.getCODBDI() + "'," + 
	                   "'"+this.getCODNEG() + "'," + 
	                   "'"+this.getTPMERC() + "'," + 
	                   "'"+this.getNOMRES() + "'," + 
	                   "'"+this.getESPECI() + "'," + 
	                   "'"+this.getPRAZOT() + "'," + 
	                   "'"+this.getMODREF() + "'," + 
	                   this.getPREABE() + "," + 
	                   this.getPREMAX() + "," + 
	                   this.getPREMIN() + "," + 
	                   this.getPREMED() + "," + 
	                   this.getPREULT() + "," + 
	                   this.getPREOFC() + "," + 
	                   this.getPREOFV() + "," + 
	                   this.getTOTNEG() + "," + 
	                   this.getQUATOT() + "," + 
	                   this.getVOLTOT() + "," + 
	                   this.getPREEXE() + "," + 
	                   this.getINDOPC() + "," + 
	                   this.getDATVEN() + "," + 
	                   this.getFATCOT() + "," + 
	                   this.getPTOEXE() + "," + 
	                   "'"+this.getCODISI() + "'," + 
	                   this.getDISMES() + 

	                   ")";
			
			PreparedStatement statement = connection.prepareStatement(sql);
			statement.execute();
			
		} catch (SQLException e) {
			LOGGER.error("Error on saveing B3 Historical tuple on line: " + physicalLineCount);
			e.printStackTrace();
		} //DEBUG REMOVE LIMIT
//		timer2 = System.currentTimeMillis();
	}
}
