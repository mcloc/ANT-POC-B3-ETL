package br.com.assemblenewtechnologies.ANTLogSync.rotines;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.GlobalProperties;
import br.com.assemblenewtechnologies.ANTLogSync.Helpers.DBConnectionHelper;
import br.com.assemblenewtechnologies.ANTLogSync.Helpers.MathHelper;
import br.com.assemblenewtechnologies.ANTLogSync.model.CsvLoadLot;
import br.com.assemblenewtechnologies.ANTLogSync.process_handlers.ETLHandler;

public class Etl extends AbstractRotine {
	private static Logger LOGGER = LoggerFactory.getLogger(Etl.class);
	private long start_time;
	private String thread_name = "ETL_HANDLER";
	private Thread thread;
	private ETLHandler runnable;

	private static final int BULK_BATCH_INSERT_SIZE = 100000;
	private static final int BULK_BATCH_FETCH_SIZE = 150000;

	public Etl() throws Exception {
		try {
			DBConnectionHelper.getETLConn().setAutoCommit(true);
		} catch (Exception e) {
			e.printStackTrace();
			DBConnectionHelper.getETLConn().close();
			throw new Exception("No database DBConnectionHelper.getETLConn()...");
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					DBConnectionHelper.closeETLConn();
				} catch (Exception e) {
					LOGGER.error("ETL processment shutdown error");
					LOGGER.error(e.getMessage(), e);
				}
			}
		});
	}

	public void etl0_sanity_check() throws Exception {
		// verificar se DATA RELOGIO BATE COM DATA LOTE isso resolve sanity de
		// mes no dia e dia no mes
		// (rejeita ida pra hot_table)

		// verificar ordem de grandeza dos numeros
		// (rejeita ida pra hot_table)

		// verificar dropout (mais conhecido por dropstep)
		// (aceita ida pra hot_table e LOGA o DROPOUT. Tem uma tabela so de droupout
		// cas rastreabilidade do load LOT e arquivo)

		LOGGER.info("[ETL] etl0_sanity_check...");
	}

	public void etl1_populate_assets(BigDecimal csv_lot_id) throws Exception {
		CsvLoadLot csv_lot = CsvLoadLot.getLotByLotId(csv_lot_id);
		// ------------------- DEBUG
		// INCREMENT CSV LOT STATUS FINISHED TO +1
//		int csv_status = csv_lot.getStatus();
//		if (csv_status < 0)
//			csv_lot.changeStatus(csv_lot.getStatus() - 1);
//		else
//			csv_lot.changeStatus(csv_lot.getStatus() + 1);
//		
//		if(1==1)
//			return;
		// --------------------------------------
		long start_time = System.currentTimeMillis();
		LOGGER.debug("Initializing B3 SignalLogger ETL phase 1 - 'etl1_populate_assets' ...");

		Statement stmt;
		ResultSet rs;

		Map<String, String> ativo_opcoes_db = getAssetsInDB();

		// FOR EACH FINISHED LOT

		try {
			LOGGER.info("Fetching assets from B3Log.B3SignalLoggerRawLotBuffer:");
			stmt = DBConnectionHelper.getETLConn().createStatement(ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery("select asset, substring(asset, '[A-Z]+') as substr_ativo "
					+ "from B3Log.B3SignalLoggerRawLotBuffer  " + "WHERE strike = 0" + "and lot_id = " + csv_lot_id
					+ "  " + "group by 1,2" + "order by 1,2");
			int rows = 0;
//				if (rs.last()) {
//					rows = rs.getRow();
//					rs.beforeFirst();
//					LOGGER.debug("total assets found " + rows + " for this lot: " + csv_lot.getLot_name());
//
//					if (rows == 0)
//						return;
//				}
			List<String> _inserted_ativo = new ArrayList<String>();
			Map<String, String> _ativos = new HashMap<String, String>();
			while (rs.next()) {
				if (rs.getString("asset").equals("B3SA3"))
					continue;

				// Assets and Option already on DB
				if (ativo_opcoes_db.containsKey(rs.getString("asset")))
					continue;
				_ativos.put(rs.getString("asset"), rs.getString("substr_ativo"));
			}

			stmt = DBConnectionHelper.getETLConn().createStatement(ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery("select asset as opcao " + "from B3Log.B3SignalLoggerRawLotBuffer  "
					+ "WHERE strike != 0" + "and lot_id = " + csv_lot_id + "  " + "group by 1" + "order by 1");
//				rows = 0;
//				if (rs.last()) {
//					rows = rs.getRow();
//					rs.beforeFirst();
//					LOGGER.info("total asset options found " + rows);
//
//					if (rows == 0)
//						return;
//				}

			List<String> _ativos_options = new ArrayList<String>();
			while (rs.next()) {
				if (rs.getString("opcao").equals("B3SA3"))
					continue;

				// Asset and Option already on DB
				if (ativo_opcoes_db.containsKey(rs.getString("opcao")))
					continue;

				_ativos_options.add(rs.getString("opcao"));
			}

			Iterator<Map.Entry<String, String>> itr = _ativos.entrySet().iterator();
			while (itr.hasNext()) {
				Map.Entry<String, String> entry = itr.next();
				String _ativo = entry.getKey();
				String _ativo_substr = entry.getValue();
				insertNewAssetsInDB(_inserted_ativo, _ativos_options, _ativo, _ativo_substr);
			}

			// TODO: INSERT ASSETS WITH NO DERIVATIVES (strike = 0 and none != 0) they are
			// missing
//		        itr = _ativos.entrySet().iterator();
//		        while(itr.hasNext())
//		        {
//		        	if(_inserted_ativo.add(_ativo);)
//		        }
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			// e.printStackTrace();
			throw e;
		}

		// INCREMENT CSV LOT STATUS FINISHED TO +1
		int csv_status = csv_lot.getStatus();
		if (csv_status < 0)
			csv_lot.changeStatus(csv_lot.getStatus() - 1);
		else
			csv_lot.changeStatus(csv_lot.getStatus() + 1);

		long _now = System.currentTimeMillis();
		long _diff_time = _now - start_time;

	}

	private void populateRawLotBufferFromLot(BigDecimal csv_lot_id) throws Exception {
		PreparedStatement preparedStatement;
//		PreparedStatement preparedStatement = DBConnectionHelper.getETLConn()
//				.prepareStatement("TRUNCATE B3Log.B3SignalLoggerRawLotBuffer");
//		preparedStatement.execute();
		CsvLoadLot csv_lot = CsvLoadLot.getLotByLotId(csv_lot_id);
		try {
			DBConnectionHelper.closeETLConn();
			LOGGER.info("[ETL] DROP INDEX b3signalloggerbuffer_asset_idx from B3SignalLoggerRawLotBuffer asset.");
			preparedStatement = DBConnectionHelper.getETLConn()
					.prepareStatement("DROP INDEX IF EXISTS B3Log.b3signalloggerbuffer_asset_idx;\n");
			preparedStatement.execute();
			LOGGER.info("[ETL] DROP AND RECREATE :TABLE  B3SignalLoggerRawLotBuffer.");
			preparedStatement = DBConnectionHelper.getETLConn()
					.prepareStatement("DROP TABLE IF EXISTS B3Log.B3SignalLoggerRawLotBuffer;\n");
			preparedStatement.execute();
			preparedStatement = DBConnectionHelper.getETLConn()
					.prepareStatement("CREATE TABLE B3Log.B3SignalLoggerRawLotBuffer (\n"
							+ "                id BIGINT  NOT NULL,\n" + "                asset VARCHAR(11) NOT NULL,\n"
							+ "                data DATE NOT NULL,\n" + "                hora TIME NOT NULL,\n"
							+ "                ultimo NUMERIC(8,2) NOT NULL,\n"
							+ "                strike NUMERIC(8,2) NOT NULL,\n"
							+ "                negocios INTEGER NOT NULL,\n"
							+ "                quantidade INTEGER NOT NULL,\n"
							+ "                volume NUMERIC(18,2) NOT NULL,\n"
							+ "                oferta_compra NUMERIC(8,2) NOT NULL,\n"
							+ "                oferta_venda NUMERIC(8,2) NOT NULL,\n"
							+ "                VOC INTEGER NOT NULL,\n" + "                VOV INTEGER NOT NULL,\n"
							+ "                vencimento DATE NOT NULL,\n"
							+ "                validade DATE NOT NULL,\n"
							+ "                contratos_abertos BIGINT NOT NULL,\n"
							+ "                estado_atual VARCHAR(80) NOT NULL,\n"
							+ "                relogio TIMESTAMP NOT NULL,\n"
							+ "		 	 lot_name VARCHAR(70) NOT NULL,\n" + "		      lot_id BIGINT  NOT NULL,\n"
							+ "                CONSTRAINT b3signalloggerrawbuffer_pk PRIMARY KEY (id),\n"
							+ "                CONSTRAINT fk_B3SignalLoggerrawbuffer_lot_id FOREIGN KEY(lot_id) \n"
							+ "	  REFERENCES Intellect.csv_load_lot(id)\n" + ");\n"
							+ "ALTER TABLE B3Log.B3SignalLoggerRawLotBuffer SET (autovacuum_enabled = false); \n"
							+ "ALTER TABLE B3Log.B3SignalLoggerRawLotBuffer SET UNLOGGED;");
			preparedStatement.execute();
			if (!DBConnectionHelper.getETLConn().getAutoCommit())
				DBConnectionHelper.getETLConn().commit();
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
		LOGGER.info("[ETL] inserting raw lot buffer table for lot:" + csv_lot.getLot_name());
		String sql = "INSERT INTO B3Log.B3SignalLoggerRawLotBuffer ("
				+ "id,asset,data,  hora,  ultimo, strike, negocios, quantidade, volume, oferta_compra,oferta_venda, "
				+ "VOC, VOV, vencimento, validade, contratos_abertos,estado_atual, relogio, lot_name, lot_id" + ") "
				+ "select id,"
				+ "asset,data,  hora,  ultimo, strike, negocios, quantidade, volume, oferta_compra,oferta_venda, "
				+ "VOC, VOV, vencimento, validade, contratos_abertos,estado_atual, relogio, lot_name, lot_id "
				+ "from B3Log.B3SignalLoggerRaw  " + "WHERE lot_id = " + csv_lot_id + "  " + "order by relogio";

		preparedStatement = DBConnectionHelper.getETLConn().prepareStatement(sql);
		preparedStatement.execute();
//		String sql = "INSERT INTO B3Log.B3SignalLoggerRawLotBuffer_asset_tmp ("
//				+ "id,asset,data,  hora,  ultimo, strike, negocios, quantidade, volume, oferta_compra,oferta_venda, "
//				+ "VOC, VOV, vencimento, validade, contratos_abertos,estado_atual, relogio, lot_name, lot_id"
//				+ ") " + "select id,"
//				+ "asset,data,  hora,  ultimo, strike, negocios, quantidade, volume, oferta_compra,oferta_venda, "
//				+ "VOC, VOV, vencimento, validade, contratos_abertos,estado_atual, relogio, lot_name, lot_id "
//				+ "from B3Log.B3SignalLoggerRaw  " + "WHERE lot_id = " + csv_lot_id + " AND strike == 0 " + "order by relogio";
//		
//		preparedStatement = DBConnectionHelper.getETLConn()
//				.prepareStatement(sql);
//		preparedStatement.execute();
		if (!DBConnectionHelper.getETLConn().getAutoCommit())
			DBConnectionHelper.getETLConn().commit();

		LOGGER.info("[ETL] CLUSTERING RAW DATA INDEX");
		clusterRawDataIndex();

	}

	private void clusterRawDataIndex() {
		long _start_time = System.currentTimeMillis();
		PreparedStatement preparedStatement;
		try {
			LOGGER.info("[ETL] CREATE INDEX b3signalloggerbuffer_asset_idx on B3SignalLoggerRawLotBuffer asset.");
			preparedStatement = DBConnectionHelper.getETLConn()
					.prepareStatement("DROP INDEX IF EXISTS b3signalloggerbuffer_asset_idx;\n"
							+ "CREATE INDEX b3signalloggerbuffer_asset_idx\n"
							+ " ON B3Log.B3SignalLoggerRawLotBuffer USING BTREE\n" + " ( asset ASC );\n"
							+ "CLUSTER b3signalloggerbuffer_asset_idx ON B3Log.B3SignalLoggerRawLotBuffer;\n");
			preparedStatement.execute();
			if (!DBConnectionHelper.getETLConn().getAutoCommit())
				DBConnectionHelper.getETLConn().commit();
			long timer4 = System.currentTimeMillis();
			long diff_time = timer4 - _start_time;
			LOGGER.info("[ETL] clustering table B3SignalLoggerRawLotBuffer asset index total time: " + diff_time);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		}
	}

	private void insertNewAssetsInDB(List<String> _inserted_ativo, List<String> _ativos_options, String _ativo,
			String _ativo_substr) throws Exception {
		PreparedStatement preparedStatement;
		for (String _option : _ativos_options) {
			if (_option.contains(_ativo_substr)) {

				String sql = "INSERT INTO B3Log.B3AtivosOpcoes(" + "ativo,substr_opcao_ativo,opcao_ativo)"
						+ "VALUES (?,?,?)";
//		    					LOGGER.info(sql);

				preparedStatement = DBConnectionHelper.getETLConn().prepareStatement(sql,
						Statement.RETURN_GENERATED_KEYS);
				preparedStatement.setString(1, _ativo);
				preparedStatement.setString(2, _ativo_substr);
				preparedStatement.setString(3, _option);
				preparedStatement.execute();
				if (!DBConnectionHelper.getETLConn().getAutoCommit())
					DBConnectionHelper.getETLConn().commit();
				_inserted_ativo.add(_ativo);
			}
		}
	}

	private Map<String, String> getAssetsInDB() throws Exception {
		Statement stmt;
		ResultSet rs;
		// GET ASSESTS ALREADY IN DB
		stmt = DBConnectionHelper.getETLConn().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		rs = stmt.executeQuery("select * from B3Log.B3AtivosOpcoes");
		Map<String, String> ativo_opcoes_db = new HashMap<String, String>();
		while (rs.next()) {
			ativo_opcoes_db.put(rs.getString("ativo"), rs.getString("opcao_ativo"));
		}
		return ativo_opcoes_db;
	}

	public void etl1_normalization(BigDecimal csv_lot_id) throws Exception {
		Long start_time = System.currentTimeMillis();

		Map<String, Object> asset_book_values;

		List<String> ativos_list = new LinkedList<String>();
//		List<BigDecimal> csv_lot_finished = CsvLoadLot.getFinishedLotsAssetsExtracted();

		LOGGER.info("[ETL] etl1_normalization...");
		CsvLoadLot csv_lot = CsvLoadLot.getLotByLotId(csv_lot_id);
		LOGGER.info("Fetching assets from B3Log.B3SignalLoggerRawLotBuffer for this lot:" + csv_lot.getLot_name());
		Statement stmt = DBConnectionHelper.getETLConn().createStatement(ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
		ResultSet rs = stmt
				.executeQuery("select asset, substring(asset, '[A-Z]+') from B3Log.B3SignalLoggerRawLotBuffer a "
						+ "WHERE lot_id = " + csv_lot_id + " AND strike = 0 group by 1,2 order by 1,2");
		while (rs.next()) {
			ativos_list.add(rs.getString("asset"));
		}

		if (ativos_list.size() == 0) {
			LOGGER.error("No mapped Assets found on B3Log.B3AtivosOpcoes");
			int csv_status = csv_lot.getStatus();
			if (csv_status < 0)
				csv_lot.changeStatus(csv_lot.getStatus() - 1);
			else
				csv_lot.changeStatus(csv_lot.getStatus() + 1);

			return;
		}

		LOGGER.info("etl1_normalization(): Total assets to normalize on this lot:" + csv_lot.getLot_name() + " - "
				+ ativos_list.size());

		try {
			normalizeAssets(ativos_list, csv_lot);
			// INSERT DERIVATIVES into hot_table_derivates normalized
//			normalizeDerivatives(ativos_list, csv_lot);
			// INSERT DERIVATIVES into hot_table_assets normalized

			// INCREMENT CSV LOT STATUS FINISHED TO +1
			int csv_status = csv_lot.getStatus();
			if (csv_status < 0)
				csv_lot.changeStatus(csv_lot.getStatus() - 1);
			else
				csv_lot.changeStatus(csv_lot.getStatus() + 1);

			LOGGER.info("========================================================");
			LOGGER.info("ETL Normalization Finished for this lot: " + csv_lot.getLot_name() + " actual status: "
					+ csv_lot.getStatus());
			LOGGER.info("ETL Total time to normalize this lot: " + (start_time / 1000) + " sec");
			LOGGER.info("========================================================");
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return;
		}
	}

	private void normalizeAssets(List<String> ativos_list, CsvLoadLot csv_lot) throws SQLException {
		Map<String, Map<String, Object>> buffer_last_values = new HashMap<String, Map<String, Object>>();
		Map<String, Object> asset_book_values;
		// FOR EACH FINISHED LOT

		LOGGER.debug("normalizeAssets(): Total assets for lot:" + csv_lot.getLot_name() + " - " + ativos_list.size());
		PreparedStatement preparedStatement;
		Statement stmt;
		ResultSet rs;
		int _ativo_counter = 0;
		Connection _conn_chunks = null;
		try {
			_conn_chunks = DBConnectionHelper.getNewNonStaticConn();
			_conn_chunks.setAutoCommit(false);
			// FOREACH ASSET STRIK = 0 (ATIVOS)
			for (String _ativo : ativos_list) {

				if (GlobalProperties.getInstance().hasDerivatives(_ativo))
					normalizeDerivatives(_ativo, csv_lot);

				// FIXME REGEX QUEBRADO COM ESSE CARA
				if (_ativo.equals("B3SA3"))
					continue;

				LOGGER.info("Fetching raw data from B3Log.B3SignalLoggerRawLotBuffer:");
				LOGGER.info("asset (" + _ativo_counter + "):" + _ativo + " lot: " + csv_lot.getLot_name());

				stmt = _conn_chunks.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
				stmt.setFetchSize(BULK_BATCH_FETCH_SIZE);
				String sql = "select  a.data, a.hora, a.asset, a.ultimo valor_ativo, 0 as preco_opcao, a.strike, a.oferta_compra, "
						+ " a.oferta_venda, a.vencimento, a.validade, a.estado_atual, a.relogio,  "
						+ " a.VOC, a.VOV, a.contratos_abertos,  a.negocios, a.quantidade, a.volume "
						+ " from B3Log.B3SignalLoggerRawLotBuffer a " + " where " + "  a.strike = 0 AND "
						+ " a.asset = '" + _ativo + "' " + " ORDER BY a.relogio ASC";

//					LOGGER.info(sql);
				rs = stmt.executeQuery(sql);
				long timer1 = System.currentTimeMillis();
				long db_load_time = timer1 - start_time;
				LOGGER.info("sql DB fetch execution time:" + (db_load_time / 1000) + " sec ");

				Long asset_counter_total = 0L;
				Long asset_counter_changes = 0L;
				Long asset_counter_changes_total = 0L;
				Long asset_counter_batch_insert_total = 0L;
				boolean changes = false;
				String data;
				String hora;
				String asset;
				double valor_ativo;
				double preco_opcao;
				double strike;
				double oferta_compra;
				double oferta_venda;
				String vencimento;
				String validade;
				String estado_atual;
				Timestamp relogio;

				int VOC;
				int VOV;
				BigDecimal contratos_abertos;
				int negocios;
				int quantidade;
				Double volume;

				String compiledQuery = "INSERT INTO Intellect.hot_table_assets("
						+ "data,hora,asset,valor_ativo,ultimo,strike,oferta_compra,oferta_venda,vencimento,validade,estado_atual,relogio_last_change,"
						+ "VOC, VOV, contratos_abertos, valor_medio_by_negocios, valor_medio_by_quantidade, negocios, quantidade, volume, lot_name, lot_id) VALUES"
						+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?)";
				preparedStatement = DBConnectionHelper.getETLConn().prepareStatement(compiledQuery);

				int _option_counter = 0;
				long _option_start_time = System.currentTimeMillis();
				while (rs.next()) { // LOOP GET DERIVATIVES ORDER BY RELOGIO ASC for this ASSET
					long timer7 = System.currentTimeMillis();
					long __diff_time = timer7 - _option_start_time;

					if ((__diff_time % 10000) == 0) {
						LOGGER.info("records processed on this asset: " + _option_counter + " total spent time: "
								+ (__diff_time / 1000) + " sec");
					}

					data = rs.getString("data");
					hora = rs.getString("hora");
					asset = rs.getString("asset");
					valor_ativo = rs.getDouble("valor_ativo");
					preco_opcao = rs.getDouble("preco_opcao");
					strike = rs.getDouble("strike");
					oferta_compra = rs.getDouble("oferta_compra");
					oferta_venda = rs.getDouble("oferta_venda");
					vencimento = rs.getString("vencimento");
					validade = rs.getString("validade");
					estado_atual = rs.getString("estado_atual");
					relogio = rs.getTimestamp("relogio");

					VOC = rs.getInt("VOC");
					VOV = rs.getInt("VOV");
					contratos_abertos = rs.getBigDecimal("contratos_abertos");
					negocios = rs.getInt("negocios");
					quantidade = rs.getInt("quantidade");
					volume = rs.getDouble("volume");

					asset_book_values = hidrateAssetRTDValues(data, hora, asset, valor_ativo, preco_opcao, strike,
							oferta_compra, oferta_venda, vencimento, validade, estado_atual, relogio, VOC, VOV,
							contratos_abertos, negocios, quantidade, volume);

					boolean buffer_added = false;
					if (!buffer_last_values.containsKey(asset)) {
						LOGGER.info("Adding asset:" + asset + " to buffer");
						asset_counter_total++;
						buffer_last_values.put(asset, new HashMap<String, Object>(asset_book_values));

						asset_counter_changes++;
						asset_counter_changes_total++;
						_option_counter++;

						long timer6 = System.currentTimeMillis();
						long _diff_time = timer6 - _option_start_time;

						if ((_diff_time % 5000) == 0) {
							LOGGER.info("records processed on this asset: " + _option_counter + " total spent time: "
									+ (_diff_time / 1000) + " sec");
						}
						buffer_added = true;
//							continue;
					}

					changes = ifRawRecordChanges(valor_ativo, preco_opcao, strike, oferta_compra, oferta_venda,
							estado_atual, VOC, VOV, contratos_abertos, negocios, quantidade, volume, buffer_last_values,
							asset);

					// EH DIFERENTE DO ANTERIOR DO MESMO ASSET
					if (changes || buffer_added) {
						if (!buffer_added)
							buffer_last_values.put(asset, new HashMap<String, Object>(asset_book_values));

						asset_counter_changes++;
						asset_counter_changes_total++;
						changes = false;
						buffer_added = false;
						java.sql.Date _date = java.sql.Date.valueOf(data);
						java.sql.Date _vencimento = java.sql.Date.valueOf(vencimento);
						java.sql.Date _validade = java.sql.Date.valueOf(validade);

						SimpleDateFormat formatter = new SimpleDateFormat("hh:mm:ss");
						Time _hora = new Time(formatter.parse(hora).getTime());
						double valor_medio_by_negocios;
						double valor_medio_by_quantidade;

						if (negocios != 0)
							valor_medio_by_negocios = volume / negocios;
						else
							valor_medio_by_negocios = 0;

						if (quantidade != 0)
							valor_medio_by_quantidade = volume / quantidade;
						else
							valor_medio_by_quantidade = 0;

						preparedStatement.setDate(1, _date);
						preparedStatement.setTime(2, _hora);
						preparedStatement.setString(3, asset);
						preparedStatement.setDouble(4, valor_ativo); // 4 - valor ativo
						preparedStatement.setDouble(5, valor_ativo); // 5 - coluna ultimo
						preparedStatement.setDouble(6, strike);
						preparedStatement.setDouble(7, oferta_compra);
						preparedStatement.setDouble(8, oferta_venda);
						preparedStatement.setDate(9, _vencimento);
						preparedStatement.setDate(10, _validade);
						preparedStatement.setString(11, estado_atual);
						preparedStatement.setTimestamp(12, relogio);
						preparedStatement.setInt(13, VOC);
						preparedStatement.setInt(14, VOV);
						preparedStatement.setBigDecimal(15, contratos_abertos);
						preparedStatement.setDouble(16, MathHelper.round(valor_medio_by_negocios, 2));
						preparedStatement.setDouble(17, MathHelper.round(valor_medio_by_quantidade, 2));
						preparedStatement.setInt(18, negocios);
						preparedStatement.setInt(19, quantidade);
						preparedStatement.setDouble(20, volume);
						preparedStatement.setString(21, csv_lot.getLot_name());
						preparedStatement.setBigDecimal(22, csv_lot.getId());
						preparedStatement.addBatch();
						preparedStatement.clearParameters();
					}

					/*
					 * BULK INSERT OR IF TOTAL ROWS FETCHED < BULK_SIZE
					 */
					if ((asset_counter_changes % BULK_BATCH_INSERT_SIZE) == 0) {
						long timer2 = System.currentTimeMillis();

						/*
						 * execute BATCH INSERT OF BULK_SIZE preperadStatement
						 */
						int[] inserted = preparedStatement.executeBatch();
						if (!DBConnectionHelper.getETLConn().getAutoCommit())
							DBConnectionHelper.getETLConn().commit();
//						_conn_chunks.commit();
						long timer3 = System.currentTimeMillis();
						long diff_time = timer3 - timer2;
						LOGGER.info("BULK insert of: " + inserted.length + " inserted.lenght");
						LOGGER.info("Total time to INSERT: " + inserted.length + " registros " + diff_time + " ms");

						preparedStatement.clearBatch();

						asset_counter_changes = 0L;
						asset_counter_batch_insert_total++;
					}
					asset_counter_total++;
					_option_counter++;

					long timer4 = System.currentTimeMillis();
					long diff_time = timer4 - _option_start_time;

					if ((diff_time % 5000) == 0) {
						LOGGER.info("records processed on this asset: " + _option_counter + " total spent time: "
								+ (diff_time / 1000) + " sec");
					}
				}

				/*
				 * BULK INSERT OF RESIDUAL CHANGES
				 */
				if (asset_counter_changes > 0) {
					long timer2 = System.currentTimeMillis();

					/*
					 * execute BATCH INSERT OF BULK_SIZE preperadStatement
					 */
					int[] inserted = preparedStatement.executeBatch();
					if (!DBConnectionHelper.getETLConn().getAutoCommit())
						DBConnectionHelper.getETLConn().commit();
//						if (inserted[0] < 0)
//							throw new Exception("error on bulk insert at record: " + asset_counter_total
//									+ " from tables b3signalloggerraw " + "records before " + asset_counter_total
//									+ " counter, may be already inserted on database hot_table");

					long timer3 = System.currentTimeMillis();
					long diff_time = timer3 - timer2;
					LOGGER.info("BULK insert of: " + inserted.length + " inserted.lenght");
					LOGGER.info("Total time to INSERT: " + inserted.length + " registros " + diff_time + " ms");
					// DBConnectionHelper.getETLConn().commit();
					preparedStatement.clearBatch();

					asset_counter_changes = 0L;
					asset_counter_batch_insert_total++;
				}

				long timer5 = System.currentTimeMillis();
				long diff_time = timer5 - timer1;

				LOGGER.info("Total records processed for this asset: " + asset_counter_total);
				LOGGER.info("Total records changes for this asset: " + asset_counter_changes_total);
				LOGGER.info("Total BULK batch inserts for this asset: " + asset_counter_batch_insert_total + " of "
						+ BULK_BATCH_INSERT_SIZE + " records");
				LOGGER.info("Total time to process for this asset: " + diff_time + "ms [" + _ativo + "]");

				asset_counter_total = 0L;
				asset_counter_changes = 0L;
				asset_counter_changes_total = 0L;
				asset_counter_batch_insert_total = 0L;
				buffer_last_values = new HashMap<String, Map<String, Object>>();

				_ativo_counter++;

			} // END OF FOREACH ASSET STRIK = 0 (ATIVOS)

			long timer5 = System.currentTimeMillis();
			long _diff_time = timer5 - start_time;
			LOGGER.info("Total time to process " + ativos_list.size() + " assets: " + _diff_time + "ms ");
			if (!_conn_chunks.getAutoCommit())
				_conn_chunks.commit();
			if (_conn_chunks != null && !_conn_chunks.isClosed())
				_conn_chunks.close();
		} catch (Exception e) {
			if (_conn_chunks != null && !_conn_chunks.isClosed())
				_conn_chunks.close();
			LOGGER.error(e.getMessage(), e);
			return;
		}
	}

	private void normalizeDerivatives(String _ativo2, CsvLoadLot csv_lot) throws Exception, SQLException {
		Map<String, Map<String, Object>> buffer_last_values = new HashMap<String, Map<String, Object>>();
		Map<String, Object> asset_book_values;
		// FOR EACH FINISHED LOT

		PreparedStatement preparedStatement;
		Statement stmt;
		ResultSet rs;
		int _ativo_counter = 0;
		Connection _conn_chunks = null;
		try {
			_conn_chunks = DBConnectionHelper.getNewNonStaticConn();
			_conn_chunks.setAutoCommit(false);
			Long start_time = System.currentTimeMillis();
			// FIXME REGEX QUEBRADO COM ESSE CARA
			if (_ativo2.equals("B3SA3"))
				return;

//				recreateAssetTmpTable();

			LOGGER.info("Fetching raw data from B3Log.B3SignalLoggerRawLotBuffer:");
			LOGGER.info("asset (" + _ativo_counter + "):" + _ativo2 + " lot: " + csv_lot.getLot_name());

			stmt = _conn_chunks.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			stmt.setFetchSize(BULK_BATCH_INSERT_SIZE);
//				String sql = "select  a.data, a.hora, a.asset, b.ultimo valor_ativo, a.ultimo as preco_opcao, a.strike, a.oferta_compra, a.oferta_venda, a.vencimento, a.validade, a.estado_atual, a.relogio,  "
//						+ " a.VOC, a.VOV, a.contratos_abertos,  a.negocios, a.quantidade, a.volume "
//						+ " from B3Log.B3SignalLoggerRawLotBuffer a "
//						+ "			LEFT JOIN B3Log.B3SignalLoggerRawLotBuffer b ON a.relogio=b.relogio AND b.asset = '"
//						+ _ativo + "' " + "			where   a.lot_id = " + csv_lot.getId() + " " + "and b.lot_id = "
//						+ csv_lot.getId() + " AND a.strike != 0 and " + "a.asset like  substring('" + _ativo
//						+ "', '[A-Z]+')||'%'  " + "ORDER BY a.relogio ASC";

			String sql = "select  a.data, a.hora, a.asset, a.ultimo as preco_opcao, a.strike, a.oferta_compra, a.oferta_venda, a.vencimento, a.validade, a.estado_atual, a.relogio,  "
					+ " a.VOC, a.VOV, a.contratos_abertos,  a.negocios, a.quantidade, a.volume "
					+ " from B3Log.B3SignalLoggerRawLotBuffer a " + " where   a.strike != 0 and "
					+ " a.asset like  substring('" + _ativo2 + "', '[A-Z]+')||'%'  " + "ORDER BY a.relogio ASC";

//				String sql = "select  a.data, a.hora, a.asset, a.ultimo as preco_opcao, a.strike, a.oferta_compra, a.oferta_venda, a.vencimento, a.validade, a.estado_atual, a.relogio,  "
//						+ " a.VOC, a.VOV, a.contratos_abertos,  a.negocios, a.quantidade, a.volume "
//						+ " from B3Log.B3SignalLoggerRawLotBuffer a "
//						+ " where   a.lot_id = " + csv_lot.getId() 
//						+ " AND a.strike != 0 and " + "a.asset like  substring('" + _ativo
//						+ "', '[A-Z]+')||'%'  " + "ORDER BY a.relogio ASC";

			LOGGER.info(sql);
			rs = stmt.executeQuery(sql);
			long timer1 = System.currentTimeMillis();
			long db_load_time = timer1 - start_time;
			LOGGER.info("sql DB fetch execution time:" + db_load_time + " ms");

			Long asset_counter_total = 0L;
			Long asset_counter_changes = 0L;
			Long asset_counter_changes_total = 0L;
			Long asset_counter_batch_insert_total = 0L;
			boolean changes = false;
			String data;
			String hora;
			String asset;
			double valor_ativo;
			double preco_opcao;
			double strike;
			double oferta_compra;
			double oferta_venda;
			String vencimento;
			String validade;
			String estado_atual;
			Timestamp relogio;

			int VOC;
			int VOV;
			BigDecimal contratos_abertos;
			int negocios;
			int quantidade;
			Double volume;

			String compiledQuery = "INSERT INTO Intellect.hot_table_derivatives("
					+ "data,hora,asset,valor_ativo,ultimo,strike,oferta_compra,oferta_venda,vencimento,validade,estado_atual,relogio_last_change,"
					+ "VOC, VOV, contratos_abertos, valor_medio_by_negocios, valor_medio_by_quantidade, negocios, quantidade, volume, lot_name, lot_id) VALUES"
					+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?)";
			preparedStatement = DBConnectionHelper.getETLConn().prepareStatement(compiledQuery);

			int _option_counter = 0;
			long _option_start_time = System.currentTimeMillis();
			while (rs.next()) { // LOOP GET DERIVATIVES ORDER BY RELOGIO ASC for this ASSET
				long timer7 = System.currentTimeMillis();
				long __diff_time = timer7 - _option_start_time;

				if ((__diff_time % 10000) == 0) {
					LOGGER.info("records processed on this asset: " + _option_counter + " total spent time: "
							+ (__diff_time / 1000) + " sec");
				}
				data = rs.getString("data");
				hora = rs.getString("hora");
				asset = rs.getString("asset");
				preco_opcao = rs.getDouble("preco_opcao");
				strike = rs.getDouble("strike");
				oferta_compra = rs.getDouble("oferta_compra");
				oferta_venda = rs.getDouble("oferta_venda");
				vencimento = rs.getString("vencimento");
				validade = rs.getString("validade");
				estado_atual = rs.getString("estado_atual");
				relogio = rs.getTimestamp("relogio");

				VOC = rs.getInt("VOC");
				VOV = rs.getInt("VOV");
				contratos_abertos = rs.getBigDecimal("contratos_abertos");
				negocios = rs.getInt("negocios");
				quantidade = rs.getInt("quantidade");
				volume = rs.getDouble("volume");
				valor_ativo = 0;
//					valor_ativo = getValorAtivo(asset, relogio, csv_lot.getId());
				asset_book_values = hidrateAssetRTDValues(data, hora, asset, valor_ativo, preco_opcao, strike,
						oferta_compra, oferta_venda, vencimento, validade, estado_atual, relogio, VOC, VOV,
						contratos_abertos, negocios, quantidade, volume);

				boolean buffer_added = false;
				if (!buffer_last_values.containsKey(asset)) {
					LOGGER.info("Adding asset:" + asset + " to buffer");
					asset_counter_total++;
					buffer_last_values.put(asset, new HashMap<String, Object>(asset_book_values));

					asset_counter_changes++;
					asset_counter_changes_total++;
					_option_counter++;

					long timer6 = System.currentTimeMillis();
					long _diff_time = timer6 - _option_start_time;

					if ((_diff_time % 5000) == 0) {
						LOGGER.info("records processed on this asset: " + _option_counter + " total spent time: "
								+ (_diff_time / 1000) + " sec");
					}
					buffer_added = true;
				}

				changes = ifRawRecordChanges(valor_ativo, preco_opcao, strike, oferta_compra, oferta_venda,
						estado_atual, VOC, VOV, contratos_abertos, negocios, quantidade, volume, buffer_last_values,
						asset);

				// EH DIFERENTE DO ANTERIOR DO MESMO ASSET OU PRIMEIRO REGISTRO
				// VAI PRO BULK (Batch) INSERT
				if (changes || buffer_added) {
					if (!buffer_added)
						buffer_last_values.put(asset, new HashMap<String, Object>(asset_book_values));

					asset_counter_changes++;
					asset_counter_changes_total++;
					changes = false;
					buffer_added = false;
					java.sql.Date _date = java.sql.Date.valueOf(data);
					java.sql.Date _vencimento = java.sql.Date.valueOf(vencimento);
					java.sql.Date _validade = java.sql.Date.valueOf(validade);

					SimpleDateFormat formatter = new SimpleDateFormat("hh:mm:ss");
					Time _hora = new Time(formatter.parse(hora).getTime());
					double valor_medio_by_negocios;
					double valor_medio_by_quantidade;

					if (negocios != 0)
						valor_medio_by_negocios = volume / negocios;
					else
						valor_medio_by_negocios = 0;

					if (quantidade != 0)
						valor_medio_by_quantidade = volume / quantidade;
					else
						valor_medio_by_quantidade = 0;

//						valor_ativo = getValorAtivo(_ativo, relogio);

					preparedStatement.setDate(1, _date);
					preparedStatement.setTime(2, _hora);
					preparedStatement.setString(3, asset);
					preparedStatement.setDouble(4, valor_ativo);
					preparedStatement.setDouble(5, preco_opcao);
					preparedStatement.setDouble(6, strike);
					preparedStatement.setDouble(7, oferta_compra);
					preparedStatement.setDouble(8, oferta_venda);
					preparedStatement.setDate(9, _vencimento);
					preparedStatement.setDate(10, _validade);
					preparedStatement.setString(11, estado_atual);
					preparedStatement.setTimestamp(12, relogio);
					preparedStatement.setInt(13, VOC);
					preparedStatement.setInt(14, VOV);
					preparedStatement.setBigDecimal(15, contratos_abertos);
					preparedStatement.setDouble(16, MathHelper.round(valor_medio_by_negocios, 2));
					preparedStatement.setDouble(17, MathHelper.round(valor_medio_by_quantidade, 2));
					preparedStatement.setInt(18, negocios);
					preparedStatement.setInt(19, quantidade);
					preparedStatement.setDouble(20, volume);
					preparedStatement.setString(21, csv_lot.getLot_name());
					preparedStatement.setBigDecimal(22, csv_lot.getId());
					preparedStatement.addBatch();
					preparedStatement.clearParameters();
				}

				/*
				 * BULK INSERT OR IF TOTAL ROWS FETCHED < BULK_SIZE
				 */
				if ((asset_counter_changes % BULK_BATCH_INSERT_SIZE) == 0) {
					long timer2 = System.currentTimeMillis();

					/*
					 * execute BATCH INSERT OF BULK_SIZE preperadStatement
					 */
					int[] inserted = preparedStatement.executeBatch();
					if (!DBConnectionHelper.getETLConn().getAutoCommit())
						DBConnectionHelper.getETLConn().commit();
					long timer3 = System.currentTimeMillis();
					long diff_time = timer3 - timer2;
					LOGGER.info("BULK insert of: " + inserted.length + " inserted.lenght");
					LOGGER.info("Total time to INSERT: " + inserted.length + " registros " + diff_time + " ms");
					// DBConnectionHelper.getETLConn().commit();
					preparedStatement.clearBatch();

					asset_counter_changes = 0L;
					asset_counter_batch_insert_total++;
				}
				asset_counter_total++;
				_option_counter++;

				long timer4 = System.currentTimeMillis();
				long diff_time = timer4 - _option_start_time;

				if ((diff_time % 5000) == 0) {
					LOGGER.info("records processed on this asset: " + _option_counter + " total spent time: "
							+ (diff_time / 1000) + " sec");
				}
			}

			/*
			 * BULK INSERT OF RESIDUAL CHANGES
			 */
			if (asset_counter_changes > 0) {
				long timer2 = System.currentTimeMillis();

				/*
				 * execute BATCH INSERT OF BULK_SIZE preperadStatement
				 */
				int[] inserted = preparedStatement.executeBatch();
				if (!DBConnectionHelper.getETLConn().getAutoCommit())
					DBConnectionHelper.getETLConn().commit();
//						if (inserted[0] < 0)
//							throw new Exception("error on bulk insert at record: " + asset_counter_total
//									+ " from tables b3signalloggerraw " + "records before " + asset_counter_total
//									+ " counter, may be already inserted on database hot_table");

				long timer3 = System.currentTimeMillis();
				long diff_time = timer3 - timer2;
				LOGGER.info("BULK insert of: " + inserted.length + " inserted.lenght");
				LOGGER.info("Total time to INSERT: " + inserted.length + " registros " + diff_time + " ms");
				preparedStatement.clearBatch();

				asset_counter_changes = 0L;
				asset_counter_batch_insert_total++;
			}

			long timer5 = System.currentTimeMillis();
			long diff_time = timer5 - timer1;

			LOGGER.info("Total records processed for this asset: " + asset_counter_total);
			LOGGER.info("Total records changes for this asset: " + asset_counter_changes_total);
			LOGGER.info("Total BULK batch inserts for this asset: " + asset_counter_batch_insert_total + " of "
					+ BULK_BATCH_INSERT_SIZE + " records");
			LOGGER.info("Total time to process for this asset: " + diff_time + "ms [" + _ativo2 + "]");

			asset_counter_total = 0L;
			asset_counter_changes = 0L;
			asset_counter_changes_total = 0L;
			asset_counter_batch_insert_total = 0L;
			buffer_last_values = new HashMap<String, Map<String, Object>>();

			_ativo_counter++;
			timer5 = System.currentTimeMillis();
			long _diff_time = timer5 - start_time;
			LOGGER.info("Total time to process " + _ativo2 + _diff_time + "ms ");
			if (!_conn_chunks.getAutoCommit())
				_conn_chunks.commit();
			if (_conn_chunks != null && !_conn_chunks.isClosed())
				_conn_chunks.close();
		} catch (Exception e) {
			if (_conn_chunks != null && !_conn_chunks.isClosed())
				_conn_chunks.close();
			LOGGER.error(e.getMessage(), e);
			return;
		}
	}

	private void recreateAssetTmpTable() throws SQLException, Exception {
		PreparedStatement preparedStatement;
		preparedStatement = DBConnectionHelper.getETLConn()
				.prepareStatement("DROP TABLE IF EXISTS B3Log.B3SignalLoggerRawLotBuffer_asset_tmp;"
						+ "CREATE TABLE B3Log.B3SignalLoggerRawLotBuffer_asset_tmp (\n"
						+ "                id BIGINT  NOT NULL,\n" + "                asset VARCHAR(11) NOT NULL,\n"
						+ "                data DATE NOT NULL,\n" + "                hora TIME NOT NULL,\n"
						+ "                ultimo NUMERIC(8,2) NOT NULL,\n"
						+ "                negocios INTEGER NOT NULL,\n"
						+ "                quantidade INTEGER NOT NULL,\n"
						+ "                volume NUMERIC(18,2) NOT NULL,\n"
						+ "                oferta_compra NUMERIC(8,2) NOT NULL,\n"
						+ "                oferta_venda NUMERIC(8,2) NOT NULL,\n"
						+ "                VOC INTEGER NOT NULL,\n" + "                VOV INTEGER NOT NULL,\n"
						+ "                vencimento DATE NOT NULL,\n" + "                validade DATE NOT NULL,\n"
						+ "                contratos_abertos BIGINT NOT NULL,\n"
						+ "                estado_atual VARCHAR(80) NOT NULL,\n"
						+ "                relogio TIMESTAMP NOT NULL,\n"
						+ "		 	 lot_name VARCHAR(70) NOT NULL,\n" + "		      lot_id BIGINT  NOT NULL,\n"
						+ "                CONSTRAINT b3signalloggerrawbuffer_asset_tmp_pk PRIMARY KEY (id),\n"
						+ "                CONSTRAINT fk_B3SignalLoggerrawbuffer_lot_id FOREIGN KEY(lot_id) \n"
						+ "	  REFERENCES Intellect.csv_load_lot(id)\n" + ");\n"
						+ "ALTER TABLE B3Log.B3SignalLoggerRawLotBuffer_asset_tmp SET (autovacuum_enabled = false); \n"
						+ "ALTER TABLE B3Log.B3SignalLoggerRawLotBuffer_asset_tmp SET UNLOGGED;");
		preparedStatement.execute();
		if (!DBConnectionHelper.getETLConn().getAutoCommit())
			DBConnectionHelper.getETLConn().commit();
		preparedStatement = DBConnectionHelper.getETLConn()
				.prepareStatement("DROP INDEX IF EXISTS B3Log.b3signalloggerbuffer_asset_idx;\n"
						+ "DROP INDEX IF EXISTS B3Log.b3signalloggerbufferassettmp_asset_idx;\n"
						+ "DROP INDEX IF EXISTS B3Log.b3signalloggerbufferassettmp_asset_relogio_idx;\n");
		preparedStatement.execute();

		preparedStatement = DBConnectionHelper.getETLConn()
				.prepareStatement("DROP TABLE IF EXISTS B3Log.B3SignalLoggerRawLotBuffer_asset_tmp;"
						+ "CREATE TABLE B3Log.B3SignalLoggerRawLotBuffer_asset_tmp (\n"
						+ "                id BIGINT  NOT NULL,\n" + "                asset VARCHAR(11) NOT NULL,\n"
						+ "                data DATE NOT NULL,\n" + "                hora TIME NOT NULL,\n"
						+ "                ultimo NUMERIC(8,2) NOT NULL,\n"
						+ "                negocios INTEGER NOT NULL,\n"
						+ "                quantidade INTEGER NOT NULL,\n"
						+ "                volume NUMERIC(18,2) NOT NULL,\n"
						+ "                oferta_compra NUMERIC(8,2) NOT NULL,\n"
						+ "                oferta_venda NUMERIC(8,2) NOT NULL,\n"
						+ "                VOC INTEGER NOT NULL,\n" + "                VOV INTEGER NOT NULL,\n"
						+ "                vencimento DATE NOT NULL,\n" + "                validade DATE NOT NULL,\n"
						+ "                contratos_abertos BIGINT NOT NULL,\n"
						+ "                estado_atual VARCHAR(80) NOT NULL,\n"
						+ "                relogio TIMESTAMP NOT NULL,\n"
						+ "		 	 lot_name VARCHAR(70) NOT NULL,\n" + "		      lot_id BIGINT  NOT NULL,\n"
						+ "                CONSTRAINT b3signalloggerrawbuffer_asset_tmp_pk PRIMARY KEY (id),\n"
						+ "                CONSTRAINT fk_B3SignalLoggerrawbuffer_lot_id FOREIGN KEY(lot_id) \n"
						+ "	  REFERENCES Intellect.csv_load_lot(id)\n" + ");\n"
						+ "ALTER TABLE B3Log.B3SignalLoggerRawLotBuffer_asset_tmp SET (autovacuum_enabled = false); \n"
						+ "ALTER TABLE B3Log.B3SignalLoggerRawLotBuffer_asset_tmp SET UNLOGGED;");
		preparedStatement.execute();
		if (!DBConnectionHelper.getETLConn().getAutoCommit())
			DBConnectionHelper.getETLConn().commit();
	}

	private double getValorAtivo(String _ativo, Timestamp relogio, BigDecimal id) throws Exception {
		Statement stmt;
		double valor_ativo = 0;
		long start_time = System.currentTimeMillis();

		try {
			stmt = DBConnectionHelper.getETL2Conn().createStatement();
			ResultSet rs = stmt.executeQuery("SELECT ultimo from B3Log.B3SignalLoggerRawLotBuffer_asset_tmp a"
					+ " WHERE asset = '" + _ativo + "' AND relogio = '" + relogio + "' AND lot_id = " + id
					+ " order by relogio DESC limit 1");
			while (rs.next()) {
				valor_ativo = rs.getDouble("ultimo");
			}
		} catch (SQLException e1) {
			LOGGER.error(e1.getMessage());
			// TODO: check if connection.close() get calls on finally before the throw
			throw new Exception(e1);
		}
//		LOGGER.info("Total to fetch valor Ativo: " + (System.currentTimeMillis() - start_time));
		return valor_ativo;
	}

	private Map<String, Object> hidrateAssetRTDValues(String data, String hora, String asset, double valor_ativo,
			double preco_opcao, double strike, double oferta_compra, double oferta_venda, String vencimento,
			String validade, String estado_atual, Timestamp relogio, int VOC, int VOV, BigDecimal contratos_abertos,
			int negocios, int quantidade, Double volume) {
		Map<String, Object> asset_book_values;
		asset_book_values = new HashMap<String, Object>();
		asset_book_values.put("data", data);
		asset_book_values.put("hora", hora);
		asset_book_values.put("asset", asset);
		asset_book_values.put("valor_ativo", valor_ativo);
		asset_book_values.put("preco_opcao", preco_opcao);
		asset_book_values.put("strike", strike);
		asset_book_values.put("oferta_compra", oferta_compra);
		asset_book_values.put("oferta_venda", oferta_venda);
		asset_book_values.put("vencimento", vencimento);
		asset_book_values.put("validade", validade);
		asset_book_values.put("estado_atual", estado_atual);
		asset_book_values.put("relogio", relogio.toString());

		asset_book_values.put("VOC", VOC);
		asset_book_values.put("VOV", VOV);
		asset_book_values.put("contratos_abertos", contratos_abertos);
		asset_book_values.put("negocios", negocios);
		asset_book_values.put("quantidade", quantidade);
		asset_book_values.put("volume", volume);
		return asset_book_values;
	}

	private boolean ifRawRecordChanges(double valor_ativo, double preco_opcao, double strike, double oferta_compra,
			double oferta_venda, String estado_atual, int VOC, int VOV, BigDecimal contratos_abertos, int negocios,
			int quantidade, Double volume, Map<String, Map<String, Object>> buffer_last_values, String asset) {

		double buffer_valor_ativo = (double) buffer_last_values.get(asset).get("valor_ativo");
		double buffer_preco_opcao = (double) buffer_last_values.get(asset).get("preco_opcao");
		double buffer_strike = (double) buffer_last_values.get(asset).get("strike");
		double buffer_oferta_compra = (double) buffer_last_values.get(asset).get("oferta_compra");
		double buffer_oferta_venda = (double) buffer_last_values.get(asset).get("oferta_venda");

		int buffer_VOC = (int) buffer_last_values.get(asset).get("VOC");
		int buffer_VOV = (int) buffer_last_values.get(asset).get("VOV");
		BigDecimal buffer_contratos_abertos = (BigDecimal) buffer_last_values.get(asset).get("contratos_abertos");
		int buffer_negocios = (int) buffer_last_values.get(asset).get("negocios");
		int buffer_quantidade = (int) buffer_last_values.get(asset).get("quantidade");
		Double buffer_volume = (Double) buffer_last_values.get(asset).get("volume");
		String buffer_estado_atual = (String) buffer_last_values.get(asset).get("estado_atual");

		boolean changes = false;

		if (buffer_valor_ativo != valor_ativo)
			changes = true;
		if (buffer_preco_opcao != preco_opcao)
			changes = true;
		if (buffer_strike != strike)
			changes = true;
		if (buffer_oferta_compra != oferta_compra)
			changes = true;
		if (buffer_oferta_venda != oferta_venda)
			changes = true;

		if (buffer_VOC != VOC)
			changes = true;
		if (buffer_VOV != VOV)
			changes = true;
		if (!buffer_contratos_abertos.equals(contratos_abertos))
			changes = true;
		if (buffer_negocios != negocios)
			changes = true;
		if (buffer_quantidade != quantidade)
			changes = true;
		if (!buffer_volume.equals(volume))
			changes = true;

		if (!buffer_estado_atual.equals(estado_atual))
			changes = true;
		return changes;
	}

	public void etl2_black_scholes() throws Exception {
		// de cada tupla da hot_table
		// quero coluna tmb com Logaritimo da variancia esse log eh um vetor de angulo e
		// força
		LOGGER.info("[ETL]etl2_black_scholes ...");
	}

	@Override
	public void handler_start() throws Exception {
		runnable = new ETLHandler(this);
		thread = new Thread(runnable, thread_name);
		thread.start();
	}

	@Override
	public void handler_finish() throws Exception {
		thread.interrupt();
	}

	public void setStartTime() {
		this.start_time = System.currentTimeMillis();

	}

	public void closeConnection() throws Exception {
		DBConnectionHelper.closeETLConn();
	}

	public void etl1_execute() throws Exception {
		List<BigDecimal> csv_lot_finished = CsvLoadLot.getFinishedLots();

		for (BigDecimal csv_lot_id : csv_lot_finished) {
			CsvLoadLot csv_lot = CsvLoadLot.getLotByLotId(csv_lot_id);
			try {
				LOGGER.info("[ETL] populating raw lot buffer table for lot: " + csv_lot.getLot_name());
				populateRawLotBufferFromLot(csv_lot_id); // IO INTENSIVE, THE IN DOOR, MUST BE A FAST DISPANDABLE
															// HARDWARE
				etl1_populate_assets(csv_lot_id); // FIXME: REMOVE IT, OR FIND A POURPOUSE
				etl1_normalization(csv_lot_id); // FIXME: REMOVE ASSET AND DERIVATIVE CORRELATION
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		long _now = System.currentTimeMillis();
		long _diff_time = _now - start_time;
		if (csv_lot_finished != null && csv_lot_finished.size() > 0)
			LOGGER.info("Total time to populate assets: " + _diff_time + "ms ");

	}

}